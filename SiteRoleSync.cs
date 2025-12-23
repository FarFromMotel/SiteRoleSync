using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Core.Attributes;
using CounterStrikeSharp.API.Modules.Admin;
using CounterStrikeSharp.API.Modules.Timers;

using Microsoft.Extensions.Logging;

namespace SiteRoleSync;

[MinimumApiVersion(80)]
public class SiteRoleSync: BasePlugin, IPluginConfig<SiteRoleSyncConfig>
{
    public override string ModuleName => "SiteRoleSync";
    public override string ModuleVersion => "1.0.0";
    public override string ModuleAuthor => "";
    public override string ModuleDescription => "Sync roles from site/db into CounterStrikeSharp admins/groups (join + periodic full).";

    public SiteRoleSyncConfig Config { get; set; } = new();
    public void OnConfigParsed(SiteRoleSyncConfig config) => Config = config;

    private HttpClient? _http;
    private readonly SemaphoreSlim _applyLock = new(1, 1);

    // текущая карта: steamId64 -> group (#site/helper)
    private readonly ConcurrentDictionary<ulong, string> _steamToGroup = new();

    // таймер для редкого полного синка
    private CounterStrikeSharp.API.Modules.Timers.Timer? _fullSyncTimer;

    // debounce таймер, чтобы не перезаписывать admins.json на каждый вход мгновенно
    private CounterStrikeSharp.API.Modules.Timers.Timer? _debouncedApplyTimer;

    private string _lastFullHash = "";

    private static readonly JsonSerializerOptions JsonOptsIndented = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public override void Load(bool hotReload)
    {
        _http = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(Math.Max(2, Config.HttpTimeoutSeconds))
        };

        if (!string.IsNullOrWhiteSpace(Config.ApiKey))
        {
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Config.ApiKey);
        }

        // 1) Попытаемся загрузить текущий admins.json в память (не обязательно, но полезно)
        TryLoadExistingAdmins(Config.AdminsPath);

        // 2) Ивент “игрок полностью подключился” — SteamID уже есть
        RegisterEventHandler<EventPlayerConnectFull>(OnPlayerConnectFull);

        // 3) Стартовый полный синк (сразу)
        _ = FullSyncSafeAsync();

        // 4) Редкий полный синк по таймеру
        _fullSyncTimer = AddTimer(
            Math.Max(10, Config.FullSyncIntervalSeconds),
            () => _ = FullSyncSafeAsync(),
            TimerFlags.REPEAT
        );

        Logger.LogInformation($"[{ModuleName}] Loaded. Full sync every {Config.FullSyncIntervalSeconds}s. " +
                              $"AdminsPath={Config.AdminsPath}, GroupsPath={Config.GroupsPath}");
    }

    public override void Unload(bool hotReload)
    {
        try { _fullSyncTimer?.Kill(); } catch { /* ignore */ }
        try { _debouncedApplyTimer?.Kill(); } catch { /* ignore */ }

        _http?.Dispose();
        _http = null;

        _steamToGroup.Clear();
    }

    // ====== Join sync ======

    private HookResult OnPlayerConnectFull(EventPlayerConnectFull ev, GameEventInfo info)
    {
        try
        {
            var player = ev.Userid;
            if (player == null || !player.IsValid) return HookResult.Continue;

            var sid = (ulong)player.SteamID;
            if (sid == 0) return HookResult.Continue;

            // синк по входу (асинхронно)
            _ = SyncOneSafeAsync(sid);

            return HookResult.Continue;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, $"[{ModuleName}] OnPlayerConnectFull error");
            return HookResult.Continue;
        }
    }

    private async Task SyncOneSafeAsync(ulong steamId64)
    {
        try
        {
            await SyncOneAsync(steamId64);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, $"[{ModuleName}] SyncOne error for {steamId64}");
        }
    }

    private async Task SyncOneAsync(ulong steamId64)
    {
        if (_http == null) return;
        if (string.IsNullOrWhiteSpace(Config.BackendBaseUrl)) return;

        var url = BuildOneUrl(steamId64);

        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        using var resp = await _http.SendAsync(req);

        if (resp.StatusCode == HttpStatusCode.NotFound)
        {
            // роли нет => снять права (если были)
            if (_steamToGroup.TryRemove(steamId64, out _))
            {
                Logger.LogInformation($"[{ModuleName}] Join sync: removed {steamId64}");
                ScheduleApplyChanges();
            }
            return;
        }

        if (!resp.IsSuccessStatusCode)
        {
            var body = await SafeReadBody(resp);
            Logger.LogWarning($"[{ModuleName}] Join sync failed for {steamId64}: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body: {body}");
            return;
        }

        var json = await resp.Content.ReadAsStringAsync();
        var one = JsonSerializer.Deserialize<RoleOneResponse>(json, JsonOpts);

        var group = one?.Group?.Trim() ?? "";
        if (string.IsNullOrWhiteSpace(group))
        {
            if (_steamToGroup.TryRemove(steamId64, out _))
            {
                Logger.LogInformation($"[{ModuleName}] Join sync: removed {steamId64} (empty group)");
                ScheduleApplyChanges();
            }
            return;
        }

        if (_steamToGroup.TryGetValue(steamId64, out var old) && string.Equals(old, group, StringComparison.Ordinal))
        {
            // не изменилось
            return;
        }

        _steamToGroup[steamId64] = group;
        Logger.LogInformation($"[{ModuleName}] Join sync: {steamId64} => {group}");
        ScheduleApplyChanges();
    }

    // ====== Full sync ======

    private async Task FullSyncSafeAsync()
    {
        try
        {
            await FullSyncAsync();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, $"[{ModuleName}] FullSync error");
        }
    }

    private async Task FullSyncAsync()
    {
        if (_http == null) return;
        if (string.IsNullOrWhiteSpace(Config.BackendBaseUrl)) return;

        var url = BuildFullUrl();

        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        using var resp = await _http.SendAsync(req);

        if (!resp.IsSuccessStatusCode)
        {
            var body = await SafeReadBody(resp);
            Logger.LogWarning($"[{ModuleName}] Full sync failed: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body: {body}");
            return;
        }

        var json = await resp.Content.ReadAsStringAsync();

        // хэш, чтобы не делать лишнее
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json)));
        if (hash == _lastFullHash) return;

        var list = JsonSerializer.Deserialize<List<RoleListItem>>(json, JsonOpts) ?? new List<RoleListItem>();

        // строим новую карту
        var next = new Dictionary<ulong, string>();
        foreach (var item in list)
        {
            if (string.IsNullOrWhiteSpace(item.SteamId64)) continue;
            if (!ulong.TryParse(item.SteamId64, out var sid) || sid == 0) continue;

            var group = item.Group?.Trim() ?? "";
            if (string.IsNullOrWhiteSpace(group)) continue;

            next[sid] = group;
        }

        // сравнение с текущим
        var changed = HasMapChanged(next);
        if (!changed)
        {
            _lastFullHash = hash;
            return;
        }

        // применяем в текущую
        _steamToGroup.Clear();
        foreach (var kv in next)
            _steamToGroup[kv.Key] = kv.Value;

        _lastFullHash = hash;

        Logger.LogInformation($"[{ModuleName}] Full sync applied in memory: {next.Count} entries");
        // полный синк — можно применить сразу (без debounce)
        await ApplyChangesSafeAsync();
    }

    private bool HasMapChanged(Dictionary<ulong, string> next)
    {
        // быстрая проверка по size
        if (next.Count != _steamToGroup.Count) return true;

        foreach (var kv in next)
        {
            if (!_steamToGroup.TryGetValue(kv.Key, out var cur)) return true;
            if (!string.Equals(cur, kv.Value, StringComparison.Ordinal)) return true;
        }

        return false;
    }

    // ====== Apply changes (write admins.json + reload AdminManager) ======

    private void ScheduleApplyChanges()
    {
        // debounce: например, 2 секунды после последнего изменения
        var delay = Math.Max(1, Config.ApplyDebounceSeconds);

        try { _debouncedApplyTimer?.Kill(); } catch { /* ignore */ }

        _debouncedApplyTimer = AddTimer(delay, () => _ = ApplyChangesSafeAsync(), TimerFlags.STOP_ON_MAPCHANGE);
    }

    private async Task ApplyChangesSafeAsync()
    {
        await _applyLock.WaitAsync();
        try
        {
            // строим admins.json объект
            // стабильная сортировка для предсказуемого файла
            var ordered = _steamToGroup
                .ToArray()
                .OrderBy(kv => kv.Key)
                .ToArray();

            var root = new Dictionary<string, AdminJsonEntry>(StringComparer.Ordinal);
            foreach (var kv in ordered)
            {
                var sid = kv.Key;
                var group = kv.Value;

                root[sid.ToString()] = new AdminJsonEntry
                {
                    Identity = sid.ToString(),
                    Groups = new[] { group }
                };
            }

            var outJson = JsonSerializer.Serialize(root, JsonOptsIndented);

            WriteAtomic(Config.AdminsPath, outJson);

            // reload admin data без рестарта
            AdminManager.LoadAdminGroups(Config.GroupsPath);
            AdminManager.LoadAdminData(Config.AdminsPath);
            AdminManager.MergeGroupPermsIntoAdmins();

            Logger.LogInformation($"[{ModuleName}] Applied: wrote admins.json ({root.Count} entries) and reloaded admin data");
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, $"[{ModuleName}] ApplyChanges error");
        }
        finally
        {
            _applyLock.Release();
        }
    }

    private static void WriteAtomic(string path, string content)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        File.WriteAllText(tmp, content, Encoding.UTF8);

        // Replace atomically where possible
        if (File.Exists(path))
        {
            // File.Replace может быть надёжнее на Windows, но требует backup path.
            // Здесь используем Move с overwrite (доступно в .NET Core/5+).
            File.Delete(path);
        }

        File.Move(tmp, path, overwrite: true);
    }

    private void TryLoadExistingAdmins(string adminsPath)
    {
        try
        {
            if (!File.Exists(adminsPath)) return;

            var json = File.ReadAllText(adminsPath, Encoding.UTF8);
            var parsed = JsonSerializer.Deserialize<Dictionary<string, AdminJsonEntry>>(json, JsonOpts);

            if (parsed == null) return;

            foreach (var entry in parsed.Values)
            {
                if (string.IsNullOrWhiteSpace(entry.Identity)) continue;
                if (!ulong.TryParse(entry.Identity, out var sid) || sid == 0) continue;

                var group = entry.Groups?.FirstOrDefault()?.Trim() ?? "";
                if (string.IsNullOrWhiteSpace(group)) continue;

                _steamToGroup[sid] = group;
            }

            Logger.LogInformation($"[{ModuleName}] Loaded existing admins.json into cache: {_steamToGroup.Count} entries");
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, $"[{ModuleName}] Failed to load existing admins.json (will rebuild from backend)");
        }
    }

    private string BuildFullUrl()
    {
        var baseUrl = Config.BackendBaseUrl.TrimEnd('/');
        // пример: https://site/api/cs2/admins?serverId=cs2-1
        return $"{baseUrl}{Config.FullListPath}?serverId={Uri.EscapeDataString(Config.ServerId)}";
    }

    private string BuildOneUrl(ulong steamId64)
    {
        var baseUrl = Config.BackendBaseUrl.TrimEnd('/');
        // пример: https://site/api/cs2/admins/{steamId64}?serverId=cs2-1
        var path = Config.OneUserPathTemplate.Replace("{steamId64}", steamId64.ToString());
        return $"{baseUrl}{path}?serverId={Uri.EscapeDataString(Config.ServerId)}";
    }

    private static async Task<string> SafeReadBody(HttpResponseMessage resp)
    {
        try { return await resp.Content.ReadAsStringAsync(); }
        catch { return ""; }
    }

    // ====== DTOs ======

    private class RoleListItem
    {
        [JsonPropertyName("steamId64")]
        public string SteamId64 { get; set; } = "";

        [JsonPropertyName("group")]
        public string Group { get; set; } = "";
    }

    private class RoleOneResponse
    {
        [JsonPropertyName("steamId64")]
        public string SteamId64 { get; set; } = "";

        [JsonPropertyName("group")]
        public string Group { get; set; } = "";
    }

    private class AdminJsonEntry
    {
        [JsonPropertyName("identity")]
        public string Identity { get; set; } = "";

        [JsonPropertyName("groups")]
        public string[] Groups { get; set; } = Array.Empty<string>();

        // можно расширить, если нужно:
        // [JsonPropertyName("flags")] public string[]? Flags { get; set; }
        // [JsonPropertyName("immunity")] public int? Immunity { get; set; }
    }
}

public class SiteRoleSyncConfig
{
    // База урла бэкенда, например: "https://your-site.com"
    public string BackendBaseUrl { get; set; } = "https://your-site.com";

    // Пути эндпоинтов:
    // Full: GET {BackendBaseUrl}{FullListPath}?serverId=...
    public string FullListPath { get; set; } = "/api/cs2/admins";

    // One: GET {BackendBaseUrl}{OneUserPathTemplate}?serverId=...
    // где {steamId64} будет заменён
    public string OneUserPathTemplate { get; set; } = "/api/cs2/admins/{steamId64}";

    public string ApiKey { get; set; } = "";
    public string ServerId { get; set; } = "cs2-1";

    // Редкий полный sync
    public int FullSyncIntervalSeconds { get; set; } = 600;

    // Debounce для применений (на массовых входах)
    public int ApplyDebounceSeconds { get; set; } = 2;

    public int HttpTimeoutSeconds { get; set; } = 5;

    // Пути до файлов CSS
    public string AdminsPath { get; set; } = "addons/counterstrikesharp/configs/admins.json";
    public string GroupsPath { get; set; } = "addons/counterstrikesharp/configs/admin_groups.json";
}
