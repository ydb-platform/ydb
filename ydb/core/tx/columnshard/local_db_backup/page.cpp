#include "page.h"

#include <ydb/library/actors/protos/actors.pb.h>

#include <library/cpp/json/json_writer.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

namespace NKikimr::NColumnShard::NLocalDbBackup {

namespace {

TString EscapeHtml(const TString& in) {
    TString out;
    out.reserve(in.size());
    for (char c : in) {
        switch (c) {
            case '<':
                out += "&lt;";
                break;
            case '>':
                out += "&gt;";
                break;
            case '&':
                out += "&amp;";
                break;
            case '\'':
                out += "&#39;";
                break;
            case '\"':
                out += "&#34;";
                break;
            default:
                out += c;
        }
    }
    return out;
}

TString FormatInstant(TInstant value) {
    return value ? value.ToStringUpToSeconds() : TString("never");
}

TString FormatBytes(ui64 bytes) {
    static const char* units[] = { "B", "KiB", "MiB", "GiB", "TiB" };
    double value = bytes;
    size_t unit = 0;
    while (value >= 1024 && unit + 1 < Y_ARRAY_SIZE(units)) {
        value /= 1024;
        ++unit;
    }
    TStringBuilder out;
    if (unit == 0) {
        out << bytes;
    } else {
        out << Sprintf("%.1f", value);
    }
    out << ' ' << units[unit];
    return out;
}

TString FormatDuration(TDuration value) {
    const ui64 seconds = value.Seconds();
    if (seconds < 60) {
        return TStringBuilder() << seconds << "s";
    }
    if (seconds < 3600) {
        return TStringBuilder() << seconds / 60 << "m " << seconds % 60 << "s";
    }
    return TStringBuilder() << seconds / 3600 << "h " << (seconds % 3600) / 60 << "m";
}

const char* TableState(const TTableProgress& table) {
    return table.Done ? "done" : (table.Started ? "running" : "pending");
}

// Status of the current (or the last) backup as JSON, polled by the page while a backup is running.

}   // namespace

TString RenderJson(const TStatus& status, TInstant now) {
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["active"] = status.IsActive();
    json["path"] = status.IsActive() ? status.CurrentBackupPath : status.LastBackupPath;
    json["started_at"] = FormatInstant(status.LastStartedAt);
    json["elapsed"] = FormatDuration((status.IsActive() ? now : status.LastFinishedAt) - status.LastStartedAt);

    if (status.Progress) {
        const auto progress = status.Progress->Get();
        json["stage"] = progress.Stage;
        json["tables_total"] = progress.TablesTotal;
        json["tables_done"] = progress.TablesDone;
        json["percent"] = progress.Percent();
        json["bytes"] = progress.BytesWritten;
        json["bytes_text"] = FormatBytes(progress.BytesWritten);
        auto& tables = json["tables"];
        tables.SetType(NJson::JSON_ARRAY);
        for (const auto& table : progress.Tables) {
            NJson::TJsonValue entry(NJson::JSON_MAP);
            entry["name"] = table.Name;
            entry["state"] = TableState(table);
            entry["bytes"] = table.Bytes;
            entry["bytes_text"] = FormatBytes(table.Bytes);
            tables.AppendValue(std::move(entry));
        }
    }

    if (status.LastFinishedAt) {
        auto& last = json["last"];
        last["success"] = status.LastSuccess;
        last["path"] = status.LastBackupPath;
        last["finished_at"] = FormatInstant(status.LastFinishedAt);
        last["bytes_text"] = FormatBytes(status.LastWrittenBytes);
        last["error"] = status.LastError;
    }
    return NJson::WriteJson(json, /*formatOutput=*/false);
}

namespace {

const TStringBuf PageStyle = R"__(
<style>
.ldb { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; max-width: 980px; margin: 0 auto; padding: 8px 12px 32px; color: #1f2933; }
.ldb a { color: #1a6fd6; text-decoration: none; }
.ldb a:hover { text-decoration: underline; }
.ldb-header { display: flex; align-items: baseline; justify-content: space-between; flex-wrap: wrap; gap: 8px; margin: 8px 0 16px; }
.ldb-header h3 { margin: 0; font-size: 22px; font-weight: 600; }
.ldb-header .ldb-sub { color: #6b7785; font-size: 13px; }
.ldb-card { background: #fff; border: 1px solid #dde3ea; border-radius: 10px; padding: 16px 20px; margin-bottom: 16px; box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04); }
.ldb-card-title { display: flex; align-items: center; gap: 10px; font-size: 15px; font-weight: 600; margin-bottom: 12px; }
.ldb-badge { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; line-height: 18px; }
.ldb-badge-idle { background: #eef1f5; color: #52606d; }
.ldb-badge-running { background: #e3efff; color: #1a56b5; }
.ldb-badge-success { background: #e1f5e8; color: #1f7a3e; }
.ldb-badge-failure { background: #fde8e8; color: #b42318; }
.ldb-alert { border-radius: 8px; padding: 10px 14px; margin-bottom: 16px; font-size: 14px; border: 1px solid transparent; }
.ldb-alert-info { background: #e3efff; border-color: #b9d4fb; color: #1a56b5; }
.ldb-alert-error { background: #fde8e8; border-color: #f7c1c1; color: #b42318; }
.ldb-progress { position: relative; height: 22px; background: #eef1f5; border-radius: 6px; overflow: hidden; margin: 6px 0 12px; }
.ldb-progress-bar { height: 100%; width: 0; background-color: #7fb3f5; transition: width .4s ease; }
.ldb-progress-bar.ldb-progress-animated { background-size: 40px 40px; background-image: linear-gradient(135deg, rgba(255,255,255,.25) 25%, transparent 25%, transparent 50%, rgba(255,255,255,.25) 50%, rgba(255,255,255,.25) 75%, transparent 75%, transparent); animation: ldb-stripes 1s linear infinite; }
.ldb-progress-bar.ldb-progress-success { background-color: #8fd8ac; }
.ldb-progress-bar.ldb-progress-failure { background-color: #f1a3a3; }
.ldb-progress-text { position: absolute; left: 0; right: 0; top: 0; line-height: 22px; text-align: center; font-size: 12px; font-weight: 600; color: #1f2933; }
@keyframes ldb-stripes { from { background-position: 40px 0; } to { background-position: 0 0; } }
.ldb-kv { display: grid; grid-template-columns: 160px 1fr; row-gap: 6px; column-gap: 12px; font-size: 14px; }
.ldb-kv dt { color: #6b7785; margin: 0; }
.ldb-kv dd { margin: 0; word-break: break-all; }
.ldb-kv code { font-size: 13px; background: #f3f5f8; padding: 1px 6px; border-radius: 4px; }
.ldb-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
.ldb-table th, .ldb-table td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #eef1f5; }
.ldb-table th { color: #6b7785; font-weight: 600; }
.ldb-table td.ldb-num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
.ldb-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: middle; background: #c5cdd6; }
.ldb-dot-running { background: #2f80ed; box-shadow: 0 0 0 3px #e3efff; }
.ldb-dot-done { background: #27ae60; }
.ldb-form { display: grid; grid-template-columns: 1fr 1fr; gap: 12px 20px; }
.ldb-field { display: flex; flex-direction: column; gap: 4px; font-size: 13px; }
.ldb-field label { color: #52606d; font-weight: 600; }
.ldb-field input[type=text], .ldb-field input[type=password], .ldb-field select { font-size: 14px; padding: 7px 10px; border: 1px solid #c5cdd6; border-radius: 6px; background: #fff; }
.ldb-field input:focus, .ldb-field select:focus { outline: none; border-color: #2f80ed; box-shadow: 0 0 0 3px #e3efff; }
.ldb-field-wide { grid-column: 1 / -1; }
.ldb-check { display: flex; align-items: center; gap: 8px; font-size: 13px; color: #52606d; align-self: end; padding-bottom: 8px; }
.ldb-actions { grid-column: 1 / -1; display: flex; align-items: center; gap: 12px; margin-top: 4px; }
.ldb-btn { font-size: 14px; font-weight: 600; padding: 8px 20px; border-radius: 6px; border: 1px solid #1a6fd6; background: #1a6fd6; color: #fff; cursor: pointer; }
.ldb-btn:hover { background: #155cb3; }
.ldb-btn:disabled { background: #a9c4ea; border-color: #a9c4ea; cursor: not-allowed; }
.ldb-hint { color: #6b7785; font-size: 13px; }
.ldb pre { background: #f3f5f8; border-radius: 6px; padding: 10px 12px; font-size: 12px; overflow-x: auto; margin: 0; }
</style>
)__";

const TStringBuf PageScript = R"__(
<script>
(function () {
    var root = document.getElementById('ldb-root');
    if (!root || root.getAttribute('data-active') !== '1') { return; }
    if (window.history && window.history.replaceState) {
        // the page may have been rendered in response to the POST; make reloads safe
        window.history.replaceState(null, '', root.getAttribute('data-page-url'));
    }
    var statusUrl = root.getAttribute('data-status-url');
    function text(id, value) { var el = document.getElementById(id); if (el) { el.textContent = value; } }
    function badge(table) {
        return '<span class="ldb-dot ldb-dot-' + table.state + '"></span>' + table.state;
    }
    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, function (c) {
            return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
        });
    }
    function apply(data) {
        if (!data.active) { window.location.reload(); return; }
        var bar = document.getElementById('ldb-bar');
        if (bar) { bar.style.width = data.percent + '%'; }
        text('ldb-bar-text', data.tables_done + ' / ' + data.tables_total + ' tables (' + data.percent + '%), ' + data.bytes_text);
        text('ldb-stage', data.stage || '');
        text('ldb-elapsed', data.elapsed || '');
        var rows = document.getElementById('ldb-rows');
        if (rows && data.tables) {
            var html = '';
            for (var i = 0; i < data.tables.length; ++i) {
                var t = data.tables[i];
                html += '<tr><td>' + escapeHtml(t.name) + '</td><td>' + badge(t) + '</td><td class="ldb-num">' + escapeHtml(t.bytes_text) + '</td></tr>';
            }
            rows.innerHTML = html;
        }
        window.setTimeout(poll, 1000);
    }
    function poll() {
        var xhr = new XMLHttpRequest();
        xhr.open('GET', statusUrl, true);
        xhr.onreadystatechange = function () {
            if (xhr.readyState !== 4) { return; }
            if (xhr.status === 200) {
                try { apply(JSON.parse(xhr.responseText)); return; } catch (e) {}
            }
            window.setTimeout(poll, 2000);
        };
        xhr.send();
    }
    window.setTimeout(poll, 1000);
})();
</script>
)__";

void RenderKv(TStringBuilder& html, TStringBuf name, const TString& value, const TString& id = {}, bool code = false) {
    html << "<dt>" << name << "</dt><dd" << (id ? TStringBuilder() << " id=\"" << id << "\"" : TStringBuilder()) << ">";
    if (code) {
        html << "<code>" << value << "</code>";
    } else {
        html << value;
    }
    html << "</dd>";
}

void RenderTablesTable(TStringBuilder& html, const TProgressSnapshot& progress) {
    html << "<table class=\"ldb-table\"><thead><tr><th>Table</th><th>State</th><th style=\"text-align:right\">Uploaded</th></tr></thead>";
    html << "<tbody id=\"ldb-rows\">";
    for (const auto& table : progress.Tables) {
        const char* state = TableState(table);
        html << "<tr><td>" << EscapeHtml(table.Name) << "</td><td><span class=\"ldb-dot ldb-dot-" << state << "\"></span>" << state
             << "</td><td class=\"ldb-num\">" << FormatBytes(table.Bytes) << "</td></tr>";
    }
    html << "</tbody></table>";
}

void RenderCurrentBackup(TStringBuilder& html, const TStatus& status, TInstant now) {
    const auto progress = status.Progress ? status.Progress->Get() : TProgressSnapshot();

    html << "<div class=\"ldb-card\"><div class=\"ldb-card-title\">Current backup ";
    if (status.IsActive()) {
        html << "<span class=\"ldb-badge ldb-badge-running\">Running</span>";
    } else if (status.LastFinishedAt) {
        html << (status.LastSuccess ? "<span class=\"ldb-badge ldb-badge-success\">Completed</span>"
                                    : "<span class=\"ldb-badge ldb-badge-failure\">Failed</span>");
    } else {
        html << "<span class=\"ldb-badge ldb-badge-idle\">Idle</span>";
    }
    html << "</div>";

    if (!status.LastStartedAt) {
        html << "<div class=\"ldb-hint\">No backups have been made since the tablet started.</div></div>";
        return;
    }

    TString barClass = "ldb-progress-bar";
    ui32 percent = progress.Percent();
    if (status.IsActive()) {
        barClass += " ldb-progress-animated";
    } else if (status.LastSuccess) {
        barClass += " ldb-progress-success";
        percent = 100;
    } else {
        barClass += " ldb-progress-failure";
    }
    html << "<div class=\"ldb-progress\"><div id=\"ldb-bar\" class=\"" << barClass << "\" style=\"width:" << percent << "%\"></div>";
    html << "<span id=\"ldb-bar-text\" class=\"ldb-progress-text\">" << progress.TablesDone << " / " << progress.TablesTotal << " tables ("
         << percent << "%), " << FormatBytes(progress.BytesWritten) << "</span></div>";

    const TInstant endTime = status.IsActive() ? now : status.LastFinishedAt;
    html << "<dl class=\"ldb-kv\">";
    RenderKv(html, "Stage", EscapeHtml(progress.Stage), "ldb-stage");
    RenderKv(html, "Path", EscapeHtml(status.IsActive() ? status.CurrentBackupPath : status.LastBackupPath), {}, true);
    RenderKv(html, "Started", FormatInstant(status.LastStartedAt));
    RenderKv(html, status.IsActive() ? "Elapsed" : "Duration", FormatDuration(endTime - status.LastStartedAt), "ldb-elapsed");
    if (!status.IsActive()) {
        RenderKv(html, "Finished", FormatInstant(status.LastFinishedAt));
        RenderKv(html, "Written", FormatBytes(status.LastWrittenBytes));
        if (!status.LastSuccess) {
            RenderKv(html, "Error", EscapeHtml(status.LastError));
        }
    }
    html << "</dl>";

    if (!progress.Tables.empty()) {
        RenderTablesTable(html, progress);
    }
    html << "</div>";
}

void RenderField(TStringBuilder& html, TStringBuf label, TStringBuf name, TStringBuf type, const TString& value, TStringBuf placeholder = {},
    bool wide = false) {
    html << "<div class=\"ldb-field" << (wide ? " ldb-field-wide" : "") << "\"><label for=\"ldb-" << name << "\">" << label << "</label>";
    html << "<input id=\"ldb-" << name << "\" type=\"" << type << "\" name=\"" << name << "\" value=\"" << EscapeHtml(value) << "\"";
    if (placeholder) {
        html << " placeholder=\"" << placeholder << "\"";
    }
    html << " autocomplete=\"off\" /></div>";
}

void RenderForm(TStringBuilder& html, ui64 tabletId, const TStatus& status, const NKikimrSchemeOp::TS3Settings& prefill) {
    const bool https = !prefill.HasScheme() || prefill.GetScheme() == NKikimrSchemeOp::TS3Settings::HTTPS;
    const bool virtualAddressing = !prefill.HasUseVirtualAddressing() || prefill.GetUseVirtualAddressing();

    html << "<div class=\"ldb-card\"><div class=\"ldb-card-title\">Make a backup</div>";
    html << "<form class=\"ldb-form\" method=\"post\" action=\"app?page=localdb_backup&amp;TabletID=" << tabletId << "\">";
    // The tablet monitoring proxy takes TabletID from the body of a POST request.
    html << "<input type=\"hidden\" name=\"TabletID\" value=\"" << tabletId
         << "\" /><input type=\"hidden\" name=\"page\" value=\"localdb_backup\" />";
    RenderField(html, "Endpoint", "endpoint", "text", prefill.GetEndpoint(), "storage.example.com:443");
    html << "<div class=\"ldb-field\"><label for=\"ldb-scheme\">Scheme</label><select id=\"ldb-scheme\" name=\"scheme\">";
    html << "<option value=\"HTTPS\"" << (https ? " selected" : "") << ">HTTPS</option>";
    html << "<option value=\"HTTP\"" << (https ? "" : " selected") << ">HTTP</option></select></div>";
    RenderField(html, "Bucket", "bucket", "text", prefill.GetBucket(), "my-bucket");
    RenderField(html, "Key prefix (optional)", "prefix", "text", prefill.GetObjectKeyPattern(), "backups/ydb");
    RenderField(html, "Region (optional)", "region", "text", prefill.GetRegion(), "us-east-1");
    html << "<label class=\"ldb-check\"><input type=\"checkbox\" name=\"virtual_addressing\" value=\"1\""
         << (virtualAddressing ? " checked" : "") << " /> Use virtual-hosted-style addressing</label>";
    RenderField(html, "Access key", "access_key", "text", prefill.GetAccessKey());
    RenderField(html, "Secret key", "secret_key", "password", {});
    html << "<div class=\"ldb-actions\"><button class=\"ldb-btn\" type=\"submit\"" << (status.IsActive() ? " disabled" : "")
         << ">Start backup</button>";
    html << "<span class=\"ldb-hint\">"
         << (status.IsActive() ? "A backup is already running; wait for it to finish."
                               : "Credentials are used only for this backup and are not stored.")
         << "</span></div>";
    html << "</form></div>";
}

}   // namespace

TString RenderPage(
    ui64 tabletId, const TStatus& status, const TString& message, bool isError, const NKikimrSchemeOp::TS3Settings& prefill, TInstant now) {
    const TString pageUrl = TStringBuilder() << "app?page=localdb_backup&TabletID=" << tabletId;

    TStringBuilder html;
    html << PageStyle;
    html << "<div class=\"ldb\" id=\"ldb-root\" data-active=\"" << (status.IsActive() ? 1 : 0) << "\" data-page-url=\"" << pageUrl
         << "\" data-status-url=\"" << pageUrl << "&amp;format=json\">";

    html << "<div class=\"ldb-header\"><div><h3>Local DB backup to S3</h3><div class=\"ldb-sub\">ColumnShard " << tabletId
         << " &middot; snapshot of the tablet local database in the system tablet backup format</div></div>";
    html << "<a href=\"app?TabletID=" << tabletId << "\">&larr; Tablet main page</a></div>";

    if (!message.empty()) {
        html << "<div class=\"ldb-alert " << (isError ? "ldb-alert-error" : "ldb-alert-info") << "\">" << EscapeHtml(message) << "</div>";
    }

    RenderCurrentBackup(html, status, now);
    RenderForm(html, tabletId, status, prefill);

    html << "<div class=\"ldb-card\"><div class=\"ldb-card-title\">Layout in the bucket</div>";
    html << "<pre>&lt;prefix&gt;/column_shard/" << tabletId << "/backup_&lt;YYYYMMDDhhmmssZ&gt;_g&lt;generation&gt;_s&lt;step&gt;/snapshot/\n"
         << "    schema.json            local database scheme (NTable::TSchemeChanges as JSON)\n"
         << "    &lt;table&gt;.json           one JSON object per row per line, for every table\n"
         << "    manifest.json          tablet type, id, generation, step and sha256 of every file\n"
         << "    manifest.json.sha256   checksum of manifest.json</pre></div>";

    html << "</div>";
    html << PageScript;
    return html;
}

// Reads S3 connection parameters from the POST form of the page.
TString ParseS3Form(const NActorsProto::TRemoteHttpInfo& query, NKikimrSchemeOp::TS3Settings& s3) {
    THashMap<TString, TString> params;
    for (const auto& kv : query.GetPostParams()) {
        params[kv.GetKey()] = Strip(kv.GetValue());
    }
    auto get = [&](const TString& key) -> TString {
        auto it = params.find(key);
        return it == params.end() ? TString() : it->second;
    };

    s3.SetEndpoint(get("endpoint"));
    s3.SetBucket(get("bucket"));
    s3.SetObjectKeyPattern(get("prefix"));
    s3.SetRegion(get("region"));
    s3.SetAccessKey(get("access_key"));
    s3.SetSecretKey(get("secret_key"));
    s3.SetScheme(get("scheme") == "HTTP" ? NKikimrSchemeOp::TS3Settings::HTTP : NKikimrSchemeOp::TS3Settings::HTTPS);
    s3.SetUseVirtualAddressing(params.contains("virtual_addressing"));

    if (s3.GetEndpoint().empty()) {
        return "S3 endpoint is required";
    }
    if (s3.GetBucket().empty()) {
        return "S3 bucket is required";
    }
    return {};
}

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
