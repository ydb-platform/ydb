#include "page.h"

#include <library/cpp/json/json_writer.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NKikimr::NColumnShard::NLocalDbBackup {

namespace {

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

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
