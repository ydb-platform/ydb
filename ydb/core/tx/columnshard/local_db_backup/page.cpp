#include "page.h"

#include <library/cpp/json/json_writer.h>
#include <util/stream/format.h>

namespace NKikimr::NColumnShard::NLocalDbBackup {

namespace {

TString FormatInstant(TInstant value) {
    return value ? value.ToStringUpToSeconds() : TString("never");
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
    json["elapsed"] = ToString(HumanReadable((status.IsActive() ? now : status.LastFinishedAt) - status.LastStartedAt));

    if (status.LastStartedAt) {
        const auto& progress = status.Progress.Snapshot;
        json["stage"] = progress.Stage;
        json["tables_total"] = progress.TablesTotal;
        json["tables_done"] = progress.TablesDone;
        json["percent"] = progress.Percent();
        json["bytes"] = progress.BytesWritten;
        json["bytes_text"] = ToString(HumanReadableSize(progress.BytesWritten, SF_BYTES));
        auto& tables = json["tables"];
        tables.SetType(NJson::JSON_ARRAY);
        for (const auto& table : progress.Tables) {
            NJson::TJsonValue entry(NJson::JSON_MAP);
            entry["name"] = table.Name;
            entry["state"] = TableState(table);
            entry["bytes"] = table.Bytes;
            entry["bytes_text"] = ToString(HumanReadableSize(table.Bytes, SF_BYTES));
            tables.AppendValue(std::move(entry));
        }
    }

    if (status.LastFinishedAt) {
        auto& last = json["last"];
        last["success"] = status.LastSuccess;
        last["path"] = status.LastBackupPath;
        last["finished_at"] = FormatInstant(status.LastFinishedAt);
        last["bytes_text"] = ToString(HumanReadableSize(status.LastWrittenBytes, SF_BYTES));
        last["error"] = status.LastError;
    }
    return NJson::WriteJson(json, /*formatOutput=*/false);
}

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
