#pragma once

#include "events.h"

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NColumnShard::NLocalDbBackup {

struct TTableProgress {
    TString Name;
    ui64 Bytes = 0;
    bool Started = false;
    bool Done = false;
};

struct TProgressSnapshot {
    TString Stage;
    ui64 TablesTotal = 0;
    ui64 TablesDone = 0;
    ui64 BytesWritten = 0;
    TVector<TTableProgress> Tables;   // sorted by name

    ui32 Percent() const {
        return TablesTotal ? static_cast<ui32>(TablesDone * 100 / TablesTotal) : 0;
    }
};

// Progress of the current backup. Touched only by the actor that owns TStatus:
// the writer reports changes with TEvBackupProgress, and that actor applies them here.
class TProgress {
public:
    void Apply(const TEvBackupProgress& ev) {
        switch (ev.Kind) {
            case TEvBackupProgress::EKind::Init:
                Init(ev.Tables);
                break;
            case TEvBackupProgress::EKind::Stage:
                Snapshot.Stage = ev.Stage;
                break;
            case TEvBackupProgress::EKind::Bytes:
                Snapshot.BytesWritten += ev.Bytes;
                break;
            case TEvBackupProgress::EKind::Chunk:
                OnChunk(ev.TableId, ev.Bytes);
                break;
            case TEvBackupProgress::EKind::TableDone:
                OnTableDone(ev.TableId);
                break;
        }
    }

    void Reset() {
        Snapshot = {};
        Index.clear();
    }

    TProgressSnapshot Snapshot;

private:
    void Init(const TMap<TTableId, TString>& tables) {
        Snapshot.Tables.clear();
        Index.clear();
        for (const auto& [tableId, name] : tables) {
            Index[tableId] = Snapshot.Tables.size();
            Snapshot.Tables.push_back(TTableProgress{ .Name = name });
        }
        Snapshot.TablesTotal = Snapshot.Tables.size();
    }

    void OnChunk(TTableId tableId, ui64 bytes) {
        Snapshot.BytesWritten += bytes;
        if (auto* table = Find(tableId)) {
            table->Started = true;
            table->Bytes += bytes;
        }
    }

    void OnTableDone(TTableId tableId) {
        if (auto* table = Find(tableId); table && !table->Done) {
            table->Started = true;
            table->Done = true;
            ++Snapshot.TablesDone;
        }
    }

    TTableProgress* Find(TTableId tableId) {
        auto it = Index.find(tableId);
        return it == Index.end() ? nullptr : &Snapshot.Tables[it->second];
    }

    THashMap<TTableId, size_t> Index;
};

// In-memory status of the manually triggered local DB backup of a ColumnShard tablet.
struct TStatus {
    NActors::TActorId WriterActorId;   // non-empty while a backup is in progress
    TString CurrentBackupPath;
    TProgress Progress;   // of the current (or the last) backup

    TInstant LastStartedAt;
    TInstant LastFinishedAt;
    TString LastBackupPath;
    bool LastSuccess = false;
    TString LastError;
    ui64 LastWrittenBytes = 0;

    bool IsActive() const {
        return !!WriterActorId;
    }

    void OnStarted(const NActors::TActorId& writer, const TString& backupPath, TInstant now) {
        WriterActorId = writer;
        CurrentBackupPath = backupPath;
        Progress.Reset();
        Progress.Snapshot.Stage = "Starting";
        LastStartedAt = now;
    }

    void OnFinished(bool success, const TString& error, ui64 writtenBytes, TInstant now) {
        WriterActorId = {};
        LastBackupPath = CurrentBackupPath;
        CurrentBackupPath.clear();
        LastFinishedAt = now;
        LastSuccess = success;
        LastError = error;
        LastWrittenBytes = writtenBytes;
    }
};

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
