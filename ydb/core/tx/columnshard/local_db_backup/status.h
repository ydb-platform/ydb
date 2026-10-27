#pragma once

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <memory>

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

// Progress of a running backup, shared between the S3 writer actor (producer) and the
// tablet's monitoring page (consumer). Thread-safe.
class TProgress {
public:
    using TPtr = std::shared_ptr<TProgress>;

    void Init(const TMap<ui32, TString>& tables) {
        TGuard<TMutex> guard(Lock);
        Data.Tables.clear();
        Index.clear();
        for (const auto& [tableId, name] : tables) {
            Index[tableId] = Data.Tables.size();
            Data.Tables.push_back(TTableProgress{ .Name = name });
        }
        Data.TablesTotal = Data.Tables.size();
    }

    void SetStage(const TString& stage) {
        TGuard<TMutex> guard(Lock);
        Data.Stage = stage;
    }

    void AddBytes(ui64 bytes) {
        TGuard<TMutex> guard(Lock);
        Data.BytesWritten += bytes;
    }

    void OnChunk(ui32 tableId, ui64 bytes) {
        TGuard<TMutex> guard(Lock);
        Data.BytesWritten += bytes;
        if (auto* table = Find(tableId)) {
            table->Started = true;
            table->Bytes += bytes;
        }
    }

    void OnTableDone(ui32 tableId) {
        TGuard<TMutex> guard(Lock);
        if (auto* table = Find(tableId); table && !table->Done) {
            table->Started = true;
            table->Done = true;
            ++Data.TablesDone;
        }
    }

    TProgressSnapshot Get() const {
        TGuard<TMutex> guard(Lock);
        return Data;
    }

private:
    TTableProgress* Find(ui32 tableId) {
        auto it = Index.find(tableId);
        return it == Index.end() ? nullptr : &Data.Tables[it->second];
    }

private:
    mutable TMutex Lock;
    TProgressSnapshot Data;
    THashMap<ui32, size_t> Index;
};

// In-memory status of the manually triggered local DB backup of a ColumnShard tablet.
struct TStatus {
    NActors::TActorId WriterActorId;   // non-empty while a backup is in progress
    TString CurrentBackupPath;
    TProgress::TPtr Progress;   // of the current (or the last) backup

    TInstant LastStartedAt;
    TInstant LastFinishedAt;
    TString LastBackupPath;
    bool LastSuccess = false;
    TString LastError;
    ui64 LastWrittenBytes = 0;

    bool IsActive() const {
        return !!WriterActorId;
    }

    void OnStarted(const NActors::TActorId& writer, const TString& backupPath, const TProgress::TPtr& progress, TInstant now) {
        WriterActorId = writer;
        CurrentBackupPath = backupPath;
        Progress = progress;
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
