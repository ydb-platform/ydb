#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NTabletFlatExecutor::NRecovery {

enum EEv {
    EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR),

    EvRestoreBackup = EvBegin + 1792,
    EvRestoreCompleted,

    EvBackupReaderResult,
    EvBackupInfo,
    EvReadBackup,
    EvSchemaData,
    EvSnapshotData,
    EvChangelogData,
    EvDataAck,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_EXECUTOR));

struct TEvRestoreBackup : public TEventLocal<TEvRestoreBackup, EvRestoreBackup> {
    TEvRestoreBackup(const TString& backupPath)
        : BackupPath(backupPath)
    {}

    TString BackupPath;
};

struct TEvRestoreCompleted : public TEventLocal<TEvRestoreCompleted, EvRestoreCompleted> {
    TEvRestoreCompleted(bool success, const TString& error = "")
        : Success(success)
        , Error(error)
    {}

    bool Success = false;
    TString Error;
};

struct TEvBackupReaderResult : public TEventLocal<TEvBackupReaderResult, EvBackupReaderResult> {
    TEvBackupReaderResult(bool success, const TString& error = "")
        : Success(success)
        , Error(error)
    {}

    bool Success = false;
    TString Error;
};

struct TEvBackupInfo : public TEventLocal<TEvBackupInfo, EvBackupInfo> {
    TEvBackupInfo(ui64 totalBytes)
        : TotalBytes(totalBytes)
    {}

    ui64 TotalBytes = 0;
};

struct TEvReadBackup : public TEventLocal<TEvReadBackup, EvReadBackup> {};

struct TEvSchemaData : public TEventLocal<TEvSchemaData, EvSchemaData> {
    TEvSchemaData(TString&& data)
        : Data(std::move(data))
    {}

    TString Data;
};

struct TEvSnapshotData : public TEventLocal<TEvSnapshotData, EvSnapshotData> {
    TEvSnapshotData(const TString& tableName, TVector<TString>&& lines, ui64 size)
        : TableName(tableName)
        , Lines(std::move(lines))
        , Size(size)
    {}

    TString TableName;
    TVector<TString> Lines;
    ui64 Size = 0;
};

struct TEvChangelogData : public TEventLocal<TEvChangelogData, EvChangelogData> {
    TEvChangelogData(TVector<TString>&& lines, ui64 size)
        : Lines(std::move(lines))
        , Size(size)
    {}

    TVector<TString> Lines;
    ui64 Size = 0;
};

struct TEvDataAck : public TEventLocal<TEvDataAck, EvDataAck> {
    TEvDataAck(bool success, const TString& error = "")
        : Success(success)
        , Error(error)
    {}

    bool Success = false;
    TString Error;
};

enum class ERestoreState : ui8 {
    NotStarted,
    InProgress,
    Done,
    Error,
    DoneWithWarning,
};

IActor* CreateRecoveryShard(const TActorId &tablet, TTabletStorageInfo *info);
IActor* CreateBackupReader(TActorId owner, const TString& backupPath);

} // namespace NKikimr::NTabletFlatExecutor::NRecovery
