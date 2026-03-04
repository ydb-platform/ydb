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
    TEvRestoreBackup(const TString& backupPath, bool skipChecksumValidation = false)
        : BackupPath(backupPath)
        , SkipChecksumValidation(skipChecksumValidation)
    {}

    TString BackupPath;
    bool SkipChecksumValidation = false;
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
    TEvBackupInfo(ui64 totalBytes, ui32 generation, ui32 step)
        : TotalBytes(totalBytes)
        , Generation(generation)
        , Step(step)
    {}

    ui64 TotalBytes = 0;
    ui32 Generation = 0;
    ui32 Step = 0;
};

struct TEvReadBackup : public TEventLocal<TEvReadBackup, EvReadBackup> {};

struct TEvSchemaData : public TEventLocal<TEvSchemaData, EvSchemaData> {
    TEvSchemaData(TString&& data)
        : Data(std::move(data))
    {}

    TString Data;
};

struct TEvSnapshotData : public TEventLocal<TEvSnapshotData, EvSnapshotData> {
    TEvSnapshotData(const TString& tableName, TVector<TString>&& lines)
        : TableName(tableName)
        , Lines(std::move(lines))
    {}

    TString TableName;
    TVector<TString> Lines;
};

struct TEvChangelogData : public TEventLocal<TEvChangelogData, EvChangelogData> {
    TEvChangelogData(TVector<TString>&& lines)
        : Lines(std::move(lines))
    {}

    TVector<TString> Lines;
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
IActor* CreateBackupReader(TActorId owner, const TString& backupPath,
                           TTabletTypes::EType tabletType, ui64 tabletId,
                           bool skipChecksumValidation = false);

} // namespace NKikimr::NTabletFlatExecutor::NRecovery
