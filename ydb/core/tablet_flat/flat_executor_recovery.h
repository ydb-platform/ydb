#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NTabletFlatExecutor::NRecovery {

enum EEv {
    EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR),

    EvRestoreBackup = EvBegin + 1792,
    EvRestoreCompleted,

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

enum class ERestoreState : ui8 {
    NotStarted,
    InProgress,
    Done,
    Error,
};

IActor* CreateRecoveryShard(const TActorId &tablet, TTabletStorageInfo *info);

} // namespace NKikimr::NTabletFlatExecutor::NRecovery
