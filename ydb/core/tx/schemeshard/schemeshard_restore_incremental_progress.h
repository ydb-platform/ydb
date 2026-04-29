#pragma once

#include "schemeshard_info_types.h"

#include <ydb/public/api/protos/draft/ydb_backup.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NSchemeShard {

// Shared progress/status filler for the per-entry BackupCollectionRestore proto.
// Used by both Get and List handlers so they expose the same outcome.
inline void FillRestoreProgress(
        NKikimrBackup::TBackupCollectionRestore& restore,
        const TIncrementalRestoreState& restoreInfo)
{
    restore.SetId(restoreInfo.OriginalOperationId);
    restore.SetStatus(Ydb::StatusIds::SUCCESS);

    switch (restoreInfo.State) {
        case TIncrementalRestoreState::EState::Completed:
            restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_DONE);
            restore.SetProgressPercent(100);
            break;
        case TIncrementalRestoreState::EState::Failed:
            restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_DONE);
            restore.SetProgressPercent(100);
            restore.SetStatus(Ydb::StatusIds::GENERIC_ERROR);
            break;
        case TIncrementalRestoreState::EState::Finalizing:
            restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_TRANSFER_DATA);
            restore.SetProgressPercent(99);
            break;
        case TIncrementalRestoreState::EState::Running: {
            restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_TRANSFER_DATA);
            const ui32 total = restoreInfo.IncrementalBackups.size();
            if (total > 0) {
                // 1-98% range split across incrementals, with per-shard granularity within each
                float incrProgress = restoreInfo.CurrentIncrementalIdx + restoreInfo.CalcCurrentIncrementalProgress();
                restore.SetProgressPercent(1 + static_cast<ui32>(incrProgress * 97 / total));
            } else {
                restore.SetProgressPercent(1);
            }
            break;
        }
        default:
            restore.SetProgress(Ydb::Backup::RestoreProgress::PROGRESS_PREPARING);
            restore.SetProgressPercent(0);
            break;
    }
}

} // namespace NKikimr::NSchemeShard
