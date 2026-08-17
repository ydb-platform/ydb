#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>

// Persist/resume helpers for full-backup tracking rows (FullBackups + FullBackupItems).

namespace NKikimr::NSchemeShard {

void TSchemeShard::FillFullBackupProto(NKikimrBackup::TFullBackup& backup, const TFullBackupInfo& info) {
    backup.SetId(info.Id);
    backup.SetStatus(Ydb::StatusIds::SUCCESS);

    if (info.StartTime != TInstant::Zero()) {
        *backup.MutableStartTime() = SecondsToProtoTimeStamp(info.StartTime.Seconds());
    }
    if (info.EndTime != TInstant::Zero()) {
        *backup.MutableEndTime() = SecondsToProtoTimeStamp(info.EndTime.Seconds());
    }

    if (info.UserSID) {
        backup.SetUserSID(*info.UserSID);
    }

    switch (info.State) {
        case TFullBackupInfo::EState::Done:
            // Done => 100% by definition.
            backup.SetProgress(Ydb::Backup::BackupProgress::PROGRESS_DONE);
            backup.SetProgressPercent(100);
            break;
        case TFullBackupInfo::EState::Transferring: {
            i32 itemsDone = 0;
            for (const auto& [_, item] : info.Items) {
                if (item.IsDone()) {
                    ++itemsDone;
                }
            }
            const i32 itemsTotal = static_cast<i32>(info.ExpectedItemCount);

            backup.SetProgressPercent(itemsTotal == 0 ? 100 : 100 * itemsDone / itemsTotal);
            backup.SetProgress(Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA);
            break;
        }
        case TFullBackupInfo::EState::Failed: {
            backup.SetProgress(Ydb::Backup::BackupProgress::PROGRESS_DONE);
            backup.SetStatus(Ydb::StatusIds::GENERIC_ERROR);
            if (!info.FinalIssues.empty()) {
                auto& issue = *backup.MutableIssues()->Add();
                issue.set_severity(NYql::TSeverityIds::S_ERROR);
                issue.set_message(info.FinalIssues);
            }
            break;
        }
        default:
            backup.SetStatus(Ydb::StatusIds::UNDETERMINED);
            backup.SetProgress(Ydb::Backup::BackupProgress::PROGRESS_UNSPECIFIED);
            break;
    }
}

void TSchemeShard::FinalizeFullBackupOnOpComplete(NIceDb::TNiceDb& db, ui64 id, const TActorContext& ctx) {
    auto* infoPtr = FullBackups.FindPtr(id);
    if (!infoPtr) {
        return;
    }
    auto& info = **infoPtr;
    if (info.IsFinished()) {
        // Already terminal (AbortUnsafe or a prior call); don't clobber Failed.
        return;
    }

    // Control op completed - op completion is the durable source of truth. Do NOT gate on
    // Items.size() >= ExpectedItemCount: item-done events can be lost across a reboot, so
    // persisted item rows are not a reliable count. Runs in the same Tx that completes the op.
    for (auto& [_, item] : info.Items) {
        if (item.State == TFullBackupInfo::TItem::EState::Transferring) {
            item.State = TFullBackupInfo::TItem::EState::Done;
            PersistFullBackupItem(db, info.Id, item);
        }
    }
    if (info.HasAnyFailed()) {
        info.State = TFullBackupInfo::EState::Failed;
        if (info.FinalIssues.empty()) {
            info.FinalIssues = "one or more items failed";
        }
    } else {
        info.State = TFullBackupInfo::EState::Done;
    }
    info.EndTime = AppData(ctx)->TimeProvider->Now();
    PersistFullBackupState(db, info);
    BCPathToFullBackup.erase(info.BackupCollectionPathId);
}

void TSchemeShard::ResumeFullBackups(const TVector<ui64>& ids, const TActorContext& ctx) {
    for (const ui64 id : ids) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TSchemeShard::ResumeFullBackups: rehydrated full-backup id# " << id
            << ", at schemeshard: " << TabletID());
        // Re-subscribe after reboot: id == control op's TxId. If the op already
        // completed before the crash, TEvNotifyTxCompletion replies immediately,
        // so a non-terminal row is always finalized on resume.
        ctx.Send(SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(id));
    }
}

void TSchemeShard::PersistFullBackup(NIceDb::TNiceDb& db, ui64 id) {
    PersistFullBackup(db, *FullBackups.at(id));
}

void TSchemeShard::PersistFullBackup(NIceDb::TNiceDb& db, const TFullBackupInfo& info) {
    PersistFullBackupState(db, info);
    for (const auto& [_, item] : info.Items) {
        PersistFullBackupItem(db, info.Id, item);
    }
}

void TSchemeShard::PersistRemoveFullBackup(NIceDb::TNiceDb& db, const TFullBackupInfo& info) {
    db.Table<Schema::FullBackups>().Key(info.Id).Delete();
    for (const auto& [pathId, _] : info.Items) {
        db.Table<Schema::FullBackupItems>().Key(info.Id, pathId.OwnerId, pathId.LocalPathId).Delete();
    }
}

void TSchemeShard::PersistFullBackupState(NIceDb::TNiceDb& db, const TFullBackupInfo& info) {
    db.Table<Schema::FullBackups>().Key(info.Id).Update(
        NIceDb::TUpdate<Schema::FullBackups::State>(static_cast<ui8>(info.State)),
        NIceDb::TUpdate<Schema::FullBackups::DomainPathOwnerId>(info.DomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::FullBackups::DomainPathId>(info.DomainPathId.LocalPathId),
        NIceDb::TUpdate<Schema::FullBackups::StartTime>(info.StartTime.Seconds()),
        NIceDb::TUpdate<Schema::FullBackups::EndTime>(info.EndTime.Seconds()),
        NIceDb::TUpdate<Schema::FullBackups::FinalIssues>(info.FinalIssues),
        NIceDb::TUpdate<Schema::FullBackups::BackupCollectionPathOwnerId>(info.BackupCollectionPathId.OwnerId),
        NIceDb::TUpdate<Schema::FullBackups::BackupCollectionLocalPathId>(info.BackupCollectionPathId.LocalPathId),
        NIceDb::TUpdate<Schema::FullBackups::ExpectedItemCount>(info.ExpectedItemCount)
    );

    if (info.UserSID) {
        db.Table<Schema::FullBackups>().Key(info.Id).Update(
            NIceDb::TUpdate<Schema::FullBackups::UserSID>(*info.UserSID)
        );
    }
}

void TSchemeShard::PersistFullBackupItem(NIceDb::TNiceDb& db, ui64 backupId, const TFullBackupInfo::TItem& item) {
    db.Table<Schema::FullBackupItems>().Key(backupId, item.PathId.OwnerId, item.PathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::FullBackupItems::State>(static_cast<ui8>(item.State))
    );
}

} // namespace NKikimr::NSchemeShard
