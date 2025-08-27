#include "schemeshard_backup.h"

#include "schemeshard_impl.h"
#include "schemeshard_continuous_backup_cleaner.h"

namespace NKikimr::NSchemeShard {

void TSchemeShard::Handle(TEvBackup::TEvFetchBackupCollectionsRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Sender, new TEvBackup::TEvFetchBackupCollectionsResponse(), 0, ev->Cookie);
}

void TSchemeShard::Handle(TEvBackup::TEvListBackupCollectionsRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Sender, new TEvBackup::TEvListBackupCollectionsResponse(), 0, ev->Cookie);
}

void TSchemeShard::Handle(TEvBackup::TEvCreateBackupCollectionRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Sender, new TEvBackup::TEvCreateBackupCollectionResponse(), 0, ev->Cookie);
}

void TSchemeShard::Handle(TEvBackup::TEvReadBackupCollectionRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Sender, new TEvBackup::TEvReadBackupCollectionResponse(), 0, ev->Cookie);
}

void TSchemeShard::Handle(TEvBackup::TEvUpdateBackupCollectionRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Sender, new TEvBackup::TEvUpdateBackupCollectionResponse(), 0, ev->Cookie);
}

void TSchemeShard::Handle(TEvBackup::TEvDeleteBackupCollectionRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Sender, new TEvBackup::TEvDeleteBackupCollectionResponse(), 0, ev->Cookie);
}

void TSchemeShard::Handle(TEvBackup::TEvGetIncrementalBackupRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxGet(ev), ctx);
}

void TSchemeShard::Handle(TEvBackup::TEvForgetIncrementalBackupRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForget(ev), ctx);
}

void TSchemeShard::Handle(TEvBackup::TEvListIncrementalBackupsRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxList(ev), ctx);
}

void TSchemeShard::Handle(TEvBackup::TEvGetBackupCollectionRestoreRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxGetRestore(ev), ctx);
}

void TSchemeShard::Handle(TEvBackup::TEvForgetBackupCollectionRestoreRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForgetRestore(ev), ctx);
}

void TSchemeShard::Handle(TEvBackup::TEvListBackupCollectionRestoresRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxListRestore(ev), ctx);
}

void TSchemeShard::Handle(TEvPersQueue::TEvOffloadStatus::TPtr& ev, const TActorContext&) {
    Execute(CreateTxProgress(ev));
}

void TSchemeShard::Handle(TEvPrivate::TEvContinuousBackupCleanerResult::TPtr& ev, const TActorContext&) {
    Execute(CreateTxProgress(ev));
}

void TSchemeShard::ResumeIncrementalBackups(const TVector<ui64>& incrementalBackupsIds, const TActorContext& ctx) {
    for (const ui64 id : incrementalBackupsIds) {
        Execute(CreateTxProgress(id), ctx);
    }
}

void TSchemeShard::PersistIncrementalBackup(NIceDb::TNiceDb& db, ui64 id) {
    PersistIncrementalBackup(db, *IncrementalBackups.at(id));
}

void TSchemeShard::PersistIncrementalBackup(NIceDb::TNiceDb& db, const TIncrementalBackupInfo& info) {
    PersistIncrementalBackupState(db, info);
    for (const auto& [_, item] : info.Items) {
        PersistIncrementalBackupItem(db, info.Id, item);
    }
}

void TSchemeShard::PersistRemoveIncrementalBackup(NIceDb::TNiceDb& db, const TIncrementalBackupInfo& info) {
    db.Table<Schema::IncrementalBackups>().Key(info.Id).Delete();
    for (const auto& [pathId, _] : info.Items) {
        db.Table<Schema::IncrementalBackupItems>().Key(info.Id, pathId.OwnerId, pathId.LocalPathId).Delete();
    }
}

void TSchemeShard::PersistIncrementalBackupState(NIceDb::TNiceDb& db, const TIncrementalBackupInfo& info) {
    db.Table<Schema::IncrementalBackups>().Key(info.Id).Update(
        NIceDb::TUpdate<Schema::IncrementalBackups::State>(static_cast<ui8>(info.State)),
        NIceDb::TUpdate<Schema::IncrementalBackups::DomainPathOwnerId>(info.DomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::IncrementalBackups::DomainPathId>(info.DomainPathId.LocalPathId),
        NIceDb::TUpdate<Schema::IncrementalBackups::StartTime>(info.StartTime.Seconds()),
        NIceDb::TUpdate<Schema::IncrementalBackups::EndTime>(info.EndTime.Seconds())
    );

    if (info.UserSID) {
        db.Table<Schema::IncrementalBackups>().Key(info.Id).Update(
            NIceDb::TUpdate<Schema::IncrementalBackups::UserSID>(*info.UserSID)
        );
    }
}

void TSchemeShard::PersistIncrementalBackupItem(NIceDb::TNiceDb& db, ui64 backupId, const TIncrementalBackupInfo::TItem& item) {
    db.Table<Schema::IncrementalBackupItems>().Key(backupId, item.PathId.OwnerId, item.PathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::IncrementalBackupItems::State>(static_cast<ui8>(item.State))
    );
}

} // namespace NKikimr::NSchemeshard
