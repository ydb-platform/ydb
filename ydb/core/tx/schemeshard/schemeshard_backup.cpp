#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

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

} // namespace NKikimr::NSchemeshard
