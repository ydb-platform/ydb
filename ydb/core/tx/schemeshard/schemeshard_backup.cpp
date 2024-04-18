#include "schemeshard_backup.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

void TSchemeShard::Handle(TEvBackup::TEvFetchBackupCollectionsRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Sender, new TEvBackup::TEvFetchBackupCollectionsResponse(), 0, ev->Cookie);
}

} // namespace NKikimr::NSchemeshard
