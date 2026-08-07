#include "dbs_controller_actor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::HandleUpdateDDiskMapRequest(
    const TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle UpdateDDiskMap request" << ", tabletId: "
                                        << ev->Get()->Record.GetTabletId());

    // TODO Dummy
    Reply(
        ctx,
        *ev,
        std::make_unique<TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>(
            MakeError(S_OK)));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
