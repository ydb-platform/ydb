#include "dbs_controller_actor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::HandleGetNodesForPartitionRequest(
    const TEvDbsControllerPrivate::TEvGetNodesForPartitionRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle GetNodesForPartition request"
            << ", tabletId: " << ev->Get()->Record.GetPartitionTabletId());

    // TODO Dummy
    Reply(
        ctx,
        *ev,
        std::make_unique<
            TEvDbsControllerPrivate::TEvGetNodesForPartitionResponse>(
            MakeError(S_OK)));
}

void TDbsControllerActor::HandleGetPartitionsForNodeRequest(
    const TEvDbsControllerPrivate::TEvGetPartitionsForNodeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "Handle GetPartitionsForNode request" << ", nodeId: "
                                              << ev->Get()->Record.GetNodeId());

    // TODO Dummy
    Reply(
        ctx,
        *ev,
        std::make_unique<
            TEvDbsControllerPrivate::TEvGetPartitionsForNodeResponse>(
            MakeError(S_OK)));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
