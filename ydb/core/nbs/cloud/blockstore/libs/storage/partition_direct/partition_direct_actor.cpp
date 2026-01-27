#include "partition_direct_actor.h"


namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NYdb::NBS;

////////////////////////////////////////////////////////////////////////////////
void TPartitionActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);

    LOG_INFO(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
        "Started NBS partition: actor id %s", SelfId().ToString().data());
}

////////////////////////////////////////////////////////////////////////////////
STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocksRequest);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocksRequest);
        default:
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
            break;
    }
}

//////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Handle WriteBlocks request event: " << ev->ToString());

    auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>();
    response->Record.MutableError()->CopyFrom(MakeError(STATUS_OK));

    ctx.Send(
        ev->Sender,
        response.release(),
        0,  // flags
        ev->Cookie);
}

//////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Handle ReadBlocks request event: " << ev->ToString());

    auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
    response->Record.MutableError()->CopyFrom(MakeError(STATUS_OK));

    ctx.Send(
        ev->Sender,
        response.release(),
        0,  // flags
        ev->Cookie);
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
