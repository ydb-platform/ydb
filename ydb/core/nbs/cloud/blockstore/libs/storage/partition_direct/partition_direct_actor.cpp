#include "partition_direct_actor.h"


namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NYdb::NBS;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);

    LOG_INFO(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
        "Started NBS partition: actor id %s", SelfId().ToString().data());
        
    AllocateDDiskBlockGroup(ctx);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::CreateBSControllerPipeClient(const TActorContext& ctx)
{
    BSControllerPipeClient = ctx.Register(NTabletPipe::CreateClient(
        ctx.SelfID,
        MakeBSControllerID()));
}

void TPartitionActor::AllocateDDiskBlockGroup(const TActorContext& ctx)
{    
    CreateBSControllerPipeClient(ctx);

    auto request = std::make_unique<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    // TODO: get from config
    request->Record.SetDDiskPoolName("ddp1");
    request->Record.SetPersistentBufferDDiskPoolName("ddp1");
    // TODO: fill with tablet id
    request->Record.SetTabletId(1);

    // TODO: add more direct block groups
    auto *query = request->Record.AddQueries();
    query->SetDirectBlockGroupId(0);
    // TODO: fill with target num v chunks. vchunk is 128MB. let us use 1 vchunk since disk size will be 128MB.
    query->SetTargetNumVChunks(1);

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const TActorContext& ctx)
{
    auto* res = ev->Get();
    if (res->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "Responses size: %d",
            res->Record.GetResponses().size());
        Y_ABORT_UNLESS(res->Record.GetResponses().size() == 1);
        const auto& response = res->Record.GetResponses()[0];
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "DirectBlockGroupId: %d",
            response.GetDirectBlockGroupId());
        Y_ABORT_UNLESS(response.GetDirectBlockGroupId() == 0);
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "ActualNumVChunks: %d",
            response.GetActualNumVChunks());
        Y_ABORT_UNLESS(response.GetActualNumVChunks() == 1);
        
        TVector<NBsController::TDDiskId> ddiskIds;
        TVector<NBsController::TDDiskId> persistentBufferDDiskIds;
        for (const auto& node : response.GetNodes()) {
            ddiskIds.emplace_back(node.GetDDiskId());
            // persistentBufferDDiskIds.emplace_back(node.GetPersistentBufferDDiskId());
        }

        DirectBlockGroup = std::make_unique<TDirectBlockGroup>(
            1, // tabletId
            1, // generation
            ddiskIds,
            persistentBufferDDiskIds);

        DirectBlockGroup->EstablishConnections(ctx);
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "ControllerAllocateDDiskBlockGroupResult finished with error: %s, reason: %s",
            res->Record.GetStatus(),
            res->Record.GetErrorReason().data());
    }

    NTabletPipe::CloseClient(ctx, BSControllerPipeClient);
}

void TPartitionActor::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Handle WriteBlocks request event: " << ev->ToString());

    DirectBlockGroup->HandleWriteBlocksRequest(ev, ctx);
}

void TPartitionActor::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Handle ReadBlocks request event: " << ev->ToString());

    DirectBlockGroup->HandleReadBlocksRequest(ev, ctx);
}

///////////////////////////////////////////////////////////////////////////////

// Forward events to DirectBlockGroup

void TPartitionActor::HandleDDiskConnectResult(
    const NDDisk::TEvDDiskConnectResult::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandleDDiskConnectResult(ev, ctx);
}

void TPartitionActor::HandleDDiskWriteResult(
    const NDDisk::TEvDDiskWriteResult::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandleDDiskWriteResult(ev, ctx);
}

void TPartitionActor::HandleDDiskReadResult(
    const NDDisk::TEvDDiskReadResult::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandleDDiskReadResult(ev, ctx);
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
        HFunc(TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult, HandleControllerAllocateDDiskBlockGroupResult);
        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocksRequest);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocksRequest);
        
        // Forward events to DirectBlockGroup
        HFunc(NDDisk::TEvDDiskConnectResult, HandleDDiskConnectResult);
        HFunc(NDDisk::TEvDDiskWriteResult, HandleDDiskWriteResult);
        HFunc(NDDisk::TEvDDiskReadResult, HandleDDiskReadResult);

        default:
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
            break;
    }
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
