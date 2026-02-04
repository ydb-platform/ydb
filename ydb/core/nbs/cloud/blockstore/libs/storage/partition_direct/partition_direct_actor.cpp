#include "partition_direct_actor.h"


namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NYdb::NBS;

////////////////////////////////////////////////////////////////////////////////

TPartitionActor::TPartitionActor(TStorageConfig storageConfig)
    : StorageConfig(std::move(storageConfig))
{
    TraceSamplePeriod = TDuration::MilliSeconds(StorageConfig.GetTraceSamplePeriod());
}

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
    request->Record.SetDDiskPoolName(StorageConfig.GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(StorageConfig.GetPersistentBufferDDiskPoolName());
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
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
        "HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        msg->Record.DebugString().data());

    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        Y_ABORT_UNLESS(msg->Record.GetResponses().size() == 1);
        const auto& response = msg->Record.GetResponses()[0];
        Y_ABORT_UNLESS(response.GetDirectBlockGroupId() == 0);
        Y_ABORT_UNLESS(response.GetActualNumVChunks() == 1);

        TVector<NBsController::TDDiskId> ddiskIds;
        TVector<NBsController::TDDiskId> persistentBufferDDiskIds;
        for (const auto& node : response.GetNodes()) {
            ddiskIds.emplace_back(node.GetDDiskId());
            persistentBufferDDiskIds.emplace_back(node.GetPersistentBufferDDiskId());
        }

        DirectBlockGroup = std::make_unique<TDirectBlockGroup>(
            1, // tabletId
            1, // generation
            ddiskIds,
            persistentBufferDDiskIds);

        DirectBlockGroup->EstablishConnections(ctx);
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "HandleControllerAllocateDDiskBlockGroupResult finished with error: %d, reason: %s",
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }

    NTabletPipe::CloseClient(ctx, BSControllerPipeClient);
}

///////////////////////////////////////////////////////////////////////////////

// Forward events to DirectBlockGroup
// TODO: Handle IO requests only after partition and direct block group are ready

void TPartitionActor::HandleDDiskConnectResult(
    const NDDisk::TEvConnectResult::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandleDDiskConnectResult(ev, ctx);
}

void TPartitionActor::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    AddTraceId(ev, ctx);
    DirectBlockGroup->HandleWriteBlocksRequest(ev, ctx);
}

void TPartitionActor::HandlePersistentBufferWriteResult(
    const NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandlePersistentBufferWriteResult(ev, ctx);
}

void TPartitionActor::HandlePersistentBufferFlushResult(
    const NDDisk::TEvFlushPersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandlePersistentBufferFlushResult(ev, ctx);
}

void TPartitionActor::HandlePersistentBufferEraseResult(
    const NDDisk::TEvErasePersistentBufferResult::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandlePersistentBufferEraseResult(ev, ctx);
}

void TPartitionActor::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    AddTraceId(ev, ctx);
    DirectBlockGroup->HandleReadBlocksRequest(ev, ctx);
}

template <typename TEvent>
void TPartitionActor::HandleReadResult(
    const typename TEvent::TPtr& ev,
    const TActorContext& ctx)
{
    DirectBlockGroup->HandleReadResult<TEvent>(ev, ctx);
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

        // Forward events to DirectBlockGroup
        HFunc(NDDisk::TEvConnectResult, HandleDDiskConnectResult);

        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocksRequest);
        HFunc(NDDisk::TEvWritePersistentBufferResult, HandlePersistentBufferWriteResult);
        HFunc(NDDisk::TEvFlushPersistentBufferResult, HandlePersistentBufferFlushResult);
        HFunc(NDDisk::TEvErasePersistentBufferResult, HandlePersistentBufferEraseResult);

        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocksRequest);
        HFunc(NDDisk::TEvReadPersistentBufferResult, HandleReadResult<NDDisk::TEvReadPersistentBufferResult>);
        HFunc(NDDisk::TEvReadResult, HandleReadResult<NDDisk::TEvReadResult>);

        default:
            LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
            break;
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
