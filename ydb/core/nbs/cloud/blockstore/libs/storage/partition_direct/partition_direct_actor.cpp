#include "partition_direct_actor.h"

#include "fast_path_service.h"
#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TPartitionActor::TPartitionActor(
    NYdb::NBS::NProto::TStorageConfig storageConfig,
    NKikimrBlockStore::TVolumeConfig volumeConfig)
    : StorageConfig(std::move(storageConfig))
    , VolumeConfig(std::move(volumeConfig))
{
    Y_ABORT_UNLESS(VolumeConfig.GetPartitions().size() == 1);
}

void TPartitionActor::Bootstrap(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "Started NBS partition: actor id %s",
        SelfId().ToString().data());

    AllocateDDiskBlockGroup(ctx);
}

void TPartitionActor::CreateBSControllerPipeClient(
    const NActors::TActorContext& ctx)
{
    BSControllerPipeClient = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
}

void TPartitionActor::AllocateDDiskBlockGroup(const NActors::TActorContext& ctx)
{
    CreateBSControllerPipeClient(ctx);

    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig.GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig.GetPersistentBufferDDiskPoolName());

    // TODO: fill with tablet id
    request->Record.SetTabletId(1);

    // TODO: add more direct block groups
    auto* query = request->Record.AddQueries();
    query->SetDirectBlockGroupId(0);

    // TODO: fill with target num v chunks. vchunk is 128MB. let us use 1 vchunk
    // since disk size will be 128MB.
    query->SetTargetNumVChunks(1);

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        msg->Record.DebugString().data());

    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        Y_ABORT_UNLESS(msg->Record.GetResponses().size() == 1);
        const auto& response = msg->Record.GetResponses()[0];
        Y_ABORT_UNLESS(response.GetDirectBlockGroupId() == 0);
        Y_ABORT_UNLESS(response.GetActualNumVChunks() == 1);

        TVector<NBsController::TDDiskId> ddiskIds;
        TVector<NBsController::TDDiskId> persistentBufferDDiskIds;
        for (const auto& node: response.GetNodes()) {
            ddiskIds.emplace_back(node.GetDDiskId());
            persistentBufferDDiskIds.emplace_back(
                node.GetPersistentBufferDDiskId());
        }

        auto fastPathService = std::make_shared<TFastPathService>(
            TActivationContext::ActorSystem(),
            SelfId().Hash(),   // tabletId
            1,                 // generation
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds),
            VolumeConfig.GetBlockSize(),
            VolumeConfig.GetPartitions(0).GetBlockCount(),
            VolumeConfig.GetStorageMediaKind(),   // storageMedia
            StorageConfig,
            AppData()->Counters);

        LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, fastPathService);

        {
            auto service = GetNbsService();

            TString diskId = VolumeConfig.GetDiskId();
            ui32 blockSize = VolumeConfig.GetBlockSize();
            ui64 blockCount = 0;
            for (const auto& p: VolumeConfig.GetPartitions()) {
                blockCount += p.GetBlockCount();
            }

            // Fix me
            diskId = "nbs-1";
            blockSize = 4096;
            blockCount = 32768;

            TString socketPath = "/tmp/" + diskId + ".sock";
            NVhost::TStorageOptions options{
                .DiskId = diskId,
                .ClientId = "client-1",
                .BlockSize = blockSize,
                .BlocksCount = blockCount,
                .VhostQueuesCount = 1};
            service->VhostServer->StartEndpoint(
                std::move(socketPath),
                fastPathService,
                options);
        }

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "Started NBS partition LoadActorAdapter: actor id %s",
            LoadActorAdapter.ToString().data());
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "HandleControllerAllocateDDiskBlockGroupResult finished with "
            "error: %d, reason: %s",
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }

    NTabletPipe::CloseClient(ctx, BSControllerPipeClient);
}

void TPartitionActor::HandleGetLoadActorAdapterActorId(
    const TEvService::TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvGetLoadActorAdapterActorIdResponse>();
    response->ActorId = LoadActorAdapter.ToString();
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
            HandleControllerAllocateDDiskBlockGroupResult);
        HFunc(
            TEvService::TEvGetLoadActorAdapterActorIdRequest,
            HandleGetLoadActorAdapterActorId);

        default:
            LOG_DEBUG_S(
                TActivationContext::AsActorContext(),
                NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                                         << " event: " << ev->ToString());
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
