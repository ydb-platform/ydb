#include "rpc_deferrable.h"
#include <ydb/public/api/protos/draft/ydb_nbs.pb.h>

#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct.h>
#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>

namespace NKikimr::NGRpcService {

using TEvCreatePartitionRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::CreatePartitionRequest,
        Ydb::Nbs::CreatePartitionResponse>;
using TEvDeletePartitionRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::DeletePartitionRequest,
        Ydb::Nbs::DeletePartitionResponse>;
using TEvGetLoadActorAdapterActorIdRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::GetLoadActorAdapterActorIdRequest,
        Ydb::Nbs::GetLoadActorAdapterActorIdResponse>;
using TEvListPartitionsRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::ListPartitionsRequest,
        Ydb::Nbs::ListPartitionsResponse>;

using namespace NActors;
using namespace Ydb;
using namespace NYdb::NBS::NStorage;


class TCreatePartitionRequest
    : public TRpcOperationRequestActor<TCreatePartitionRequest, TEvCreatePartitionRequest> {

public:
    TCreatePartitionRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateWork);

        // Extract parameters from request
        const auto* request = GetProtoRequest();
        const TString storagePoolName = request->GetStoragePoolName();
        const ui32 blockSize = request->GetBlockSize() ? request->GetBlockSize() : 4096;
        const ui64 blocksCount = request->GetBlocksCount() ? request->GetBlocksCount() : 32768;
        const ui32 storageMedia = request->GetStorageMedia();

        NYdb::NBS::NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetDDiskPoolName(storagePoolName);
        storageConfig.SetPersistentBufferDDiskPoolName(storagePoolName);
        // One trace per 1s should be sufficient for observability.
        storageConfig.SetTraceSamplePeriod(1000);

        NKikimrBlockStore::TVolumeConfig volumeConfig;
        volumeConfig.SetBlockSize(blockSize);
        if (storageMedia == Ydb::Nbs::StorageMediaKind::STORAGE_MEDIA_MEMORY) {
            volumeConfig.SetStorageMediaKind(NYdb::NBS::NProto::EStorageMediaKind::STORAGE_MEDIA_MEMORY);
        }

        auto* partition = volumeConfig.AddPartitions();
        partition->SetBlockCount(blocksCount);

        volumeConfig.SetBlockSize(blockSize);

        // volume identifier
        volumeConfig.SetDiskId(request->GetDiskId());
        // user folder Id, used for billing
        volumeConfig.SetFolderId("testFolderId");
        // owner information
        volumeConfig.SetProjectId("testProjectId");
        // cloud Id, used for billing
        volumeConfig.SetCloudId("testCloudId");
        volumeConfig.SetStorageMediaKind(NYdb::NBS::NProto::STORAGE_MEDIA_SSD);
        volumeConfig.SetTabletVersion(3);
        volumeConfig.SetStoragePoolName(storagePoolName);

        auto createVolumeRequest = std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(
            std::move(volumeConfig));

        LOG_DEBUG(ctx, NKikimrServices::NBS_PARTITION,
            "Sending createvolume request for volume testDiskId");

        NYdb::NBS::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(createVolumeRequest),
            0);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSSProxy::TEvCreateVolumeResponse, Handle);
        }
    }

    void Handle(TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev) {
        const auto& response = *ev->Get();
        
        LOG_DEBUG(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: received TEvCreateVolumeResponse from ss proxy: %s, status: %d, reason: %s",
            ev->Sender.ToString().data(),
            static_cast<int>(response.Status),
            response.Reason.data());

        Ydb::Nbs::CreatePartitionResult result;

        if (response.Status == NKikimrScheme::StatusSuccess) {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
        } else {
            if (!response.Reason.empty()) {
                auto issue = NYql::TIssue(response.Reason);
                Request_->RaiseIssue(issue);
            }
            Reply(Ydb::StatusIds::GENERIC_ERROR, ActorContext());
        }
    }
};

class TDeletePartitionRequest
    : public TRpcOperationRequestActor<TDeletePartitionRequest, TEvDeletePartitionRequest> {

public:
    TDeletePartitionRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateWork);

        auto tabletIdStr = GetProtoRequest()->GetTabletId();

        // Parse the string to create a TActorId
        NActors::TActorId tabletId;
        tabletId.Parse(tabletIdStr.data(), tabletIdStr.size());

        // Send poison pill to partition actor
        ctx.Send(tabletId, new TEvents::TEvPoisonPill);

        LOG_INFO(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: sent poison event to partition actor with id: %s",
            tabletId.ToString().data());

        Ydb::Nbs::DeletePartitionResult result;
        result.SetTabletId(tabletIdStr);
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ev);
        // TODO
    }
};

class TGetLoadActorAdapterActorIdRequest
    : public TRpcOperationRequestActor<TGetLoadActorAdapterActorIdRequest, TEvGetLoadActorAdapterActorIdRequest> {

public:
    TGetLoadActorAdapterActorIdRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateWork);

        auto tabletIdStr = GetProtoRequest()->GetTabletId();

        NActors::TActorId tabletId;
        tabletId.Parse(tabletIdStr.data(), tabletIdStr.size());

        ctx.Send(tabletId, new NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdRequest());
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdResponse, Handle);
            default:
                break;
        }
    }

    void Handle(NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdResponse::TPtr& ev) {
        Ydb::Nbs::GetLoadActorAdapterActorIdResult result;
        result.SetActorId(ev->Get()->ActorId);
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
    }
};

class TListPartitionsRequest
    : public TRpcOperationRequestActor<TListPartitionsRequest, TEvListPartitionsRequest> {

public:
    TListPartitionsRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        // TODO: list partition actors
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ev);
        // TODO
    }
};

void DoCreatePartition(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreatePartitionRequest(p.release()));
}

void DoDeletePartition(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDeletePartitionRequest(p.release()));
}

void DoGetLoadActorAdapterActorId(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TGetLoadActorAdapterActorIdRequest(p.release()));
}

void DoListPartitions(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TListPartitionsRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
