#include "rpc_deferrable.h"
#include <ydb/public/api/protos/draft/ydb_nbs.pb.h>

#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/grpc_services/operation_helpers.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/events.h>

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/volume_label.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>
#include <ydb/core/protos/blockstore_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/string/cast.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

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

namespace {

Ydb::StatusIds::StatusCode NbsErrorToYdbStatus(
    const NYdb::NBS::NProto::TError& error)
{
    switch (error.GetCode()) {
        case NYdb::NBS::S_ALREADY:
            return Ydb::StatusIds::ALREADY_EXISTS;
        case NYdb::NBS::E_ARGUMENT:
            return Ydb::StatusIds::BAD_REQUEST;
        case NYdb::NBS::E_NOT_FOUND:
            return Ydb::StatusIds::NOT_FOUND;
        case NYdb::NBS::E_UNAUTHORIZED:
            return Ydb::StatusIds::UNAUTHORIZED;
        case NYdb::NBS::E_NOT_IMPLEMENTED:
            return Ydb::StatusIds::UNSUPPORTED;
        case NYdb::NBS::E_ABORTED:
            return Ydb::StatusIds::ABORTED;
        case NYdb::NBS::E_REJECTED:
        case NYdb::NBS::E_TRY_AGAIN:
            return Ydb::StatusIds::UNAVAILABLE;
        case NYdb::NBS::E_TIMEOUT:
        case NYdb::NBS::E_RETRY_TIMEOUT:
            return Ydb::StatusIds::TIMEOUT;
        case NYdb::NBS::E_CANCELLED:
            return Ydb::StatusIds::CANCELLED;
        case NYdb::NBS::E_PRECONDITION_FAILED:
            return Ydb::StatusIds::PRECONDITION_FAILED;
        default:
            break;
    }

    if (FACILITY_FROM_CODE(error.GetCode()) == NYdb::NBS::FACILITY_SCHEMESHARD) {
        switch (static_cast<NKikimrScheme::EStatus>(
            STATUS_FROM_CODE(error.GetCode())))
        {
            case NKikimrScheme::StatusPathDoesNotExist:
                return Ydb::StatusIds::NOT_FOUND;
            case NKikimrScheme::StatusAlreadyExists:
            case NKikimrScheme::StatusNameConflict:
                return Ydb::StatusIds::ALREADY_EXISTS;
            default:
                break;
        }
    }

    if (!NYdb::NBS::HasError(error)) {
        return Ydb::StatusIds::SUCCESS;
    }

    return Ydb::StatusIds::GENERIC_ERROR;
}

}   // namespace

class TCreatePartitionRequest
    : public TRpcOperationRequestActor<TCreatePartitionRequest, TEvCreatePartitionRequest> {

public:
    TCreatePartitionRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateCreate);

        // Extract parameters from request
        const auto* request = GetProtoRequest();
        DiskId = request->GetDiskId();
        const TString storagePoolName = request->GetStoragePoolName();
        const ui32 blockSize = request->GetBlockSize() ? request->GetBlockSize() : 4096;
        const ui64 blocksCount = request->GetBlocksCount() ? request->GetBlocksCount() : 32768;
        const ui32 storageMedia = request->GetStorageMedia();

        NKikimrBlockStore::TVolumeConfig volumeConfig;
        volumeConfig.SetBlockSize(blockSize);
        if (storageMedia == Ydb::Nbs::StorageMediaKind::STORAGE_MEDIA_MEMORY) {
            volumeConfig.SetStorageMediaKind(NYdb::NBS::NProto::EStorageMediaKind::STORAGE_MEDIA_MEMORY);
        }

        auto* partition = volumeConfig.AddPartitions();
        partition->SetBlockCount(blocksCount);

        volumeConfig.SetBlockSize(blockSize);

        // volume identifier
        volumeConfig.SetDiskId(DiskId);
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

        YDB_LOG_DEBUG_CTX(ctx, "Sending createvolume request for volume",
            {"diskId", DiskId});

        NYdb::NBS::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(createVolumeRequest),
            0);
    }

private:
    TString DiskId;
    ui32 DescribeAttempts = 0;
    static constexpr ui32 MaxDescribeAttempts = 10;
    Ydb::StatusIds::StatusCode CreateStatus = Ydb::StatusIds::SUCCESS;

    STFUNC(StateCreate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSSProxy::TEvCreateVolumeResponse, HandleCreateVolume);
            default:
                break;
        }
    }

    STFUNC(StateDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSSProxy::TEvDescribeSchemeResponse, HandleDescribeScheme);
            hFunc(TEvents::TEvWakeup, HandleDescribeRetry);
            default:
                break;
        }
    }

    void SendDescribeScheme(const TActorContext& ctx) {
        auto describeRequest = std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(DiskId);
        NYdb::NBS::Send(ctx, MakeSSProxyServiceId(), std::move(describeRequest), 0);
    }

    void HandleDescribeRetry(TEvents::TEvWakeup::TPtr&) {
        SendDescribeScheme(TActivationContext::AsActorContext());
    }

    void HandleCreateVolume(TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();
        const auto& response = *ev->Get();
        const auto& error = response.GetError();

        YDB_LOG_DEBUG_CTX(ctx, "Grpc service: received TEvCreateVolumeResponse from ss proxy",
            {"sender", ev->Sender},
            {"error", NYdb::NBS::FormatError(error)},
            {"status", static_cast<int>(response.Status)},
            {"reason", response.Reason},
            {"tabletId", response.TabletId});

        const auto status = NbsErrorToYdbStatus(error);
        if (status != Ydb::StatusIds::SUCCESS &&
            status != Ydb::StatusIds::ALREADY_EXISTS)
        {
            Request_->RaiseIssue(NYql::TIssue(NYdb::NBS::FormatError(error)));
            Reply(status, ctx);
            return;
        }

        Ydb::Nbs::CreatePartitionResult result;
        if (response.TabletId != 0) {
            result.SetTabletId(ToString(response.TabletId));
            ReplyWithResult(status, result, ctx);
            return;
        }

        CreateStatus = status;
        Become(&TThis::StateDescribe);
        SendDescribeScheme(ctx);
    }

    void HandleDescribeScheme(TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();
        const auto& response = *ev->Get();

        Ydb::Nbs::CreatePartitionResult result;

        const auto& error = response.GetError();
        if (NYdb::NBS::HasError(error)) {
            YDB_LOG_ERROR_CTX(ctx, "CreatePartition: DescribeScheme failed after create",
                {"error", NYdb::NBS::FormatError(error)});
            Request_->RaiseIssue(NYql::TIssue(NYdb::NBS::FormatError(error)));
            Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            return;
        }

        const auto& pathDescription = response.PathDescription;
        if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeBlockStoreVolume ||
            pathDescription.GetBlockStoreVolumeDescription().PartitionsSize() == 0)
        {
            if (DescribeAttempts < MaxDescribeAttempts) {
                ++DescribeAttempts;
                YDB_LOG_DEBUG_CTX(ctx, "CreatePartition: describe returned no partitions, retry",
                    {"attempt", DescribeAttempts},
                    {"maxAttempts", MaxDescribeAttempts});
                ctx.Schedule(TDuration::MilliSeconds(200), new TEvents::TEvWakeup());
                return;
            }
            auto issue = NYql::TIssue(
                "CreatePartition: volume describe returned no partitions");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            return;
        }

        const ui64 tabletId = pathDescription.GetBlockStoreVolumeDescription()
            .GetPartitions(0)
            .GetTabletId();
        result.SetTabletId(ToString(tabletId));
        ReplyWithResult(CreateStatus, result, ctx);
    }
};

class TDeletePartitionRequest
    : public TRpcOperationRequestActor<TDeletePartitionRequest, TEvDeletePartitionRequest> {

public:
    TDeletePartitionRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateDescribeScheme);

        const auto* request = GetProtoRequest();
        const TString diskId = request->GetDiskId();

        YDB_LOG_DEBUG_CTX(ctx, "DeletePartition: sending DescribeScheme request for disk",
            {"diskId", diskId});

        auto describeRequest = std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(diskId);

        NYdb::NBS::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(describeRequest),
            0);
    }

private:
    NActors::TActorId PipeClient;
    TString DiskId;

    STFUNC(StateDescribeScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSSProxy::TEvDescribeSchemeResponse, HandleDescribeScheme);
            default:
                break;
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYdb::NBS::NBlockStore::TEvService::TEvDeletePartitionResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);
            default:
                break;
        }
    }

    STFUNC(StateDestroyVolume) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSSProxy::TEvDestroyVolumeResponse, HandleDestroyVolume);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleIgnoredDisconnect);
            default:
                break;
        }
    }

    void HandleIgnoredDisconnect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void HandleDescribeScheme(TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();
        const auto& response = *ev->Get();

        YDB_LOG_DEBUG_CTX(ctx, "DeletePartition: received DescribeScheme response",
            {"response", response.ToString()});

        const auto& error = response.GetError();
        if (NYdb::NBS::HasError(error)) {
            YDB_LOG_ERROR_CTX(ctx, "DeletePartition: DescribeScheme failed",
                {"error", NYdb::NBS::FormatError(error)});
            auto issue = NYql::TIssue(
                error.GetMessage().empty()
                    ? NYdb::NBS::FormatError(error)
                    : error.GetMessage());
            Request_->RaiseIssue(issue);
            Reply(NbsErrorToYdbStatus(error), ActorContext());
            return;
        }

        const auto& pathDescription = response.PathDescription;
        const auto pathType = pathDescription.GetSelf().GetPathType();

        if (pathType != NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
            YDB_LOG_ERROR_CTX(ctx, "DeletePartition: path is not a BlockStoreVolume",
                {"type", static_cast<int>(pathType)});
            auto issue = NYql::TIssue("Path is not a BlockStoreVolume");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::BAD_REQUEST, ActorContext());
            return;
        }

        const auto& volumeDescription = pathDescription.GetBlockStoreVolumeDescription();

        if (volumeDescription.PartitionsSize() == 0) {
            YDB_LOG_ERROR_CTX(ctx, "DeletePartition: volume has no partitions");
            auto issue = NYql::TIssue("Volume has no partitions");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::BAD_REQUEST, ActorContext());
            return;
        }

        DiskId = GetProtoRequest()->GetDiskId();
        const auto& partition = volumeDescription.GetPartitions(0);
        const ui64 tabletId = partition.GetTabletId();

        YDB_LOG_DEBUG_CTX(ctx, "DeletePartition: extracted partition tablet id, creating pipe",
            {"tabletId", tabletId},
            {"diskId", DiskId});

        Become(&TThis::StateWork);

        PipeClient = CreatePipeClient(tabletId, ctx);

        auto request = MakeHolder<NYdb::NBS::NBlockStore::TEvService::TEvDeletePartitionRequest>();
        NTabletPipe::SendData(
            ctx,
            PipeClient,
            request.Release());
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();

        if (ev->Get()->Status != NKikimrProto::OK) {
            YDB_LOG_ERROR_CTX(ctx, "DeletePartition: failed to connect to partition tablet");
            auto issue = NYql::TIssue("Failed to connect to partition tablet");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::UNAVAILABLE, ActorContext());
            return;
        }

        YDB_LOG_DEBUG_CTX(ctx, "DeletePartition: connected to partition tablet");
    }

    void HandleDisconnect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Y_UNUSED(ev);
        const auto& ctx = TActivationContext::AsActorContext();

        YDB_LOG_ERROR_CTX(ctx, "DeletePartition: pipe to partition tablet destroyed before response");
        auto issue = NYql::TIssue("Pipe to partition tablet destroyed");
        Request_->RaiseIssue(issue);
        Reply(Ydb::StatusIds::UNAVAILABLE, ActorContext());
    }

    void Handle(NYdb::NBS::NBlockStore::TEvService::TEvDeletePartitionResponse::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();

        YDB_LOG_DEBUG_CTX(ctx, "DeletePartition: received response from partition tablet");

        Become(&TThis::StateDestroyVolume);

        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = {};
        }

        if (ev->Get()->GetError().GetCode() != 0) {
            auto issue = NYql::TIssue(ev->Get()->GetErrorReason());
            Request_->RaiseIssue(issue);
            Reply(NbsErrorToYdbStatus(ev->Get()->GetError()), ActorContext());
            return;
        }

        // Wipe + BSC deallocate succeeded; drop the volume so SchemeShard
        // deletes the volume and partition tablets.

        YDB_LOG_DEBUG_CTX(ctx, "DeletePartition: sending DestroyVolume for disk",
            {"diskId", DiskId});

        auto destroyRequest = std::make_unique<TEvSSProxy::TEvDestroyVolumeRequest>(DiskId);
        NYdb::NBS::Send(ctx, MakeSSProxyServiceId(), std::move(destroyRequest), 0);
    }

    void HandleDestroyVolume(TEvSSProxy::TEvDestroyVolumeResponse::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();
        const auto& error = ev->Get()->GetError();

        if (NYdb::NBS::HasError(error)) {
            YDB_LOG_ERROR_CTX(ctx, "DeletePartition: DestroyVolume failed",
                {"diskId", DiskId},
                {"error", NYdb::NBS::FormatError(error)});
            auto issue = NYql::TIssue(
                error.GetMessage().empty()
                    ? NYdb::NBS::FormatError(error)
                    : error.GetMessage());
            Request_->RaiseIssue(issue);
            Reply(NbsErrorToYdbStatus(error), ActorContext());
            return;
        }

        YDB_LOG_DEBUG_CTX(ctx, "DeletePartition: DestroyVolume succeeded",
            {"diskId", DiskId});

        Ydb::Nbs::DeletePartitionResult result;
        result.SetDiskId(DiskId);
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
    }
};

class TGetLoadActorAdapterActorIdRequest
    : public TRpcOperationRequestActor<TGetLoadActorAdapterActorIdRequest, TEvGetLoadActorAdapterActorIdRequest> {

public:
    TGetLoadActorAdapterActorIdRequest(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateDescribeScheme);

        const auto* request = GetProtoRequest();
        const TString diskId = request->GetDiskId();

        YDB_LOG_DEBUG_CTX(ctx, "GetLoadActorAdapterActorId: sending DescribeScheme request for disk",
            {"diskId", diskId.data()});

        auto describeRequest = std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(diskId);

        NYdb::NBS::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(describeRequest),
            0);
    }

private:
    NActors::TActorId PipeClient;

    STFUNC(StateDescribeScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSSProxy::TEvDescribeSchemeResponse, HandleDescribeScheme);
            default:
                break;
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);
            default:
                break;
        }
    }

    void HandleDescribeScheme(TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();
        const auto& response = *ev->Get();

        YDB_LOG_DEBUG_CTX(ctx, "GetLoadActorAdapterActorId: received DescribeScheme",
            {"response", response});

        const auto& pathDescription = response.PathDescription;
        const auto pathType = pathDescription.GetSelf().GetPathType();

        if (pathType != NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
            YDB_LOG_ERROR_CTX(ctx, "GetLoadActorAdapterActorId: path is not a BlockStoreVolume",
                {"type", static_cast<int>(pathType)});
            auto issue = NYql::TIssue("Path is not a BlockStoreVolume");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::BAD_REQUEST, ActorContext());
            return;
        }

        const auto& volumeDescription = pathDescription.GetBlockStoreVolumeDescription();

        if (volumeDescription.PartitionsSize() == 0) {
            YDB_LOG_ERROR_CTX(ctx, "GetLoadActorAdapterActorId: volume has no partitions");
            auto issue = NYql::TIssue("Volume has no partitions");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::BAD_REQUEST, ActorContext());
            return;
        }

        const auto& partition = volumeDescription.GetPartitions(0);
        ui64 tabletId = partition.GetTabletId();

        YDB_LOG_DEBUG_CTX(ctx, "GetLoadActorAdapterActorId: extracted partition tablet id, creating pipe",
            {"tabletId", tabletId});

        Become(&TThis::StateWork);

        // Create pipe to partition tablet
        PipeClient = CreatePipeClient(tabletId, ctx);

        auto request = MakeHolder<NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdRequest>();
        // Send request to partition tablet
        NTabletPipe::SendData(
            ctx,
            PipeClient,
            request.Release());
    }

    void HandleConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();

        if (ev->Get()->Status != NKikimrProto::OK) {
            YDB_LOG_ERROR_CTX(ctx, "GetLoadActorAdapterActorId: failed to connect to partition tablet");
            auto issue = NYql::TIssue("Failed to connect to partition tablet");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::UNAVAILABLE, ActorContext());
            return;
        }

        YDB_LOG_DEBUG_CTX(ctx, "GetLoadActorAdapterActorId: connected to partition tablet");
    }

    void HandleDisconnect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Y_UNUSED(ev);
        const auto& ctx = TActivationContext::AsActorContext();

        YDB_LOG_WARN_CTX(ctx, "GetLoadActorAdapterActorId: pipe to partition tablet destroyed");
    }

    void Handle(NYdb::NBS::NBlockStore::TEvService::TEvGetLoadActorAdapterActorIdResponse::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();

        YDB_LOG_DEBUG_CTX(ctx, "GetLoadActorAdapterActorId: received response from partition tablet");

        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
        }

        Ydb::Nbs::GetLoadActorAdapterActorIdResult result;
        result.SetActorId(ev->Get()->Record.GetActorId());
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
