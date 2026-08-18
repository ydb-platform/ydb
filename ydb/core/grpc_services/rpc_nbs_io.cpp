#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/partition_actor_id.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/public/api/protos/draft/ydb_nbs.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

namespace NKikimr::NGRpcService {


using TEvOpWriteBlocksRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::WriteBlocksRequest,
        Ydb::Nbs::WriteBlocksResponse>;
using TEvOpReadBlocksRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::ReadBlocksRequest,
        Ydb::Nbs::ReadBlocksResponse>;

using namespace NActors;
using namespace Ydb;

namespace {

constexpr TDuration RequestTimeout = TDuration::Seconds(30);

} // namespace

class TWriteBlocksRequestHandler
    : public TRpcOperationRequestActor<TWriteBlocksRequestHandler, TEvOpWriteBlocksRequest> {

public:
    TWriteBlocksRequestHandler(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateWork);

        auto protoRequest = GetProtoRequest();
        auto diskIdStr = protoRequest->GetDiskId();

        // For now diskIdStr == partition actor id
        NActors::TActorId tabletId;
        if (!NYdb::NBS::NBlockStore::TryDeserializePartitionActorId(diskIdStr, tabletId)) {
            LOG_ERROR(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Grpc service: invalid WriteBlocks DiskId (expected [node:pool:localId:hint]): %s",
                diskIdStr.data());
            auto issue = NYql::TIssue(
                "Invalid DiskId (expected [node:pool:localId:hint])");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::BAD_REQUEST, ActorContext());
            return;
        }

        // Construct WriteBlocks request event from the protobuf request
        auto request = std::make_unique<NYdb::NBS::NBlockStore::TEvService::TEvWriteBlocksRequest>();
        request->Record.SetDiskId(protoRequest->GetDiskId());
        request->Record.SetStartIndex(protoRequest->GetStartIndex());

        const auto& srcBlocks = protoRequest->GetBlocks();
        auto* dstBlocks = request->Record.MutableBlocks();
        for (const auto& buffer : srcBlocks.GetBuffers()) {
            dstBlocks->AddBuffers(buffer);
        }

        // Send event to partition actor
        ctx.Send(new IEventHandle(
            tabletId,
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagTrackDelivery));
        ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Grpc service: sent WriteBlocksRequest to partition",
            {"partition", diskIdStr.data()});
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYdb::NBS::NBlockStore::TEvService::TEvWriteBlocksResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(NYdb::NBS::NBlockStore::TEvService::TEvWriteBlocksResponse::TPtr& ev) {
        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Grpc service: received WriteBlocksResponse from partition",
            {"partition", ev->Sender});
        ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record, ActorContext());
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        Y_UNUSED(ev);
        LOG_ERROR(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: WriteBlocksRequest undelivered");
        auto issue = NYql::TIssue("WriteBlocksRequest undelivered");
        Request_->RaiseIssue(issue);
        Reply(Ydb::StatusIds::UNAVAILABLE, ActorContext());
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        Y_UNUSED(ev);
        LOG_ERROR(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: WriteBlocksRequest timed out");
        auto issue = NYql::TIssue("WriteBlocksRequest timed out");
        Request_->RaiseIssue(issue);
        Reply(Ydb::StatusIds::TIMEOUT, ActorContext());
    }
};


class TReadBlocksRequestHandler
    : public TRpcOperationRequestActor<TReadBlocksRequestHandler, TEvOpReadBlocksRequest> {

public:
    TReadBlocksRequestHandler(IRequestOpCtx* request)
        : TRpcOperationRequestActor(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();

        Become(&TThis::StateWork);

        auto protoRequest = GetProtoRequest();
        auto diskIdStr = protoRequest->GetDiskId();

        // For now diskIdStr == partition actor id
        NActors::TActorId tabletId;
        if (!NYdb::NBS::NBlockStore::TryDeserializePartitionActorId(diskIdStr, tabletId)) {
            LOG_ERROR(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Grpc service: invalid ReadBlocks DiskId (expected [node:pool:localId:hint]): %s",
                diskIdStr.data());
            auto issue = NYql::TIssue(
                "Invalid DiskId (expected [node:pool:localId:hint])");
            Request_->RaiseIssue(issue);
            Reply(Ydb::StatusIds::BAD_REQUEST, ActorContext());
            return;
        }

        // Construct ReadBlocks request event from the protobuf request
        auto request = std::make_unique<NYdb::NBS::NBlockStore::TEvService::TEvReadBlocksRequest>();
        request->Record.SetDiskId(protoRequest->GetDiskId());
        request->Record.SetStartIndex(protoRequest->GetStartIndex());
        request->Record.SetBlocksCount(protoRequest->GetBlocksCount());

        // Send event to partition actor
        ctx.Send(new IEventHandle(
            tabletId,
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagTrackDelivery));
        ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Grpc service: sent ReadBlocksRequest to partition",
            {"partition", diskIdStr.data()});
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYdb::NBS::NBlockStore::TEvService::TEvReadBlocksResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(NYdb::NBS::NBlockStore::TEvService::TEvReadBlocksResponse::TPtr& ev) {
        YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Grpc service: received ReadBlocksResponse from partition",
            {"partition", ev->Sender});

        // Convert from NYdb::NBS::NProto::TReadBlocksResponse to Ydb::Nbs::ReadBlocksResult
        Ydb::Nbs::ReadBlocksResult result;
        const auto& srcBlocks = ev->Get()->Record.GetBlocks();
        auto* dstBlocks = result.mutable_blocks();
        for (const auto& buffer : srcBlocks.GetBuffers()) {
            dstBlocks->add_buffers(buffer);
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        Y_UNUSED(ev);
        LOG_ERROR(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: ReadBlocksRequest undelivered");
        auto issue = NYql::TIssue("ReadBlocksRequest undelivered");
        Request_->RaiseIssue(issue);
        Reply(Ydb::StatusIds::UNAVAILABLE, ActorContext());
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        Y_UNUSED(ev);
        LOG_ERROR(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: ReadBlocksRequest timed out");
        auto issue = NYql::TIssue("ReadBlocksRequest timed out");
        Request_->RaiseIssue(issue);
        Reply(Ydb::StatusIds::TIMEOUT, ActorContext());
    }
};

void DoWriteBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TWriteBlocksRequestHandler(p.release()));
}

void DoReadBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReadBlocksRequestHandler(p.release()));
}

} // namespace NKikimr::NGRpcService
