#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/public/api/protos/draft/ydb_nbs.pb.h>

namespace NKikimr::NGRpcService {


using TEvOpWriteBlocksRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::WriteBlocksRequest,
        Ydb::Nbs::WriteBlocksResponse>;
using TEvOpReadBlocksRequest =
    TGrpcRequestOperationCall<Ydb::Nbs::ReadBlocksRequest,
        Ydb::Nbs::ReadBlocksResponse>;

using namespace NActors;
using namespace Ydb;

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
        tabletId.Parse(diskIdStr.data(), diskIdStr.size());

        // Construct WriteBlocks request event from the protobuf request
        auto request = std::make_unique<NYdb::NBS::TEvService::TEvWriteBlocksRequest>();
        request->Record.SetDiskId(protoRequest->GetDiskId());
        request->Record.SetStartIndex(protoRequest->GetStartIndex());

        const auto& srcBlocks = protoRequest->GetBlocks();
        auto* dstBlocks = request->Record.MutableBlocks();
        for (const auto& buffer : srcBlocks.GetBuffers()) {
            dstBlocks->AddBuffers(buffer);
        }

        // Send event to partition actor
        ctx.Send(new IEventHandle(tabletId, ctx.SelfID, request.release()));

        LOG_DEBUG(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: sent WriteBlocksRequest to partition: %s",
            diskIdStr.data());
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYdb::NBS::TEvService::TEvWriteBlocksResponse, Handle);
        }
    }

    void Handle(NYdb::NBS::TEvService::TEvWriteBlocksResponse::TPtr& ev) {
        LOG_DEBUG(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: received WriteBlocksResponse from partition: %s",
            ev->Sender.ToString().data());
        ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record, ActorContext());
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
        tabletId.Parse(diskIdStr.data(), diskIdStr.size());

        // Construct ReadBlocks request event from the protobuf request
        auto request = std::make_unique<NYdb::NBS::TEvService::TEvReadBlocksRequest>();
        request->Record.SetDiskId(protoRequest->GetDiskId());
        request->Record.SetStartIndex(protoRequest->GetStartIndex());
        request->Record.SetBlocksCount(protoRequest->GetBlocksCount());

        // Send event to partition actor
        ctx.Send(new IEventHandle(tabletId, ctx.SelfID, request.release()));

        LOG_DEBUG(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: sent ReadBlocksRequest to partition: %s",
            diskIdStr.data());
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NYdb::NBS::TEvService::TEvReadBlocksResponse, Handle);
        }
    }

    void Handle(NYdb::NBS::TEvService::TEvReadBlocksResponse::TPtr& ev) {
        LOG_DEBUG(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
            "Grpc service: received ReadBlocksResponse from partition: %s",
            ev->Sender.ToString().data());

        // Convert from NYdb::NBS::NProto::TReadBlocksResponse to Ydb::Nbs::ReadBlocksResult
        Ydb::Nbs::ReadBlocksResult result;
        const auto& srcBlocks = ev->Get()->Record.GetBlocks();
        auto* dstBlocks = result.mutable_blocks();
        for (const auto& buffer : srcBlocks.GetBuffers()) {
            dstBlocks->add_buffers(buffer);
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
    }
};

void DoWriteBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TWriteBlocksRequestHandler(p.release()));
}

void DoReadBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReadBlocksRequestHandler(p.release()));
}

} // namespace NKikimr::NGRpcService
