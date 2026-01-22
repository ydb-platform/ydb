#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/public/api/protos/draft/ydb_nbs.pb.h>

namespace NKikimr::NGRpcService {


using TEvOpWriteBlocksRequest =
    TGrpcRequestOperationCall<NYdb::NBS::NProto::WriteBlocksRequest,
        NYdb::NBS::NProto::WriteBlocksResponse>;
using TEvOpReadBlocksRequest =
    TGrpcRequestOperationCall<NYdb::NBS::NProto::ReadBlocksRequest,
        NYdb::NBS::NProto::ReadBlocksResponse>;

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

        auto req = GetProtoRequest()->Getrequest();
        auto diskIdStr = req.GetDiskId();

        // For now diskIdStr == partition actor id
        NActors::TActorId tabletId;
        tabletId.Parse(diskIdStr.data(), diskIdStr.size());

        // Construct WriteBlocks request event from the protobuf request
        auto callContext = MakeIntrusive<NYdb::NBS::TCallContext>();
        auto request = std::make_unique<NYdb::NBS::TEvService::TEvWriteBlocksRequest>(callContext, req);

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

        auto req = GetProtoRequest()->Getrequest();
        auto diskIdStr = req.GetDiskId();

        // For now diskIdStr == partition actor id
        NActors::TActorId tabletId;
        tabletId.Parse(diskIdStr.data(), diskIdStr.size());

        // Construct ReadBlocks request event from the protobuf request
        auto callContext = MakeIntrusive<NYdb::NBS::TCallContext>();
        auto request = std::make_unique<NYdb::NBS::TEvService::TEvReadBlocksRequest>(callContext, req);

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
        ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record, ActorContext());
    }
};

void DoWriteBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TWriteBlocksRequestHandler(p.release()));
}

void DoReadBlocks(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReadBlocksRequestHandler(p.release()));
}

} // namespace NKikimr::NGRpcService
