#pragma once

#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/public/api/protos/ydb_udf.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

#include <memory>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDeleteModuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListModulesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeModuleRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

using IUploadModuleStreamContext = NGRpcServer::IGRpcStreamingContext<
    Ydb::Udf::UploadModuleChunk,
    Ydb::Udf::UploadModuleResponse>;

//! Serves one UploadModule stream. The actor is registered straight from the
//! accept callback of the gRPC service instead of being routed through
//! TGRpcRequestProxy, so it authenticates the caller itself through
//! TEvRequestAuthAndCheck; the request enum the proxy dispatches on is closed
//! and a new streaming method cannot join it.
NActors::IActor* CreateUploadModuleStreamActor(
    TIntrusivePtr<IUploadModuleStreamContext> context,
    const NActors::TActorId& grpcRequestProxyId);

}
}
