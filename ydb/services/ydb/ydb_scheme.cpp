#include "ydb_scheme.h"

#include <ydb/core/grpc_services/grpc_helper.h>

#include <ydb/core/grpc_services/service_scheme.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYdbSchemeService::TGRpcYdbSchemeService(NActors::TActorSystem *system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id)
{ }

void TGRpcYdbSchemeService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYdbSchemeService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcYdbSchemeService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYdbSchemeService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcYdbSchemeService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response, TGRpcYdbSchemeService>> \
        (this, &Service_, CQ_,                                                                                  \
            [this](NGrpc::IRequestContextBase *ctx) {                                                           \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestOperationCall<Ydb::Scheme::NAME##Request, Ydb::Scheme::NAME##Response>      \
                        (ctx, &CB, TRequestAuxSettings{TRateLimiterMode::Rps, nullptr}));                       \
            }, &Ydb::Scheme::V1::SchemeService::AsyncService::Request ## NAME,                                  \
            #NAME, logger, getCounterBlock("scheme", #NAME))->Run();

    ADD_REQUEST(MakeDirectory, DoMakeDirectoryRequest)
    ADD_REQUEST(RemoveDirectory, DoRemoveDirectoryRequest)
    ADD_REQUEST(ListDirectory, DoListDirectoryRequest)
    ADD_REQUEST(DescribePath, DoDescribePathRequest)
    ADD_REQUEST(ModifyPermissions, DoModifyPermissionsRequest)
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
