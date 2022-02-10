#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/rpc_calls.h>

namespace NKikimr {
namespace NGRpcService {

static TString GetSdkBuildInfo(NGrpc::IRequestContextBase* reqCtx) { 
    const auto& res = reqCtx->GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    if (res.empty()) {
        return {};
    }
    return TString{res[0]}; 
}

TGRpcMonitoringService::TGRpcMonitoringService(NActors::TActorSystem* system,
                                               TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                                               NActors::TActorId id) 
     : ActorSystem_(system)
     , Counters_(counters)
     , GRpcRequestProxyId_(id)
{
}

void TGRpcMonitoringService::InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) { 
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger)); 
}

void TGRpcMonitoringService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) { 
    Limiter_ = limiter;
}

bool TGRpcMonitoringService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcMonitoringService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcMonitoringService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) { 
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Monitoring::IN, Ydb::Monitoring::OUT, TGRpcMonitoringService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase* reqCtx) { \ 
           NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer(), GetSdkBuildInfo(reqCtx)); \
           ACTION; \
        }, &Ydb::Monitoring::V1::MonitoringService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("monitoring", #NAME))->Run(); 

    ADD_REQUEST(SelfCheck, SelfCheckRequest, SelfCheckResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvSelfCheckRequest(reqCtx));
    })

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
