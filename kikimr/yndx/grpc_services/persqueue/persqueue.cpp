#include "persqueue.h"
#include "grpc_pq_read.h"
#include "grpc_pq_write.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>


namespace NKikimr {
namespace NGRpcService {

static const ui32 PersQueueWriteSessionsMaxCount = 1000000;
static const ui32 PersQueueReadSessionsMaxCount = 100000;

TGRpcPersQueueService::TGRpcPersQueueService(NActors::TActorSystem *system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache)
    : ActorSystem(system)
    , Counters(counters)
    , SchemeCache(schemeCache)
{ }

void TGRpcPersQueueService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr) {
    CQ = cq;
    if (ActorSystem->AppData<TAppData>()->PQConfig.GetEnabled()) {
        WriteService.reset(new NGRpcProxy::TPQWriteService(GetService(), CQ, ActorSystem, SchemeCache, Counters, PersQueueWriteSessionsMaxCount));
        WriteService->InitClustersUpdater();
        ReadService.reset(new NGRpcProxy::TPQReadService(this, CQ, ActorSystem, SchemeCache, Counters, PersQueueReadSessionsMaxCount));
        SetupIncomingRequests();
    }
}

void TGRpcPersQueueService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter = limiter;
}

bool TGRpcPersQueueService::IncRequest() {
    return Limiter->Inc();
}

void TGRpcPersQueueService::DecRequest() {
    Limiter->Dec();
}

void TGRpcPersQueueService::SetupIncomingRequests() {
    WriteService->SetupIncomingRequests();
    ReadService->SetupIncomingRequests();
}

void TGRpcPersQueueService::StopService() noexcept {
    TGrpcServiceBase::StopService();
    if (WriteService.get() != nullptr) {
        WriteService->StopService();
    }
    if (ReadService.get() != nullptr) {
        ReadService->StopService();
    }
}

} // namespace NGRpcService
} // namespace NKikimr
