#include "persqueue.h"
#include "grpc_pq_read.h"
#include "grpc_pq_write.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>

namespace NKikimr {
namespace NGRpcService {

static const ui32 PersQueueWriteSessionsMaxCount = 1000000;
static const ui32 PersQueueReadSessionsMaxCount = 100000;

TGRpcPersQueueService::TGRpcPersQueueService(NActors::TActorSystem *system,
                                             TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                                             const NActors::TActorId& schemeCache)
    : ActorSystem(system)
    , Counters(counters)
    , SchemeCache(schemeCache)
{ }

void TGRpcPersQueueService::InitService(const std::vector<std::unique_ptr<grpc::ServerCompletionQueue>>& cqs, NYdbGrpc::TLoggerPtr logger, size_t index) {
    CQS.reserve(cqs.size());
    for (auto& cq: cqs) {
        CQS.push_back(cq.get());
    }

    CQ = CQS[index % cqs.size()];

    // note that we might call an overloaded InitService(), and not the one from this class
    InitService(CQ, logger);
}

void TGRpcPersQueueService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    if (ActorSystem->AppData<TAppData>()->PQConfig.GetEnabled()) {
        WriteService.reset(new NGRpcProxy::TPQWriteService(GetService(), CQS, ActorSystem, SchemeCache, Counters, PersQueueWriteSessionsMaxCount));
        WriteService->InitClustersUpdater();
        ReadService.reset(new NGRpcProxy::TPQReadService(this, CQS, ActorSystem, SchemeCache, Counters, PersQueueReadSessionsMaxCount));
        SetupIncomingRequests(logger);
    }
}

void TGRpcPersQueueService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) {
    Limiter = limiter;
}

bool TGRpcPersQueueService::IncRequest() {
    return Limiter->Inc();
}

void TGRpcPersQueueService::DecRequest() {
    Limiter->Dec();
}

void TGRpcPersQueueService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr /*logger*/) {
    WriteService->SetupIncomingRequests();
    ReadService->SetupIncomingRequests();
    auto getCounterBlock = CreateCounterCb(Counters, ActorSystem);
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
