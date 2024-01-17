#include "persqueue.h"
#include "grpc_pq_read.h"
#include "grpc_pq_write.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include "move_topic_actor.h"

namespace NKikimr {
namespace NGRpcService {

static const ui32 PersQueueWriteSessionsMaxCount = 1000000;
static const ui32 PersQueueReadSessionsMaxCount = 100000;

void DoMovePersQueueTopic(TActorSystem* actorSystem, NYdbGrpc::IRequestContextBase* ctx) {
    actorSystem->Register(new TMoveTopicActor(ctx));
}

TGRpcPersQueueService::TGRpcPersQueueService(NActors::TActorSystem *system,
                                             TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
                                             const NActors::TActorId& schemeCache)
    : ActorSystem(system)
    , Counters(counters)
    , SchemeCache(schemeCache)
{ }

void TGRpcPersQueueService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    if (ActorSystem->AppData<TAppData>()->PQConfig.GetEnabled()) {
        WriteService.reset(new NGRpcProxy::TPQWriteService(GetService(), CQ, ActorSystem, SchemeCache, Counters, PersQueueWriteSessionsMaxCount));
        WriteService->InitClustersUpdater();
        ReadService.reset(new NGRpcProxy::TPQReadService(this, CQ, ActorSystem, SchemeCache, Counters, PersQueueReadSessionsMaxCount));
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

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB)                                                                              \
    MakeIntrusive<TGRpcRequest<NPersQueue::NAME##Request, NPersQueue::NAME##Response, TGRpcPersQueueService>>     \
        (this, &Service_, CQ,                                                                                    \
            [this](NYdbGrpc::IRequestContextBase* ctx) {                                                             \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem, ctx->GetPeer());                            \
                CB(this->ActorSystem, ctx);                                                                        \
            }, &NPersQueue::PersQueueService::AsyncService::Request##NAME ,                                   \
            #NAME, logger, getCounterBlock("operation", #NAME))->Run();


void TGRpcPersQueueService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    WriteService->SetupIncomingRequests();
    ReadService->SetupIncomingRequests();
    auto getCounterBlock = CreateCounterCb(Counters, ActorSystem);
    ADD_REQUEST(MoveTopic, DoMovePersQueueTopic);
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
