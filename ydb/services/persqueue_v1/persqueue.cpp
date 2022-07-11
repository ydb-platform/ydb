#include "persqueue.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include "grpc_pq_read.h"
#include "grpc_pq_write.h"
#include "grpc_pq_schema.h"

namespace NKikimr {
namespace NGRpcService {
namespace V1 {

static const ui32 PersQueueWriteSessionsMaxCount = 1000000;
static const ui32 PersQueueReadSessionsMaxCount = 100000;

TGRpcPersQueueService::TGRpcPersQueueService(NActors::TActorSystem *system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache,const NActors::TActorId& grpcRequestProxy)
    : ActorSystem(system)
    , Counters(counters)
    , SchemeCache(schemeCache)
    , GRpcRequestProxy(grpcRequestProxy)
{ }

void TGRpcPersQueueService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ = cq;
    InitNewSchemeCacheActor();

    if (ActorSystem->AppData<TAppData>()->PQConfig.GetEnabled()) {

        IActor* writeSvc = NGRpcProxy::V1::CreatePQWriteService(SchemeCache, Counters, PersQueueWriteSessionsMaxCount);
        TActorId actorId = ActorSystem->Register(writeSvc, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
        ActorSystem->RegisterLocalService(NGRpcProxy::V1::GetPQWriteServiceActorID(), actorId);

        IActor* readSvc = NGRpcProxy::V1::CreatePQReadService(SchemeCache, NewSchemeCache, Counters, PersQueueReadSessionsMaxCount);
        actorId = ActorSystem->Register(readSvc, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
        ActorSystem->RegisterLocalService(NGRpcProxy::V1::GetPQReadServiceActorID(), actorId);

        IActor* schemaSvc = NGRpcProxy::V1::CreatePQSchemaService(SchemeCache, Counters);
        actorId = ActorSystem->Register(schemaSvc, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
        ActorSystem->RegisterLocalService(NGRpcProxy::V1::GetPQSchemaServiceActorID(), actorId);

        SetupIncomingRequests(std::move(logger));
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

void TGRpcPersQueueService::InitNewSchemeCacheActor() {
    auto appData = ActorSystem->AppData<TAppData>();
    auto cacheCounters = GetServiceCounters(Counters, "pqproxy|schemecache");
    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, cacheCounters);
    NewSchemeCache = ActorSystem->Register(CreateSchemeBoardSchemeCache(cacheConfig.Get()),
        TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
}

void TGRpcPersQueueService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {

    auto getCounterBlock = NKikimr::NGRpcService::CreateCounterCb(Counters, ActorSystem);

    {
        using TBiRequest = Ydb::PersQueue::V1::StreamingWriteClientMessage;

        using TBiResponse = Ydb::PersQueue::V1::StreamingWriteServerMessage;

        using TStreamGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
                    TBiRequest,
                    TBiResponse,
                    TGRpcPersQueueService,
                    NKikimrServices::GRPC_SERVER>;


        TStreamGRpcRequest::Start(this, this->GetService(), CQ, &Ydb::PersQueue::V1::PersQueueService::AsyncService::RequestStreamingWrite,
                    [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
                        ActorSystem->Send(GRpcRequestProxy, new NKikimr::NGRpcService::TEvStreamPQWriteRequest(context));
                    },
                    *ActorSystem, "PersQueueService/CreateWriteSession", getCounterBlock("persistent_queue", "WriteSession", true, true), nullptr
                );
    }

    {
        using TBiRequest = Ydb::PersQueue::V1::MigrationStreamingReadClientMessage;

        using TBiResponse = Ydb::PersQueue::V1::MigrationStreamingReadServerMessage;

        using TStreamGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
                    TBiRequest,
                    TBiResponse,
                    TGRpcPersQueueService,
                    NKikimrServices::GRPC_SERVER>;


        TStreamGRpcRequest::Start(this, this->GetService(), CQ, &Ydb::PersQueue::V1::PersQueueService::AsyncService::RequestMigrationStreamingRead,
                    [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
                        ActorSystem->Send(GRpcRequestProxy, new NKikimr::NGRpcService::TEvStreamPQMigrationReadRequest(context));
                    },
                    *ActorSystem, "PersQueueService/CreateMigrationReadSession", getCounterBlock("persistent_queue", "MigrationReadSession", true, true), nullptr
                );
    }

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, SVC, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::PersQueue::V1::IN, Ydb::PersQueue::V1::OUT, NGRpcService::V1::TGRpcPersQueueService>>(this, this->GetService(), CQ, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::PersQueue::V1::SVC::AsyncService::Request ## NAME, \
        "PersQueueService/"#NAME, logger, getCounterBlock("persistent_queue", #NAME))->Run();

    ADD_REQUEST(GetReadSessionsInfo, PersQueueService, ReadInfoRequest, ReadInfoResponse, {
            ActorSystem->Send(GRpcRequestProxy, new NGRpcService::TEvPQReadInfoRequest(ctx));
        })

    ADD_REQUEST(DropTopic, PersQueueService, DropTopicRequest, DropTopicResponse, {
            ActorSystem->Send(GRpcRequestProxy, new NGRpcService::TEvPQDropTopicRequest(ctx));
        })

    ADD_REQUEST(CreateTopic, PersQueueService, CreateTopicRequest, CreateTopicResponse, {
            ActorSystem->Send(GRpcRequestProxy, new NGRpcService::TEvPQCreateTopicRequest(ctx));
        })
    ADD_REQUEST(AlterTopic, PersQueueService, AlterTopicRequest, AlterTopicResponse, {
            ActorSystem->Send(GRpcRequestProxy, new NGRpcService::TEvPQAlterTopicRequest(ctx));
        })
    ADD_REQUEST(DescribeTopic, PersQueueService, DescribeTopicRequest, DescribeTopicResponse, {
            ActorSystem->Send(GRpcRequestProxy, new NGRpcService::TEvPQDescribeTopicRequest(ctx));
        })
    ADD_REQUEST(AddReadRule, PersQueueService, AddReadRuleRequest, AddReadRuleResponse, {
        ActorSystem->Send(GRpcRequestProxy, new NGRpcService::TEvPQAddReadRuleRequest(ctx));
    })
    ADD_REQUEST(RemoveReadRule, PersQueueService, RemoveReadRuleRequest, RemoveReadRuleResponse, {
        ActorSystem->Send(GRpcRequestProxy, new NGRpcService::TEvPQRemoveReadRuleRequest(ctx));
    })

#undef ADD_REQUEST


}

void TGRpcPersQueueService::StopService() noexcept {
    TGrpcServiceBase::StopService();
}

} // V1
} // namespace NGRpcService
} // namespace NKikimr
