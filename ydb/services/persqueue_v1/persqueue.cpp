#include "persqueue.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include "grpc_pq_read.h"
#include "grpc_pq_write.h"
#include "grpc_pq_schema.h"
#include "services_initializer.h"

namespace NKikimr {
namespace NGRpcService {
namespace V1 {

TGRpcPersQueueService::TGRpcPersQueueService(NActors::TActorSystem *system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache,const NActors::TActorId& grpcRequestProxy, bool rlAllowed)
    : TGrpcServiceBase<Ydb::PersQueue::V1::PersQueueService>(system, counters, grpcRequestProxy, rlAllowed)
    , SchemeCache(schemeCache)
{ }

void TGRpcPersQueueService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;

    ServicesInitializer(ActorSystem_, SchemeCache, Counters_).Execute();

    if (ActorSystem_->AppData<TAppData>()->PQConfig.GetEnabled()) {
        SetupIncomingRequests(std::move(logger));
    }
}

void TGRpcPersQueueService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {

    auto getCounterBlock = NKikimr::NGRpcService::CreateCounterCb(Counters_, ActorSystem_);

    {
        using TBiRequest = Ydb::PersQueue::V1::StreamingWriteClientMessage;

        using TBiResponse = Ydb::PersQueue::V1::StreamingWriteServerMessage;

        using TStreamGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
                    TBiRequest,
                    TBiResponse,
                    TGRpcPersQueueService,
                    NKikimrServices::GRPC_SERVER>;


        TStreamGRpcRequest::Start(this, this->GetService(), CQ_, &Ydb::PersQueue::V1::PersQueueService::AsyncService::RequestStreamingWrite,
                    [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
                        ActorSystem_->Send(GRpcRequestProxyId_, new NKikimr::NGRpcService::TEvStreamPQWriteRequest(context));
                    },
                    *ActorSystem_, "PersQueueService/CreateWriteSession", getCounterBlock("persistent_queue", "WriteSession", true), nullptr
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


        TStreamGRpcRequest::Start(this, this->GetService(), CQ_, &Ydb::PersQueue::V1::PersQueueService::AsyncService::RequestMigrationStreamingRead,
                    [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
                        ActorSystem_->Send(GRpcRequestProxyId_, new NKikimr::NGRpcService::TEvStreamPQMigrationReadRequest(context));
                    },
                    *ActorSystem_, "PersQueueService/CreateMigrationReadSession", getCounterBlock("persistent_queue", "MigrationReadSession", true), nullptr
                );
    }

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, SVC, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::PersQueue::V1::IN, Ydb::PersQueue::V1::OUT, NGRpcService::V1::TGRpcPersQueueService>>(this, this->GetService(), CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::PersQueue::V1::SVC::AsyncService::Request ## NAME, \
        "PersQueueService/"#NAME, logger, getCounterBlock("persistent_queue", #NAME))->Run();

    ADD_REQUEST(GetReadSessionsInfo, PersQueueService, ReadInfoRequest, ReadInfoResponse, {
            ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvPQReadInfoRequest(ctx));
        })

    ADD_REQUEST(DropTopic, PersQueueService, DropTopicRequest, DropTopicResponse, {
            ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvPQDropTopicRequest(ctx));
        })

    ADD_REQUEST(CreateTopic, PersQueueService, CreateTopicRequest, CreateTopicResponse, {
            ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvPQCreateTopicRequest(ctx));
        })
    ADD_REQUEST(AlterTopic, PersQueueService, AlterTopicRequest, AlterTopicResponse, {
            ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvPQAlterTopicRequest(ctx));
        })
    ADD_REQUEST(DescribeTopic, PersQueueService, DescribeTopicRequest, DescribeTopicResponse, {
            ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvPQDescribeTopicRequest(ctx));
        })
    ADD_REQUEST(AddReadRule, PersQueueService, AddReadRuleRequest, AddReadRuleResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvPQAddReadRuleRequest(ctx));
    })
    ADD_REQUEST(RemoveReadRule, PersQueueService, RemoveReadRuleRequest, RemoveReadRuleResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvPQRemoveReadRuleRequest(ctx));
    })

#undef ADD_REQUEST


}

void TGRpcPersQueueService::StopService() noexcept {
    TGrpcServiceBase::StopService();
}

} // V1
} // namespace NGRpcService
} // namespace NKikimr
