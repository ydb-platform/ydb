#include "topic.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include "actors/update_offsets_in_transaction_actor.h"

#include "grpc_pq_read.h"
#include "grpc_pq_write.h"
#include "grpc_pq_schema.h"
#include "services_initializer.h"

namespace NKikimr::NGRpcService::V1 {

TGRpcTopicService::TGRpcTopicService(NActors::TActorSystem *system,
                                     TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                     const NActors::TActorId& schemeCache,
                                     const NActors::TActorId& grpcRequestProxy,
                                     bool rlAllowed)
    : TGrpcServiceBase<Ydb::Topic::V1::TopicService>(system, counters, grpcRequestProxy, rlAllowed)
    , SchemeCache(schemeCache)
{
}

void TGRpcTopicService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;

    ServicesInitializer(ActorSystem_, SchemeCache, Counters_, &ClustersCfgProvider).Execute();

    if (ActorSystem_->AppData<TAppData>()->PQConfig.GetEnabled()) {
        SetupIncomingRequests(std::move(logger));
    }
}

void TGRpcTopicService::DoUpdateOffsetsInTransaction(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &)
{
    TActivationContext::AsActorContext().Register(new TUpdateOffsetsInTransactionActor(p.release()));
}

namespace {

using namespace NKikimr;

void YdsProcessAttr(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData, NGRpcService::ICheckerIface* checker) {
    static const std::vector<TString> allowedAttributes = {"folder_id", "service_account_id", "database_id"};
    //full list of permissions for compatibility. remove old permissions later.
    static const TVector<TString> permissions = {
        "ydb.databases.list",
        "ydb.databases.create",
        "ydb.databases.connect",
        "ydb.tables.select",
        "ydb.schemas.getMetadata",
        "ydb.streams.write"
    };
    TVector<std::pair<TString, TString>> attributes;
    attributes.reserve(schemeData.GetPathDescription().UserAttributesSize());
    for (const auto& attr : schemeData.GetPathDescription().GetUserAttributes()) {
        if (std::find(allowedAttributes.begin(), allowedAttributes.end(), attr.GetKey()) != allowedAttributes.end()) {
            attributes.emplace_back(attr.GetKey(), attr.GetValue());
        }
    }
    if (!attributes.empty()) {
        checker->SetEntries({{permissions, attributes}});
    }
}

}

void TGRpcTopicService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {

    using namespace std::placeholders;
    auto getCounterBlock = NKikimr::NGRpcService::CreateCounterCb(Counters_, ActorSystem_);

    {
        using TBiRequest = Ydb::Topic::StreamWriteMessage::FromClient;

        using TBiResponse = Ydb::Topic::StreamWriteMessage::FromServer;

        using TStreamGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
                    TBiRequest,
                    TBiResponse,
                    TGRpcTopicService,
                    NKikimrServices::GRPC_SERVER>;


        TStreamGRpcRequest::Start(this, this->GetService(), CQ_, &Ydb::Topic::V1::TopicService::AsyncService::RequestStreamWrite,
                    [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
                        ActorSystem_->Send(GRpcRequestProxyId_, new NKikimr::NGRpcService::TEvStreamTopicWriteRequest(context, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::RuManual), .AuditMode = TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml)}));
                    },
                    *ActorSystem_, "TopicService/StreamWrite", getCounterBlock("topic", "StreamWrite", true), nullptr
                );
    }

    {
        using TBiRequest = Ydb::Topic::StreamReadMessage::FromClient;

        using TBiResponse = Ydb::Topic::StreamReadMessage::FromServer;

        using TStreamGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
                    TBiRequest,
                    TBiResponse,
                    TGRpcTopicService,
                    NKikimrServices::GRPC_SERVER>;


        TStreamGRpcRequest::Start(this, this->GetService(), CQ_, &Ydb::Topic::V1::TopicService::AsyncService::RequestStreamRead,
                    [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
                        ActorSystem_->Send(GRpcRequestProxyId_, new NKikimr::NGRpcService::TEvStreamTopicReadRequest(context, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::RuManual), .AuditMode = TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml)}));
                    },
                    *ActorSystem_, "TopicService/StreamRead", getCounterBlock("topic", "StreamRead", true), nullptr
                );
    }

    {
        using TBiRequest = Ydb::Topic::StreamDirectReadMessage::FromClient;

        using TBiResponse = Ydb::Topic::StreamDirectReadMessage::FromServer;

        using TStreamGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
                    TBiRequest,
                    TBiResponse,
                    TGRpcTopicService,
                    NKikimrServices::GRPC_SERVER>;


        TStreamGRpcRequest::Start(this, this->GetService(), CQ_, &Ydb::Topic::V1::TopicService::AsyncService::RequestStreamDirectRead,
                    [this](TIntrusivePtr<TStreamGRpcRequest::IContext> context) {
                        ActorSystem_->Send(GRpcRequestProxyId_, new NKikimr::NGRpcService::TEvStreamTopicDirectReadRequest(context, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::RuManual), .AuditMode = TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml)}));
                    },
                    *ActorSystem_, "TopicService/StreamDirectRead", getCounterBlock("topic", "StreamDirectRead", true), nullptr
                );
    }


#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, SVC, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Topic::IN, Ydb::Topic::OUT, NGRpcService::V1::TGRpcTopicService>>(this, this->GetService(), CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Topic::V1::SVC::AsyncService::Request ## NAME, \
        "TopicService/"#NAME, logger, getCounterBlock("topic", #NAME))->Run();

    ADD_REQUEST(CommitOffset, TopicService, CommitOffsetRequest, CommitOffsetResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvCommitOffsetRequest(ctx));
    })

    ADD_REQUEST(DropTopic, TopicService, DropTopicRequest, DropTopicResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDropTopicRequest(ctx, &DoDropTopicRequest, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::Rps), .AuditMode = TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl)}));
    })

    ADD_REQUEST(CreateTopic, TopicService, CreateTopicRequest, CreateTopicResponse, {
        auto clusterCfg = ClustersCfgProvider->GetCfg();
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCreateTopicRequest(ctx, std::bind(DoCreateTopicRequest, _1, _2, clusterCfg), TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::Rps), .AuditMode = TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl)}));
    })

    ADD_REQUEST(AlterTopic, TopicService, AlterTopicRequest, AlterTopicResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvAlterTopicRequest(ctx, &DoAlterTopicRequest, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::Rps), .AuditMode = TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl)}));
    })

    ADD_REQUEST(DescribeTopic, TopicService, DescribeTopicRequest, DescribeTopicResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDescribeTopicRequest(ctx, &DoDescribeTopicRequest, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::Rps), .AuditMode = TAuditMode::NonModifying()}));
    })

    ADD_REQUEST(DescribeConsumer, TopicService, DescribeConsumerRequest, DescribeConsumerResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDescribeConsumerRequest(ctx, &DoDescribeConsumerRequest, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::Rps), .AuditMode = TAuditMode::NonModifying()}));
    })

    ADD_REQUEST(DescribePartition, TopicService, DescribePartitionRequest, DescribePartitionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDescribePartitionRequest(ctx, &DoDescribePartitionRequest, TRequestAuxSettings{.RlMode = RLSWITCH(TRateLimiterMode::Rps), .CustomAttributeProcessor = YdsProcessAttr, .AuditMode = TAuditMode::NonModifying()}));
    })
#undef ADD_REQUEST

#ifdef ADD_REQUEST_LIMIT
#error ADD_REQUEST_LIMIT macro already defined
#endif

#define ADD_REQUEST_LIMIT(NAME, CB, LIMIT_TYPE, AUDIT_MODE) \
    MakeIntrusive<TGRpcRequest<Ydb::Topic::NAME##Request, Ydb::Topic::NAME##Response, TGRpcTopicService>>     \
        (this, this->GetService(), CQ_,                                                                       \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                         \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                              \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                          \
                    new TGrpcRequestOperationCall<Ydb::Topic::NAME##Request, Ydb::Topic::NAME##Response>      \
                        (ctx, &CB, TRequestAuxSettings{                                                       \
                            .RlMode = TRateLimiterMode::LIMIT_TYPE,                                           \
                            .AuditMode = AUDIT_MODE                                                           \
                        }));                                                                                  \
            }, &Ydb::Topic::V1::TopicService::AsyncService::Request ## NAME,                                  \
            #NAME, logger, getCounterBlock("topic", #NAME))->Run();

    ADD_REQUEST_LIMIT(UpdateOffsetsInTransaction, DoUpdateOffsetsInTransaction, Ru, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml))

#undef ADD_REQUEST_LIMIT
}

void TGRpcTopicService::StopService() noexcept {
    TGrpcServiceBase::StopService();
}

} // namespace NKikimr::NGRpcService::V1
