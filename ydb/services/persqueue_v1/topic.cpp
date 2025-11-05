#include "topic.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_table.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/library/cloud_permissions/cloud_permissions.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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
    TVector<std::pair<TString, TString>> attributes;
    attributes.reserve(schemeData.GetPathDescription().UserAttributesSize());
    for (const auto& attr : schemeData.GetPathDescription().GetUserAttributes()) {
        if (std::find(allowedAttributes.begin(), allowedAttributes.end(), attr.GetKey()) != allowedAttributes.end()) {
            attributes.emplace_back(attr.GetKey(), attr.GetValue());
        }
    }
    if (!attributes.empty()) {
        //full list of permissions for compatibility. remove old permissions later.
        checker->SetEntries({{NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get(), attributes}});
    }
}

}

void TGRpcTopicService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace std::placeholders;
    using namespace Ydb::Topic;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_TOPIC_METHOD
#error SETUP_TOPIC_METHOD macro already defined
#endif

#ifdef SETUP_TOPIC_STREAM_METHOD
#error SETUP_TOPIC_STREAM_METHOD macro already defined
#endif

#define SETUP_TOPIC_METHOD(methodName, methodCallback, rlMode, requestType, auditMode, customAttributeProcessorCallback) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                                                                \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                                         \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                                                        \
        methodCallback,                                                                                   \
        rlMode,                                                                                           \
        requestType,                                                                                      \
        YDB_API_DEFAULT_COUNTER_BLOCK(topic, methodName),                                                 \
        auditMode,                                                                                        \
        COMMON,                                                                                           \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall,                                               \
        GRpcRequestProxyId_,                                                                              \
        CQ_,                                                                                              \
        nullptr,                                                                                          \
        customAttributeProcessorCallback)

#define SETUP_TOPIC_STREAM_METHOD(methodName, rlMode, requestType, auditMode, operationCallClass) \
        SETUP_RUNTIME_EVENT_STREAM_METHOD(methodName,                \
            Y_CAT(methodName, Message)::FromClient,                  \
            Y_CAT(methodName, Message)::FromServer,                  \
            rlMode,                                                  \
            requestType,                                             \
            YDB_API_DEFAULT_STREAM_COUNTER_BLOCK(topic, methodName), \
            auditMode,                                               \
            operationCallClass,                                      \
            GRpcRequestProxyId_,                                     \
            CQ_,                                                     \
            nullptr,                                                 \
            nullptr)

    SETUP_TOPIC_METHOD(CommitOffset, DoCommitOffsetRequest, RLSWITCH(Rps), TOPIC_COMMITOFFSET, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), nullptr);
    SETUP_TOPIC_METHOD(DropTopic, DoDropTopicRequest, RLSWITCH(Rps), TOPIC_DROPTOPIC, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_TOPIC_METHOD(CreateTopic, std::bind(DoCreateTopicRequest, _1, _2, ClustersCfgProvider->GetCfg()), RLSWITCH(Rps), TOPIC_CREATETOPIC, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_TOPIC_METHOD(AlterTopic, DoAlterTopicRequest, RLSWITCH(Rps), TOPIC_ALTERTOPIC, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_TOPIC_METHOD(DescribeTopic, DoDescribeTopicRequest, RLSWITCH(Rps), TOPIC_DESCRIBETOPIC, TAuditMode::NonModifying(), nullptr);
    SETUP_TOPIC_METHOD(DescribeConsumer, DoDescribeConsumerRequest, RLSWITCH(Rps), TOPIC_DESCRIBECONSUMER, TAuditMode::NonModifying(), nullptr);
    SETUP_TOPIC_METHOD(DescribePartition, DoDescribePartitionRequest, RLSWITCH(Rps), TOPIC_DESCRIBEPARTITION, TAuditMode::NonModifying(), YdsProcessAttr);
    SETUP_TOPIC_METHOD(UpdateOffsetsInTransaction, DoUpdateOffsetsInTransaction, RLSWITCH(Ru), TOPIC_UPDATEOFFSETSINTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), nullptr);
    SETUP_TOPIC_STREAM_METHOD(StreamRead, RLSWITCH(RuTopic), TOPIC_STREAMREAD, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TEvStreamTopicReadRequest);
    SETUP_TOPIC_STREAM_METHOD(StreamDirectRead, RLSWITCH(RuTopic), TOPIC_STREAMDIRECTREAD, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TEvStreamTopicDirectReadRequest);
    SETUP_TOPIC_STREAM_METHOD(StreamWrite, RLSWITCH(RuTopic), TOPIC_STREAMWRITE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TEvStreamTopicWriteRequest);

#undef SETUP_TOPIC_METHOD
#undef SETUP_TOPIC_STREAM_METHOD
}

void TGRpcTopicService::StopService() noexcept {
    TGrpcServiceBase::StopService();
}

} // namespace NKikimr::NGRpcService::V1
