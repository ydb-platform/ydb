#include "persqueue.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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

    ServicesInitializer(ActorSystem_, SchemeCache, Counters_, &ClustersCfgProvider).Execute();

    if (ActorSystem_->AppData<TAppData>()->PQConfig.GetEnabled()) {
        SetupIncomingRequests(std::move(logger));
    }
}

void TGRpcPersQueueService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace std::placeholders;
    using namespace Ydb::PersQueue::V1;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_PQ_METHOD
#error SETUP_PQ_METHOD macro already defined
#endif

#ifdef SETUP_PQ_METHOD_IN_OUT
#error SETUP_PQ_METHOD_IN_OUT macro already defined
#endif

#ifdef SETUP_PQ_STREAM_METHOD
#error SETUP_PQ_STREAM_METHOD macro already defined
#endif

#define SETUP_PQ_METHOD_IN_OUT(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                           \
        inputType,                                                   \
        outputType,                                                  \
        methodCallback,                                              \
        rlMode,                                                      \
        requestType,                                                 \
        YDB_API_DEFAULT_COUNTER_BLOCK(persistent_queue, methodName), \
        auditMode,                                                   \
        COMMON,                                                      \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall,          \
        GRpcRequestProxyId_,                                         \
        CQ_,                                                         \
        nullptr,                                                     \
        nullptr)

#define SETUP_PQ_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_PQ_METHOD_IN_OUT(methodName, YDB_API_DEFAULT_REQUEST_TYPE(methodName), YDB_API_DEFAULT_RESPONSE_TYPE(methodName), methodCallback, rlMode, requestType, auditMode)

#define SETUP_PQ_STREAM_METHOD(methodName, rlMode, requestType, auditMode, operationCallClass) \
        SETUP_RUNTIME_EVENT_STREAM_METHOD(methodName,                           \
            Y_CAT(methodName, ClientMessage),                                   \
            Y_CAT(methodName, ServerMessage),                                   \
            rlMode,                                                             \
            requestType,                                                        \
            YDB_API_DEFAULT_STREAM_COUNTER_BLOCK(persistent_queue, methodName), \
            auditMode,                                                          \
            operationCallClass,                                                 \
            GRpcRequestProxyId_,                                                \
            CQ_,                                                                \
            nullptr,                                                            \
            nullptr)

    SETUP_PQ_METHOD_IN_OUT(GetReadSessionsInfo, ReadInfoRequest, ReadInfoResponse, DoPQReadInfoRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_PQ_METHOD(DropTopic, DoPQDropTopicRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_PQ_METHOD(CreateTopic, std::bind(DoPQCreateTopicRequest, _1, _2, ClustersCfgProvider->GetCfg()), RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_PQ_METHOD(AlterTopic, std::bind(DoPQAlterTopicRequest, _1, _2, ClustersCfgProvider->GetCfg()), RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_PQ_METHOD(DescribeTopic, DoPQDescribeTopicRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_PQ_METHOD(AddReadRule, DoPQAddReadRuleRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_PQ_METHOD(RemoveReadRule, DoPQRemoveReadRuleRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_PQ_STREAM_METHOD(MigrationStreamingRead, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TEvStreamPQMigrationReadRequest);
    SETUP_PQ_STREAM_METHOD(StreamingWrite, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TEvStreamPQWriteRequest);

#undef SETUP_PQ_METHOD
#undef SETUP_PQ_METHOD_IN_OUT
#undef SETUP_PQ_STREAM_METHOD
}

void TGRpcPersQueueService::StopService() noexcept {
    TGrpcServiceBase::StopService();
}

} // V1
} // namespace NGRpcService
} // namespace NKikimr
