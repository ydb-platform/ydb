#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_monitoring.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcMonitoringService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Monitoring;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    ReportSdkBuildInfo();

#ifdef SETUP_MON_METHOD
#error SETUP_MON_METHOD macro already defined
#endif

#define SETUP_MON_METHOD(methodName, methodCallback, rlMode, requestType, auditMode, operationCallClass) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                                                               \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                                        \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                                                       \
        methodCallback,                                                                                  \
        rlMode,                                                                                          \
        requestType,                                                                                     \
        YDB_API_DEFAULT_COUNTER_BLOCK(monitoring, methodName),                                           \
        auditMode,                                                                                       \
        COMMON,                                                                                          \
        operationCallClass,                                                                              \
        GRpcRequestProxyId_,                                                                             \
        CQ_,                                                                                             \
        nullptr,                                                                                         \
        nullptr)

    SETUP_MON_METHOD(SelfCheck, DoSelfCheckRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_MON_METHOD(ClusterState, DoClusterStateRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_MON_METHOD(NodeCheck, DoNodeCheckRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCallNoAuth);

#undef SETUP_MON_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
