#include "ydb_operation.h"

#include <ydb/core/grpc_services/service_operation.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcOperationService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Operations;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_OPERATION_METHOD
#error SETUP_OPERATION_METHOD macro already defined
#endif

#define SETUP_OPERATION_METHOD(methodName, methodCallback, rlMode, requestType, auditMode, operationCallClass) \
    SETUP_RUNTIME_EVENT_METHOD(                               \
        methodName,                                           \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),             \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),            \
        methodCallback,                                       \
        rlMode,                                               \
        requestType,                                          \
        YDB_API_DEFAULT_COUNTER_BLOCK(operation, methodName), \
        auditMode,                                            \
        COMMON,                                               \
        operationCallClass,                                   \
        GRpcRequestProxyId_,                                  \
        CQ_,                                                  \
        nullptr,                                              \
        nullptr)

    SETUP_OPERATION_METHOD(GetOperation, DoGetOperationRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_OPERATION_METHOD(CancelOperation, DoCancelOperationRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Operations), TGrpcRequestNoOperationCall);
    SETUP_OPERATION_METHOD(ForgetOperation, DoForgetOperationRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Operations), TGrpcRequestNoOperationCall);
    SETUP_OPERATION_METHOD(ListOperations, DoListOperationsRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestNoOperationCall);

#undef SETUP_OPERATION_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
