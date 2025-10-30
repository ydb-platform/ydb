#include "ydb_scripting.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_yql_scripting.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbScriptingService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Scripting;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_SCRIPTING_METHOD
#error SETUP_SCRIPTING_METHOD macro already defined
#endif

#define SETUP_SCRIPTING_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode, operationCallClass) \
    SETUP_RUNTIME_EVENT_METHOD(                               \
        methodName,                                           \
        inputType,                                            \
        outputType,                                           \
        methodCallback,                                       \
        rlMode,                                               \
        requestType,                                          \
        YDB_API_DEFAULT_COUNTER_BLOCK(scripting, methodName), \
        auditMode,                                            \
        COMMON,                                               \
        operationCallClass,                                   \
        GRpcRequestProxyId_,                                  \
        CQ_,                                                  \
        nullptr,                                              \
        nullptr)

    SETUP_SCRIPTING_METHOD(ExecuteYql, ExecuteYqlRequest, ExecuteYqlResponse, DoExecuteYqlScript, RLSWITCH(Ru), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TGrpcRequestOperationCall);
    SETUP_SCRIPTING_METHOD(StreamExecuteYql, ExecuteYqlRequest, ExecuteYqlPartialResponse, DoStreamExecuteYqlScript, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), TGrpcRequestNoOperationCall);
    SETUP_SCRIPTING_METHOD(ExplainYql, ExplainYqlRequest, ExplainYqlResponse, DoExplainYqlScript, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying(), TGrpcRequestOperationCall);

#undef SETUP_SCRIPTING_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
