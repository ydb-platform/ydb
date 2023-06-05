#include "kqp_script_executions.h"

#include <ydb/public/lib/operation_id/protos/operation_id.pb.h>

namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const TString& executionId) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::SCRIPT_EXECUTION);
    NOperationId::AddOptionalValue(operationId, "actor_id", executionId);
    return NOperationId::ProtoToString(operationId);
}

TMaybe<TString> ScriptExecutionFromOperation(const TString& operationId) {
    NOperationId::TOperationId operation(operationId);
    return ScriptExecutionFromOperation(operation);
}

TMaybe<TString> ScriptExecutionFromOperation(const NOperationId::TOperationId& operationId) {
    const auto& values = operationId.GetValue("actor_id");
    if (values.empty() || !values[0]) {
        return Nothing();
    }
    return *values[0];
}

} // namespace NKikimr::NKqp
