#include "kqp_script_executions.h"

#include <ydb-cpp-sdk/library/operation_id/operation_id.h>

namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const TString& executionId) {
    NOperationId::TOperationId operationId;
    operationId.GetMutableKind() = NOperationId::TOperationId::SCRIPT_EXECUTION;
    NOperationId::AddOptionalValue(operationId, "id", executionId);
    return operationId.ToString();
}

TMaybe<TString> ScriptExecutionIdFromOperation(const TString& operationId) {
    NOperationId::TOperationId operation(operationId);
    return ScriptExecutionIdFromOperation(operation);
}

TMaybe<std::string> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId) {
    if (operationId.GetKind() != NOperationId::TOperationId::SCRIPT_EXECUTION) {
        return Nothing();
    }

    const auto& values = operationId.GetValue("id");
    if (values.empty() || !values[0]) {
        return Nothing();
    }
    return TString{*values[0]};
}

} // namespace NKikimr::NKqp
