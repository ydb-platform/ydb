#include "kqp_script_executions.h"

#include <ydb/public/lib/operation_id/protos/operation_id.pb.h>

namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const TString& executionId) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::SCRIPT_EXECUTION);
    NOperationId::AddOptionalValue(operationId, "id", executionId);
    return NOperationId::ProtoToString(operationId);
}

TMaybe<TString> ScriptExecutionIdFromOperation(const TString& operationId) {
    NOperationId::TOperationId operation(operationId);
    return ScriptExecutionIdFromOperation(operation);
}

TMaybe<TString> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId) {
    if (operationId.GetKind() != Ydb::TOperationId::SCRIPT_EXECUTION) {
        return Nothing();
    }

    const auto& values = operationId.GetValue("id");
    if (values.empty() || !values[0]) {
        return Nothing();
    }
    return *values[0];
}

} // namespace NKikimr::NKqp
