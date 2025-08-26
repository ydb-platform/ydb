#include "kqp_script_executions.h"

#include <util/string/builder.h>

#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const std::string& executionId) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::SCRIPT_EXECUTION);
    NOperationId::AddOptionalValue(operationId, "id", executionId);
    return NOperationId::ProtoToString(operationId);
}

TMaybe<TString> ScriptExecutionIdFromOperation(const TString& operationId, TString& error) try {
    NOperationId::TOperationId operation(operationId);
    return ScriptExecutionIdFromOperation(operation, error);
} catch (const std::exception& ex) {
    error = TStringBuilder() << "Invalid operation id: " << ex.what();
    return Nothing();
}

TMaybe<TString> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId, TString& error) try {
    if (operationId.GetKind() != NOperationId::TOperationId::SCRIPT_EXECUTION) {
        error = TStringBuilder() << "Invalid operation id, expected SCRIPT_EXECUTION = " << static_cast<int>(NOperationId::TOperationId::SCRIPT_EXECUTION) << " kind, got " << static_cast<int>(operationId.GetKind());
        return Nothing();
    }

    const auto& values = operationId.GetValue("id");
    if (values.empty() || !values[0]) {
        error = TStringBuilder() << "Invalid operation id, please specify key 'id'";
        return Nothing();
    }
    return TString{*values[0]};
} catch (const std::exception& ex) {
    error = TStringBuilder() << "Invalid operation id: " << ex.what();
    return Nothing();
}

} // namespace NKikimr::NKqp
