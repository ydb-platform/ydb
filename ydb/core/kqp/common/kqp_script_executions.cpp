#include "kqp_script_executions.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/builder.h>

namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const std::string& executionId) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::SCRIPT_EXECUTION);
    NOperationId::AddOptionalValue(operationId, "id", executionId);
    return NOperationId::ProtoToString(operationId);
}

NOperationId::TOperationId OperationIdFromExecutionId(const TString& executionId) {
    return NOperationId::TOperationId(ScriptExecutionOperationFromExecutionId(executionId));
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

NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues, bool addEmptyRoot) {
    if (!issues && !addEmptyRoot) {
        return {};
    }

    NYql::TIssue rootIssue(message);
    for (const auto& issue : issues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }

    return {rootIssue};
}

TString SerializeIssues(const NYql::TIssues& issues) {
    NYql::TIssue root;
    for (const auto& issue : issues) {
        root.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }

    Ydb::Issue::IssueMessage rootMessage;
    if (issues) {
        NYql::IssueToMessage(root, &rootMessage);
    }

    return NProtobufJson::Proto2Json(rootMessage, NProtobufJson::TProto2JsonConfig());
}

TString SequenceToJsonString(ui64 size, std::function<void(ui64 i, NJson::TJsonValue& value)> valueFiller) {
    NJson::TJsonValue value;
    value.SetType(NJson::EJsonValueType::JSON_ARRAY);

    NJson::TJsonValue::TArray& jsonArray = value.GetArraySafe();
    jsonArray.resize(size);
    for (ui64 i = 0; i < size; ++i) {
        valueFiller(i, jsonArray[i]);
    }

    NJsonWriter::TBuf serializedJson;
    serializedJson.WriteJsonValue(&value, false, PREC_NDIGITS, 17);

    return serializedJson.Str();
}

} // namespace NKikimr::NKqp
