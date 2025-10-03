#include "kqp_script_execution_utils.h"

#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NKqp {

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

    return NProtobufJson::Proto2Json(rootMessage);
}

NOperationId::TOperationId OperationIdFromExecutionId(const TString& executionId) {
    return NOperationId::TOperationId(ScriptExecutionOperationFromExecutionId(executionId));
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

}  // namespace NKikimr::NKqp
