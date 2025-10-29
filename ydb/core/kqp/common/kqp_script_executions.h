#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>


namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const std::string& executionId);
NOperationId::TOperationId OperationIdFromExecutionId(const TString& executionId);
TMaybe<TString> ScriptExecutionIdFromOperation(const TString& operationId, TString& error);
TMaybe<TString> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId, TString& error);

NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues, bool addEmptyRoot = true);
TString SerializeIssues(const NYql::TIssues& issues);

TString SequenceToJsonString(ui64 size, std::function<void(ui64 i, NJson::TJsonValue& value)> valueFiller);

template <typename TContainer>
TString SequenceToJsonString(const TContainer& container) {
    return SequenceToJsonString(container.size(), [&](ui64 i, NJson::TJsonValue& value) {
        value = NJson::TJsonValue(container[i]);
    });
}

} // namespace NKikimr::NKqp
