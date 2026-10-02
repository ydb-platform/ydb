#pragma once

#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <google/protobuf/message.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NKqp {

std::string ScriptExecutionOperationFromExecutionId(const std::string& executionId);

NOperationId::TOperationId OperationIdFromExecutionId(const std::string& executionId);

std::optional<TString> ScriptExecutionIdFromOperation(const std::string& operationId, TString& error);

std::optional<TString> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId, TString& error);

NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues, const bool addEmptyRoot = true);

TString SerializeIssues(const NYql::TIssues& issues);

NYql::TIssues DeserializeIssues(const TStringBuf& issuesSerialized);

void SerializeBinaryProto(const NProtoBuf::Message& proto, NJson::TJsonValue& value);

TString SerializeBinaryProto(const NProtoBuf::Message& proto);

void DeserializeBinaryProto(const NJson::TJsonValue& value, NProtoBuf::Message& proto);

void TimestampToProtoWithSaturation(const TInstant timestamp, google::protobuf::Timestamp* proto);

void DurationToProtoWithSaturation(const TDuration duration, google::protobuf::Duration* proto);

TString SequenceToJsonString(const ui64 size, std::function<void(const ui64 i, NJson::TJsonValue& value)> valueFiller);

template <typename TContainer>
TString SequenceToJsonString(const TContainer& container) {
    return SequenceToJsonString(container.size(), [&](const ui64 i, NJson::TJsonValue& value) {
        value = NJson::TJsonValue(container[i]);
    });
}

TString CheckScriptExecutionAccess(std::optional<TString>& userSID);

} // namespace NKikimr::NKqp
