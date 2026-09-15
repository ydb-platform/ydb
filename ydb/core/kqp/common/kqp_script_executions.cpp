#include "kqp_script_executions.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <google/protobuf/util/time_util.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/builder.h>

namespace NKikimr::NKqp {

std::string ScriptExecutionOperationFromExecutionId(const std::string& executionId) {
    NOperationId::TOperationId operationId;
    operationId.SetKind(NOperationId::TOperationId::SCRIPT_EXECUTION);
    operationId.AddOptionalValue("id", executionId);
    return operationId.ToString();
}

NOperationId::TOperationId OperationIdFromExecutionId(const std::string& executionId) {
    return NOperationId::TOperationId(ScriptExecutionOperationFromExecutionId(executionId));
}

std::optional<TString> ScriptExecutionIdFromOperation(const std::string& operationId, TString& error) try {
    return ScriptExecutionIdFromOperation(NOperationId::TOperationId(operationId), error);
} catch (const std::exception& ex) {
    error = TStringBuilder() << "Invalid operation id: " << ex.what();
    return std::nullopt;
}

std::optional<TString> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId, TString& error) try {
    if (operationId.GetKind() != NOperationId::TOperationId::SCRIPT_EXECUTION) {
        error = TStringBuilder() << "Invalid operation id, expected SCRIPT_EXECUTION = " << static_cast<int>(NOperationId::TOperationId::SCRIPT_EXECUTION) << " kind, got " << static_cast<int>(operationId.GetKind());
        return std::nullopt;
    }

    const auto& values = operationId.GetValue("id");
    if (values.empty() || !values[0]) {
        error = "Invalid operation id, please specify key 'id'";
        return std::nullopt;
    }

    return TString(*values[0]);
} catch (const std::exception& ex) {
    error = TStringBuilder() << "Invalid operation id: " << ex.what();
    return std::nullopt;
}

NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues, const bool addEmptyRoot) {
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
    Ydb::Issue::IssueMessage rootMessage;
    NYql::IssuesToMessage(issues, rootMessage.mutable_issues());
    return NProtobufJson::Proto2Json(rootMessage, NProtobufJson::TProto2JsonConfig());
}

NYql::TIssues DeserializeIssues(const TStringBuf& issuesSerialized) {
    const auto& rootMessage = NProtobufJson::Json2Proto<Ydb::Issue::IssueMessage>(issuesSerialized);
    NYql::TIssues issues;
    NYql::IssuesFromMessage(rootMessage.issues(), issues);
    return issues;
}

void SerializeBinaryProto(const NProtoBuf::Message& proto, NJson::TJsonValue& value) {
    value.SetType(NJson::EJsonValueType::JSON_MAP);

    NProtobufJson::TProto2JsonConfig config;
    config.AddStringTransform(MakeIntrusive<NProtobufJson::TBase64EncodeBytesTransform>());
    NProtobufJson::Proto2Json(proto, value["encoded_proto"], config);
}

TString SerializeBinaryProto(const NProtoBuf::Message& proto) {
    NJson::TJsonValue value;
    SerializeBinaryProto(proto, value);

    NJsonWriter::TBuf serializedProto;
    serializedProto.WriteJsonValue(&value, false, PREC_NDIGITS, 17);

    return serializedProto.Str();
}

void DeserializeBinaryProto(const NJson::TJsonValue& value, NProtoBuf::Message& proto) {
    const auto& valueMap = value.GetMap();
    const auto encodedProto = valueMap.find("encoded_proto");
    if (encodedProto == valueMap.end()) {
        return NProtobufJson::Json2Proto(value, proto, NProtobufJson::TJson2ProtoConfig());
    }

    NProtobufJson::TJson2ProtoConfig config;
    config.AddStringTransform(MakeIntrusive<NProtobufJson::TBase64DecodeBytesTransform>());
    NProtobufJson::Json2Proto(encodedProto->second, proto, config);
}

void TimestampToProtoWithSaturation(const TInstant timestamp, google::protobuf::Timestamp* proto) {
    *proto = google::protobuf::util::TimeUtil::MicrosecondsToTimestamp(
        std::min(timestamp.MicroSeconds(), static_cast<ui64>(google::protobuf::util::TimeUtil::kTimestampMaxSeconds * 1000000))
    );
}

void DurationToProtoWithSaturation(const TDuration duration, google::protobuf::Duration* proto) {
    *proto = google::protobuf::util::TimeUtil::MicrosecondsToDuration(
        std::min(duration.MicroSeconds(), static_cast<ui64>(google::protobuf::util::TimeUtil::kDurationMaxSeconds * 1000000))
    );
}

TString SequenceToJsonString(const ui64 size, std::function<void(const ui64 i, NJson::TJsonValue& value)> valueFiller) {
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

TString CheckScriptExecutionAccess(std::optional<TString>& userSID) {
    if (!AppData()->FeatureFlags.GetEnableSecureScriptExecutions()) {
        userSID = std::nullopt;
        return "";
    }

    if (!userSID) {
        return "Access to script execution operations without user token is not allowed";
    }

    if (NACLib::TUserToken(*userSID, {}).IsSystemUser()) {
        userSID = std::nullopt;
    }

    return "";
}

} // namespace NKikimr::NKqp
