#pragma once

#include <yql/essentials/utils/json/from.h>
#include <yql/essentials/utils/json/to.h>
#include <yql/essentials/utils/meta/reflection.h>

#include <util/generic/string.h>

#include <expected>

namespace NLsp::NJsonRpc {

/// @see https://www.jsonrpc.org/specification

struct TJsonRpcMessageId {
    std::variant<i64, TString, std::nullptr_t> Value = nullptr;
};

struct TJsonRpcRequest {
    TString Method;
    TMaybe<NJson::TJsonValue> Params;
    TMaybe<TJsonRpcMessageId> Id;
};

struct TJsonRpcError {
    using TCode = i64;

    static constexpr TCode CodeParseError = -32700;
    static constexpr TCode CodeInvalidRequest = -32600;
    static constexpr TCode CodeMethodNotFound = -32601;
    static constexpr TCode CodeInvalidParams = -32602;
    static constexpr TCode CodeInternalError = -32603;

    TCode Code = CodeInternalError;
    TString Message;
    TMaybe<NJson::TJsonValue> Data;
};

struct TJsonRpcResponse {
    std::expected<NJson::TJsonValue, TJsonRpcError> Result;
    TJsonRpcMessageId Id;
};

} // namespace NLsp::NJsonRpc

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NLsp::NJsonRpc::TJsonRpcError, (Code)(Message)(Data));

} // namespace NYql::NReflection

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::NJsonRpc::TJsonRpcMessageId, json);
JSON_DECLARE_TO(NLsp::NJsonRpc::TJsonRpcMessageId, value);
JSON_DECLARE_FROM(NLsp::NJsonRpc::TJsonRpcRequest, json);
JSON_DECLARE_TO(NLsp::NJsonRpc::TJsonRpcError, value);
JSON_DECLARE_TO(NLsp::NJsonRpc::TJsonRpcResponse, value);

} // namespace NYql::NJson
