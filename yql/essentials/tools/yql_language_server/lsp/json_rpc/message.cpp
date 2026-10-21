#include "message.h"

#include <yql/essentials/utils/json/reflection.h>

namespace NYql::NJson {

using namespace NLsp::NJsonRpc;

JSON_DEFINE_FROM(TJsonRpcMessageId, json) {
    switch (json.GetType()) {
        case NJson::JSON_STRING:
            return TJsonRpcMessageId{.Value = std::move(json.GetStringSafe())};
        case NJson::JSON_INTEGER:
        case NJson::JSON_UINTEGER:
            if (json.IsInteger()) {
                return TJsonRpcMessageId{.Value = static_cast<i64>(json.GetInteger())};
            }

            return UnexpectedField("id", "number is out of the i64 range");
        case NJson::JSON_UNDEFINED:
        case NJson::JSON_NULL:
            return TJsonRpcMessageId{.Value = nullptr};
        case NJson::JSON_BOOLEAN:
        case NJson::JSON_DOUBLE:
        case NJson::JSON_MAP:
        case NJson::JSON_ARRAY:
            return UnexpectedField("id", "must be a string, an integer, or null");
    }
}

JSON_DEFINE_TO(TJsonRpcMessageId, value) {
    return std::visit([](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::nullptr_t>) {
            return NJson::TJsonValue(NJson::JSON_NULL);
        } else {
            return NJson::TJsonValue(std::forward<T>(arg));
        }
    }, std::move(value.Value));
}

JSON_DEFINE_FROM(TJsonRpcRequest, json) {
    TString jsonrpc;
    JSON_MOVE_FROM(json, "jsonrpc", jsonrpc);
    if (jsonrpc != "2.0") {
        return UnexpectedField("jsonrpc", "expected to be 2.0");
    }

    TJsonRpcRequest x;
    JSON_MOVE_FROM(json, "method", x.Method);
    JSON_MOVE_FROM(json, "params", x.Params);
    JSON_MOVE_FROM(json, "id", x.Id);
    return x;
}

YQL_DERIVE_JSON_TO(TJsonRpcError);

JSON_DEFINE_TO(TJsonRpcResponse, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "jsonrpc", "2.0");
    if (value.Result) {
        SaveTo(json, "result", std::move(value.Result.value()));
    } else {
        SaveTo(json, "error", std::move(value.Result.error()));
    }
    SaveTo(json, "id", std::move(value.Id));
    return json;
}

} // namespace NYql::NJson
