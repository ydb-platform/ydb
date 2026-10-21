#include "marshal.h"

#include "exception.h"

namespace NLsp::NJsonRpc {

TJsonRpcRequest UnMarshal(TString request) {
    NJson::TJsonValue json;
    try {
        Y_ENSURE(NJson::ReadJsonTree(request, &json, /*throwOnError=*/true));
    } catch (const NJson::TJsonException& e) {
        throw TJsonRpcException(TJsonRpcError::CodeParseError) << e.what();
    }

    auto message = NYql::NJson::FromJson<TJsonRpcRequest>(std::move(json));
    if (!message) {
        throw TJsonRpcException(TJsonRpcError::CodeInvalidRequest) << message.error();
    }

    return std::move(*message);
}

TString Marshal(TJsonRpcResponse response) {
    return NYql::NJson::ToJsonString(std::move(response));
}

} // namespace NLsp::NJsonRpc
