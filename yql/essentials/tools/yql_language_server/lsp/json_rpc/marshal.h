#pragma once

#include "message.h"

namespace NLsp::NJsonRpc {

TJsonRpcRequest UnMarshal(TString request);

TString Marshal(TJsonRpcResponse response);

} // namespace NLsp::NJsonRpc
