#pragma once

#include "message.h"

#include <yql/essentials/tools/yql_language_server/lsp/consumer/base.h>

namespace NLsp::NJsonRpc {

using TJsonRpcOutbox = IConsumer<NJsonRpc::TJsonRpcResponse>;

using TJsonRpcListener = IConsumer<NJsonRpc::TJsonRpcRequest>;

using TJsonRpcListenerFactory = std::function<TJsonRpcListener::TPtr(TJsonRpcOutbox::TPtr out)>;

} // namespace NLsp::NJsonRpc
