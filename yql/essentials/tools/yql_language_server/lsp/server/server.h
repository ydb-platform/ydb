#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/api/api.h>

#include <functional>

namespace NLsp {

using TLspListenerFactory = std::function<
    NJsonRpc::TJsonRpcListener::TPtr(IConsumer<NJsonRpc::TJsonRpcResponse>::TPtr out)>;

struct TLspServerOptions {
    size_t Threads = 1;
};

void LspServe(
    IInputStream& cin,
    IOutputStream& cout,
    TLspServerOptions options,
    TLspListenerFactory factory);

} // namespace NLsp
