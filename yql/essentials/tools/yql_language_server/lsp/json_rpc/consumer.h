#pragma once

#include "message.h"
#include "listener.h"

#include <yql/essentials/tools/yql_language_server/lsp/consumer/base.h>

namespace NLsp::NJsonRpc {

/// For logic exceptions interception.
IConsumer<TJsonRpcRequest>::TPtr JsonRpcExceptionHandling(
    TJsonRpcOutbox::TPtr out,
    IConsumer<TJsonRpcRequest>::TPtr consumer);

/// For parsing exceptions interception.
IConsumer<TString>::TPtr JsonRpcExceptionHandling(
    TJsonRpcOutbox::TPtr out,
    IConsumer<TString>::TPtr consumer);

IConsumer<TString>::TPtr JsonRpcMarshalling(IConsumer<TJsonRpcRequest>::TPtr consumer);

IConsumer<TJsonRpcResponse>::TPtr JsonRpcMarshalling(IConsumer<TString>::TPtr consumer);

} // namespace NLsp::NJsonRpc
