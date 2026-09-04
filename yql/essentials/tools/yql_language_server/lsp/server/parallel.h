#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/consumer/blocking_queue.h>
#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/listener.h>

#include <util/thread/pool.h>

namespace NLsp {

NJsonRpc::TJsonRpcListener::TPtr Parallel(
    THolder<IThreadPool> pool,
    NJsonRpc::TJsonRpcListener::TPtr listener);

} // namespace NLsp
