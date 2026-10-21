#include "parallel.h"

#include <yql/essentials/tools/yql_language_server/lsp/consumer/parallel.h>
#include <yql/essentials/tools/yql_language_server/lsp/message/method.h>

namespace NLsp {

NJsonRpc::TJsonRpcListener::TPtr Parallel(
    THolder<IThreadPool> pool,
    NJsonRpc::TJsonRpcListener::TPtr listener)
{
    return Parallel<NJsonRpc::TJsonRpcRequest>(std::move(pool), [](const auto& x) {
        return IsReadonlyMethod(x.Method);
    }, listener);
}

} // namespace NLsp
