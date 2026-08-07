#pragma once

#include "session.h"
#include "synchronization.h"
#include "completion.h"
#include "formatting.h"

#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/listener.h>

namespace NLsp {

class ILspApi: public NJsonRpc::TJsonRpcListener,
               public ISessionApi,
               public ISynchronizationApi,
               public ICompletionApi,
               public IFormattingApi {
};

} // namespace NLsp
