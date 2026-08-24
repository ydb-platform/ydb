#pragma once

#include "session.h"
#include "synchronization.h"
#include "completion.h"
#include "diagnostic.h"
#include "formatting.h"

#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/listener.h>

namespace NLsp {

class ILspApi: public NJsonRpc::TJsonRpcListener,
               public ISessionApi,
               public ISynchronizationApi,
               public ICompletionApi,
               public IDiagnosticApi,
               public IFormattingApi {
};

} // namespace NLsp
