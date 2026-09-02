#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/api/api.h>
#include <yql/essentials/tools/yql_language_server/service/layer.h>

namespace NLsp::NYql {

NJsonRpc::TJsonRpcListenerFactory MakeYqlLspApi(TServiceLayer layer);

} // namespace NLsp::NYql
