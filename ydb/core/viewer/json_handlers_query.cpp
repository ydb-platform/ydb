#include "json_handlers.h"
#include "query_execute_script.h"
#include "query_fetch_script.h"

namespace NKikimr::NViewer {

void InitQueryExecuteScriptJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/query/script/execute", new TJsonHandler<TQueryExecuteScript>(TQueryExecuteScript::GetSwagger()));
}

void InitQueryFetchScriptJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/query/script/fetch", new TJsonHandler<TQueryFetchScript>(TQueryFetchScript::GetSwagger()));
}

void InitQueryJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitQueryExecuteScriptJsonHandler(jsonHandlers);
    InitQueryFetchScriptJsonHandler(jsonHandlers);
}

} // namespace NKikimr::NViewer
