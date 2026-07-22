#include "json_handlers.h"
#include "viewer_query.h"

namespace NKikimr::NViewer {

void InitViewerQueryJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/query", new THttpHandler<TJsonQuery>(TJsonQuery::GetSwagger()), 12);
}

} // namespace NKikimr::NViewer
