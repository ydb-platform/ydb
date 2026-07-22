#include "json_handlers.h"
#include "viewer_nodes.h"

namespace NKikimr::NViewer {

void InitViewerNodesJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/nodes", new THttpHandler<TJsonNodes>(TJsonNodes::GetSwagger()), 20);
}

} // namespace NKikimr::NViewer
