#include "json_handlers.h"
#include "viewer_cluster.h"

namespace NKikimr::NViewer {

void InitViewerClusterJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/cluster", new THttpHandler<TJsonCluster>(TJsonCluster::GetSwagger()), 8);
}

} // namespace NKikimr::NViewer
