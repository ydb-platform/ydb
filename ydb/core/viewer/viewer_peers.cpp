#include "json_handlers.h"
#include "viewer_peers.h"

namespace NKikimr::NViewer {

void InitViewerPeersJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/peers", new TJsonHandler<TJsonPeers>(TJsonPeers::GetSwagger()));
}

} // namespace NKikimr::NViewer
