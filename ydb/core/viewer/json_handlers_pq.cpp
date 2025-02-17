#include "viewer.h"
#include "browse_pq.h"

namespace NKikimr::NViewer {

void SetupPQVirtualHandlers(IViewer* viewer) {
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Root,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseRoot(owner, browseContext);
        });
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Consumers,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseConsumers(owner, browseContext);
        });
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Consumer,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseConsumer(owner, browseContext);
        });
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Topic,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerPQ::TBrowseTopic(owner, browseContext);
        });
}

}
