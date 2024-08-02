#include "json_handlers.h"
#include "browse.h"
#include "browse_db.h"
#include "browse_pq.h"
#include "viewer_browse.h"
#include "viewer_content.h"
#include "viewer_metainfo.h"

namespace NKikimr::NViewer {

void SetupDBVirtualHandlers(IViewer* viewer) {
    viewer->RegisterVirtualHandler(
        NKikimrViewer::EObjectType::Table,
        [] (const TActorId& owner, const IViewer::TBrowseContext& browseContext) -> IActor* {
            return new NViewerDB::TBrowseTable(owner, browseContext);
        });
}

void InitViewerMetaInfoJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/metainfo", new TJsonHandler<TJsonMetaInfo>(TJsonMetaInfo::GetSwagger()));
}

void InitViewerBrowseJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/browse", new TJsonHandler<TJsonBrowse>(TJsonBrowse::GetSwagger()));
}

void InitViewerContentJsonHandler(TJsonHandlers &jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/content", new TJsonHandler<TJsonContent>(TJsonContent::GetSwagger()));
}

void InitViewerBrowseJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitViewerMetaInfoJsonHandler(jsonHandlers);
    InitViewerBrowseJsonHandler(jsonHandlers);
    InitViewerContentJsonHandler(jsonHandlers);
}

}
