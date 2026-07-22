#include "json_handlers.h"
#include "viewer_groups.h"

namespace NKikimr::NViewer {

void InitViewerGroupsJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/groups", new THttpHandler<TStorageGroups>(TStorageGroups::GetSwagger()), 11);
    jsonHandlers.AddHandler("/storage/groups", new THttpHandler<TStorageGroups>(TStorageGroups::GetSwagger()), 11);
}

} // namespace NKikimr::NViewer
