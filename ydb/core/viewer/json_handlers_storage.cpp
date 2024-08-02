#include "json_handlers.h"
#include "storage_groups.h"

namespace NKikimr::NViewer {

void InitStorageGroupsJsonHandler(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/storage/groups", new TJsonHandler<TStorageGroups>(TStorageGroups::GetSwagger()));
}

void InitStorageJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitStorageGroupsJsonHandler(jsonHandlers);
}

}
