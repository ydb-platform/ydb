
#include "json_handlers.h"

namespace NKikimr::NViewer {

void InitStorageGroupsJsonHandler(TJsonHandlers& jsonHandlers);

void InitStorageJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitStorageGroupsJsonHandler(jsonHandlers);
}

}
