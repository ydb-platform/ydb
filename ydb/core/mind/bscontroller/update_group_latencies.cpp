#include "impl.h"

namespace NKikimr::NBsController {

void TBlobStorageController::Handle(TEvControllerCommitGroupLatencies::TPtr& ev) {
    auto& updates = ev->Get()->Updates;
    for (auto& [key, value] : updates) {
        if (TGroupInfo *group = FindGroup(key)) {
            group->LatencyStats = std::move(value);
            SysViewChangedGroups.insert(key);
        }
    }
}

} // NKikimr::NBsController
