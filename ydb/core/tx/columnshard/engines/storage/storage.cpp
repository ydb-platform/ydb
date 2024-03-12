#include "storage.h"
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap {

void TGranulesStorage::UpdateGranuleInfo(const TGranuleMeta& granule) {
    if (PackModificationFlag) {
        PackModifiedGranules[granule.GetPathId()] = &granule;
        return;
    }
}

std::shared_ptr<NKikimr::NOlap::TGranuleMeta> TGranulesStorage::GetGranuleForCompaction(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    const TInstant now = TInstant::Now();
    std::optional<NStorageOptimizer::TOptimizationPriority> priority;
    std::shared_ptr<TGranuleMeta> granule;
    for (auto&& i : granules) {
        i.second->ActualizeOptimizer(now);
        if (!priority || *priority < i.second->GetCompactionPriority()) {
            if (i.second->IsLockedOptimizer(dataLocksManager)) {
                continue;
            }
            priority = i.second->GetCompactionPriority();
            granule = i.second;
        }
    }
    if (!priority) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granules");
        return nullptr;
    }
    if (priority->IsZero()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "zero_priority");
        return nullptr;
    }
    return granule;
}

} // namespace NKikimr::NOlap
