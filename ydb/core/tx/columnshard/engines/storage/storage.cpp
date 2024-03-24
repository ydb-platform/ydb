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
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", i.first);
        i.second->ActualizeOptimizer(now);
        auto gPriority = i.second->GetCompactionPriority();
        if (!priority || *priority < gPriority) {
            if (i.second->IsLockedOptimizer(dataLocksManager) && !gPriority.IsZero()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_optimizer_throught_lock")("priority", gPriority.DebugString());
                continue;
            }
            priority = gPriority;
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
