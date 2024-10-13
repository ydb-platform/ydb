#include "storage.h"

#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap {

std::optional<NStorageOptimizer::TOptimizationPriority> TGranulesStorage::GetCompactionPriority(
    const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const std::set<ui64>& pathIds,
    std::shared_ptr<TGranuleMeta>* granuleResult) const {
    const TInstant now = HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now();
    std::map<NStorageOptimizer::TOptimizationPriority, std::shared_ptr<TGranuleMeta>> granulesSorted;
    std::optional<NStorageOptimizer::TOptimizationPriority> priorityChecker;
    std::shared_ptr<TGranuleMeta> maxPriorityGranule;
    const TDuration actualizationLag = NYDBTest::TControllers::GetColumnShardController()->GetCompactionActualizationLag();
    const auto actor = [&](const ui64 /*pathId*/, const std::shared_ptr<TGranuleMeta>& granule) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", i.first);
        if (pathIds.empty()) {
            granule->ActualizeOptimizer(now, actualizationLag);
        }
        auto gPriority = granule->GetCompactionPriority();
        if (gPriority.IsZero() || (priorityChecker && gPriority < *priorityChecker)) {
            return;
        }
        granulesSorted.emplace(gPriority, granule);
        if (granulesSorted.size() == 100) {
            for (auto&& it = granulesSorted.rbegin(); it != granulesSorted.rend(); ++it) {
                if (!it->second->IsLockedOptimizer(dataLocksManager)) {
                    priorityChecker = it->first;
                    maxPriorityGranule = it->second;
                    break;
                }
            }
            granulesSorted.clear();
        }
    };
    if (pathIds.size()) {
        for (auto&& pathId : pathIds) {
            auto it = Tables.find(pathId);
            AFL_VERIFY(it != Tables.end());
            actor(it->first, it->second);
        }
    } else {
        for (auto&& i : Tables) {
            actor(i.first, i.second);
        }
    }
    for (auto&& it = granulesSorted.rbegin(); it != granulesSorted.rend(); ++it) {
        if (!it->second->IsLockedOptimizer(dataLocksManager)) {
            priorityChecker = it->first;
            maxPriorityGranule = it->second;
            break;
        }
    }
    if (granuleResult) {
        *granuleResult = maxPriorityGranule;
    }
    return priorityChecker;
}

std::shared_ptr<TGranuleMeta> TGranulesStorage::GetGranuleForCompaction(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    std::shared_ptr<TGranuleMeta> granuleMaxPriority;
    std::optional<NStorageOptimizer::TOptimizationPriority> priorityChecker = GetCompactionPriority(dataLocksManager, {}, &granuleMaxPriority);
    if (!granuleMaxPriority) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granules");
        return nullptr;
    }
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", granuleMaxPriority->GetPathId());
    AFL_VERIFY(!granuleMaxPriority->IsLockedOptimizer(dataLocksManager));
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "granule_compaction_weight")("priority", priorityChecker->DebugString());
    return granuleMaxPriority;
}

}   // namespace NKikimr::NOlap
