#include "storage.h"

#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap {

std::optional<NStorageOptimizer::TOptimizationPriority> TGranulesStorage::GetCompactionPriority(
    const std::shared_ptr<NDataLocks::TManager>& dataLocksManager,
    std::shared_ptr<TGranuleMeta>* granuleResult) const {
    const TInstant now = HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now();
    std::map<NStorageOptimizer::TOptimizationPriority, std::shared_ptr<TGranuleMeta>> granulesSorted;
    ui32 countChecker = 0;
    std::optional<NStorageOptimizer::TOptimizationPriority> priorityChecker;
    std::shared_ptr<TGranuleMeta> maxPriorityGranule;
    const TDuration actualizationLag = NYDBTest::TControllers::GetColumnShardController()->GetCompactionActualizationLag();
    for (auto&& i : Tables) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", i.first);
        i.second->ActualizeOptimizer(now, actualizationLag);
        auto gPriority = i.second->GetCompactionPriority();
        if (gPriority.IsZero() || (priorityChecker && gPriority < *priorityChecker)) {
            continue;
        }
        granulesSorted.emplace(gPriority, i.second);
        if (++countChecker % 100 == 0 || !priorityChecker) {
            for (auto&& it = granulesSorted.rbegin(); it != granulesSorted.rend(); ++it) {
                if (!it->second->IsLockedOptimizer(dataLocksManager)) {
                    priorityChecker = it->first;
                    maxPriorityGranule = it->second;
                    break;
                }
            }
            granulesSorted.clear();
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
    std::optional<NStorageOptimizer::TOptimizationPriority> priorityChecker = GetCompactionPriority(dataLocksManager, &granuleMaxPriority);
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
