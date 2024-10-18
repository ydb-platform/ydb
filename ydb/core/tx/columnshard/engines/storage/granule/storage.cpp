#include "storage.h"
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::TGranuleMeta> TGranulesStorage::GetGranuleForCompaction(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    const TInstant now = HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now();
    std::map<NStorageOptimizer::TOptimizationPriority, std::shared_ptr<TGranuleMeta>> granulesSorted;
    ui32 countChecker = 0;
    std::optional<NStorageOptimizer::TOptimizationPriority> priorityChecker;
    const TDuration actualizationLag = NYDBTest::TControllers::GetColumnShardController()->GetCompactionActualizationLag();
    for (auto&& i : Tables) {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", i.first);
        i.second->ActualizeOptimizer(now, actualizationLag);
        auto gPriority = i.second->GetCompactionPriority();
        if (gPriority.IsZero() || (priorityChecker && gPriority < *priorityChecker)) {
            continue;
        }
        granulesSorted.emplace(gPriority, i.second);
        if (++countChecker % 100 == 0) {
            for (auto&& it = granulesSorted.rbegin(); it != granulesSorted.rend(); ++it) {
                if (!it->second->IsLockedOptimizer(dataLocksManager)) {
                    priorityChecker = it->first;
                    break;
                }
            }
        }
    }
    if (granulesSorted.empty()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granules");
        return nullptr;
    }
    for (auto&& it = granulesSorted.rbegin(); it != granulesSorted.rend(); ++it) {
        if (priorityChecker && it->first < *priorityChecker) {
            continue;
        }
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", it->second->GetPathId());
        if (it->second->IsLockedOptimizer(dataLocksManager)) {
            Counters.OnGranuleOptimizerLocked();
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_optimizer_throught_lock")("priority", it->first.DebugString());
        } else {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "granule_compaction_weight")("priority", it->first.DebugString());
            return it->second;
        }
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "all_significant_granules_locked")("count", granulesSorted.size());
    return nullptr;
}

} // namespace NKikimr::NOlap
