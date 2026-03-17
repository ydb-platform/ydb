#include "storage.h"

#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap {

namespace {
class TGranuleOrdered {
private:
    NStorageOptimizer::TOptimizationPriority Priority;
    YDB_READONLY_DEF(std::shared_ptr<TGranuleMeta>, Granule);

public:
    const NStorageOptimizer::TOptimizationPriority& GetPriority() const {
        return Priority;
    }

    TGranuleOrdered(const NStorageOptimizer::TOptimizationPriority& priority, const std::shared_ptr<TGranuleMeta>& meta)
        : Priority(priority)
        , Granule(meta) {
    }

    bool operator<(const TGranuleOrdered& item) const {
        return Priority < item.Priority;
    }
};
}   // namespace

std::optional<NStorageOptimizer::TOptimizationPriority> TGranulesStorage::GetCompactionPriority(
    const std::set<TInternalPathId>& pathIds, 
    const std::optional<ui64> waitingPriority,
    std::shared_ptr<TGranuleMeta>* granuleResult
) const {
    const TInstant now = HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now();
    std::vector<TGranuleOrdered> granulesSorted;
    std::optional<NStorageOptimizer::TOptimizationPriority> priorityChecker;
    std::shared_ptr<TGranuleMeta> maxPriorityGranule;
    const TDuration actualizationLag = NYDBTest::TControllers::GetColumnShardController()->GetCompactionActualizationLag();
    const auto actor = [&](const TInternalPathId /*pathId*/, const std::shared_ptr<TGranuleMeta>& granule) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", i.first);
        if (pathIds.empty()) {
            granule->ActualizeOptimizer(now, actualizationLag);
        }
        auto gPriority = granule->GetCompactionPriority();
        if (gPriority.IsZero() || (waitingPriority && gPriority.GetGeneralPriority() < *waitingPriority)) {
            return;
        }
        granulesSorted.emplace_back(gPriority, granule);
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
    std::sort(granulesSorted.begin(), granulesSorted.end());
    if (!granulesSorted.empty()) {
        priorityChecker = granulesSorted.back().GetPriority();
        maxPriorityGranule = granulesSorted.back().GetGranule();
    }
    if (granuleResult) {
        *granuleResult = maxPriorityGranule;
    }
    return priorityChecker;
}

std::shared_ptr<TGranuleMeta> TGranulesStorage::GetGranuleForCompaction() const {
    std::shared_ptr<TGranuleMeta> granuleMaxPriority;
    std::optional<NStorageOptimizer::TOptimizationPriority> priorityChecker = GetCompactionPriority({}, std::nullopt, &granuleMaxPriority);
    if (!granuleMaxPriority) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granules");
        return nullptr;
    }
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("path_id", granuleMaxPriority->GetPathId());
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "granule_compaction_weight")("priority", priorityChecker->DebugString());
    return granuleMaxPriority;
}

}   // namespace NKikimr::NOlap
