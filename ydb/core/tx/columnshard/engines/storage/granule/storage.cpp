#include "storage.h"

#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>

namespace NKikimr::NOlap {

std::vector<TGranuleOrdered> TGranulesStorage::GetGranulesForCompaction(
    const std::set<TInternalPathId>& pathIds, 
    const std::optional<ui64> waitingPriority
) const {
    const TInstant now = HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now();
    std::vector<TGranuleOrdered> granulesSorted;
    const TDuration actualizationLag = NYDBTest::TControllers::GetColumnShardController()->GetCompactionActualizationLag();
    const auto actor = [&](const TInternalPathId /*pathId*/, const std::shared_ptr<TGranuleMeta>& granule) {
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
    // descending order
    std::sort(granulesSorted.begin(), granulesSorted.end(),
        [](const TGranuleOrdered& a, const TGranuleOrdered& b) { return b < a; }
    );
    return granulesSorted;
}

}   // namespace NKikimr::NOlap
