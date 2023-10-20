#include "storage.h"

namespace NKikimr::NOlap {

void TGranulesStorage::UpdateGranuleInfo(const TGranuleMeta& granule) {
    if (PackModificationFlag) {
        PackModifiedGranules[granule.GetGranuleId()] = &granule;
        return;
    }
}

std::shared_ptr<NKikimr::NOlap::TGranuleMeta> TGranulesStorage::GetGranuleForCompaction(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules) const {
    const TInstant now = TInstant::Now();
    std::optional<NStorageOptimizer::TOptimizationPriority> priority;
    std::shared_ptr<TGranuleMeta> granule;
    for (auto&& i : granules) {
        i.second->ActualizeOptimizer(now);
        if (!priority || *priority < i.second->GetCompactionPriority()) {
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
/*
    for (auto it = GranuleCompactionPrioritySorting.rbegin(); it != GranuleCompactionPrioritySorting.rend(); ++it) {
        if (it->first.GetWeight().IsZero()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "zero_granule_reached");
            break;
        }
        Y_ABORT_UNLESS(it->second.size());
        for (auto&& i : it->second) {
            auto itGranule = granules.find(i);
            Y_ABORT_UNLESS(itGranule != granules.end());
            if (it->first.GetWeight().GetInternalLevelWeight() > 0 * 1024 * 1024) {

//            if (it->first.GetWeight().GetInternalLevelWeight() / 10000000 > 100 ||
//                it->first.GetWeight().GetInternalLevelWeight() % 10000000 > 100000) {

                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "test_granule")("granule_stats", it->first.DebugString())("granule_id", i);
                return itGranule->second;
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "test_granule_skipped")("granule_stats", it->first.DebugString())("granule_id", i)("skip_reason", "too_early_and_low_critical");
                break;
            }
        }
    }
    return {};
*/
}

} // namespace NKikimr::NOlap
