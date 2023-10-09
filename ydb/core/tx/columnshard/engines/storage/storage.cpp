#include "storage.h"

namespace NKikimr::NOlap {

void TGranulesStorage::UpdateGranuleInfo(const TGranuleMeta& granule) {
    if (PackModificationFlag) {
        PackModifiedGranules[granule.GetGranuleId()] = &granule;
        return;
    }
    {
        auto it = GranulesCompactionPriority.find(granule.GetGranuleId());
        auto gPriority = granule.GetCompactionPriority();
        if (it == GranulesCompactionPriority.end()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "UpdateGranuleInfo")("granule", granule.DebugString())("new_priority", gPriority.DebugString());
            it = GranulesCompactionPriority.emplace(granule.GetGranuleId(), gPriority).first;
            Y_VERIFY(GranuleCompactionPrioritySorting[gPriority].emplace(granule.GetGranuleId()).second);
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "UpdateGranuleInfo")("granule", granule.DebugString())("new_priority", gPriority.DebugString())("old_priority", it->second.DebugString());
            auto itSorting = GranuleCompactionPrioritySorting.find(it->second);
            Y_VERIFY(itSorting != GranuleCompactionPrioritySorting.end());
            Y_VERIFY(itSorting->second.erase(granule.GetGranuleId()));
            if (itSorting->second.empty()) {
                GranuleCompactionPrioritySorting.erase(itSorting);
            }
            it->second = gPriority;
            Y_VERIFY(GranuleCompactionPrioritySorting[gPriority].emplace(granule.GetGranuleId()).second);
        }
    }
}

std::shared_ptr<NKikimr::NOlap::TGranuleMeta> TGranulesStorage::GetGranuleForCompaction(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules) const {
    if (!GranuleCompactionPrioritySorting.size()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granules_for_compaction");
        return {};
    }
    for (auto it = GranuleCompactionPrioritySorting.rbegin(); it != GranuleCompactionPrioritySorting.rend(); ++it) {
        if (it->first.GetWeight().IsZero()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "zero_granule_reached");
            break;
        }
        Y_VERIFY(it->second.size());
        for (auto&& i : it->second) {
            auto itGranule = granules.find(i);
            Y_VERIFY(itGranule != granules.end());
//            if (TMonotonic::Now() - itGranule->second->GetLastCompactionInstant() > TDuration::Seconds(1) || it->first.GetWeight().GetInternalLevelWeight() > 5000000) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "test_granule")("granule_stats", it->first.DebugString())("granule_id", i);
                return itGranule->second;
//            } else {
//                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "test_granule_skipped")("granule_stats", it->first.DebugString())("granule_id", i)("skip_reason", "too_early_and_low_critical");
//            }
        }
    }
    return {};
}

} // namespace NKikimr::NOlap
