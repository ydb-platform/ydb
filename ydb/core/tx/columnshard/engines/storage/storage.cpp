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
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "UpdateGranuleInfo")("granule", granule.DebugString())("new_priority", gPriority.DebugString());
            it = GranulesCompactionPriority.emplace(granule.GetGranuleId(), gPriority).first;
            Y_VERIFY(GranuleCompactionPrioritySorting[gPriority].emplace(granule.GetGranuleId()).second);
        } else {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "UpdateGranuleInfo")("granule", granule.DebugString())("new_priority", gPriority.DebugString())("old_priority", it->second.DebugString());
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

} // namespace NKikimr::NOlap
