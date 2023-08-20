#include "storage.h"

namespace NKikimr::NOlap {

const THashSet<ui64>* TGranulesStorage::GetOverloaded(ui64 pathId) const {
    if (auto pi = PathsGranulesOverloaded.find(pathId); pi != PathsGranulesOverloaded.end()) {
        return &pi->second;
    }
    return nullptr;
}

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
    const ui64 pathId = granule.Record.PathId;

    // Size exceeds the configured limit. Mark granule as overloaded.
    if ((i64)granule.Size() >= Limits.GranuleOverloadSize) {
        if (PathsGranulesOverloaded[pathId].emplace(granule.GetGranuleId()).second) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "overloaded")("path_id", pathId)("granule", granule.GetGranuleId());
        }
    } else if (auto pi = PathsGranulesOverloaded.find(pathId); pi != PathsGranulesOverloaded.end()) {
        // Size is under limit. Remove granule from the overloaded set.
        if (pi->second.erase(granule.GetGranuleId())) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "reset_overload")("path_id", pathId)("granule", granule.GetGranuleId())("remained", pi->second.size());
        }
        // Remove entry for the pathId if there it has no overloaded granules any more.
        if (pi->second.empty()) {
            PathsGranulesOverloaded.erase(pi);
        }
    }
    Counters.OverloadGranules->Set(PathsGranulesOverloaded.size());
    if (IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD) && PathsGranulesOverloaded.size()) {
        TStringBuilder sb;
        for (auto&& i : PathsGranulesOverloaded) {
            sb << i.first << ":";
            bool isFirst = true;
            for (auto&& g : i.second) {
                if (!isFirst) {
                    sb << ",";
                } else {
                    isFirst = false;
                }
                sb << g;
            }
            sb << ";";
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("overload_granules", sb);
    }
}

} // namespace NKikimr::NOlap
