#include "portions_index.h"
#include "granule.h"

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

bool TPortionsIndex::HasOlderIntervals(const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions) const {
    for (auto&& [_, p] : Portions) {
        if (p->GetPortionId() == inputPortion.GetPortionId()) {
            continue;
        }
        if (inputPortion.IndexKeyEnd() < p->IndexKeyStart()) {
            continue;
        }
        if (p->IndexKeyEnd() < inputPortion.IndexKeyStart()) {
            continue;
        }
        if (skipPortions.contains(p->GetPortionId())) {
            continue;
        }
        if (inputPortion.RecordSnapshotMax() < p->RecordSnapshotMin()) {
            continue;
        }
        return true;
    }
    return false;
}

}