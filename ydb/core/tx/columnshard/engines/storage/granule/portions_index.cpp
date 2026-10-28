#include "granule.h"
#include "portions_index.h"

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

std::vector<TPortionsIndex::TKeyInterval> TPortionsIndex::GetOlderIntervals(
    const TPortions& portions, const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions) {
    const NArrow::TSimpleRow inputStart = inputPortion.IndexKeyStart();
    const NArrow::TSimpleRow inputEnd = inputPortion.IndexKeyEnd();
    std::vector<TKeyInterval> intervals;
    // A persisted `inputPortion` is listed in `skipPortions`. An intermediate merge result is absent from `portions` but carries
    // a temporary id that may collide with the id of a persisted portion, so ids are never compared with the one of `inputPortion`.
    for (const auto& p : portions) {
        if (skipPortions.contains(p->GetPortionId())) {
            continue;
        }
        TKeyInterval interval(p->IndexKeyStart(), p->IndexKeyEnd());
        if (inputEnd < interval.first || interval.second < inputStart) {
            continue;
        }
        if (inputPortion.RecordSnapshotMax() < p->RecordSnapshotMin()) {
            continue;
        }
        intervals.emplace_back(std::move(interval));
    }
    std::sort(intervals.begin(), intervals.end(), [](const TKeyInterval& l, const TKeyInterval& r) {
        return l.first < r.first;
    });
    std::vector<TKeyInterval> result;
    for (auto&& interval : intervals) {
        if (result.empty() || result.back().second < interval.first) {
            result.emplace_back(std::move(interval));
        } else if (result.back().second < interval.second) {
            result.back().second = std::move(interval.second);
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NGranule::NPortionsIndex
