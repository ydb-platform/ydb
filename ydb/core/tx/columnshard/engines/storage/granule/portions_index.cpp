#include "granule.h"
#include "portions_index.h"

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

TPortionsIndex::TPortionsSnapshot::TConstPtr TPortionsIndex::GetPortionsSnapshot() const {
    TPortions portions;
    portions.reserve(Portions.size());
    for (const auto& [_, portion] : Portions) {
        portions.emplace_back(portion);
    }
    return std::make_shared<const TPortionsSnapshot>(std::move(portions), Owner.GetLoadedKeyRanges());
}

std::vector<TPortionsIndex::TKeyInterval> TPortionsIndex::TPortionsSnapshot::FindOverlappingKeyRanges(
    const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions) const {
    const NArrow::TSimpleRow inputStart = inputPortion.IndexKeyStart();
    const NArrow::TSimpleRow inputEnd = inputPortion.IndexKeyEnd();
    if (!LoadedKeyRanges.Covers(inputStart, inputEnd)) {
        return { TKeyInterval(inputStart, inputEnd) };
    }
    std::vector<TKeyInterval> intervals;
    // A persisted `inputPortion` is listed in `skipPortions`. An intermediate merge result is absent from `Portions` but carries
    // a temporary id that may collide with the id of a persisted portion, so ids are never compared with the one of `inputPortion`.
    for (const auto& p : Portions) {
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
