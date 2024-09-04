#include "portions_index.h"
#include "granule.h"

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

TPortionsIndex::TPortionIntervals TPortionsIndex::GetIntervalFeatures(const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions) const {
    auto itFrom = Points.find(inputPortion.IndexKeyStart());
    AFL_VERIFY(itFrom != Points.end());
    auto itTo = Points.find(inputPortion.IndexKeyEnd());
    AFL_VERIFY(itTo != Points.end());
    TPortionIntervals portionExcludeIntervals;
    while (true) {
        std::optional<NArrow::TReplaceKey> nextKey;
        for (auto&& [p, _] : itFrom->second.GetPortionIds()) {
            if (skipPortions.contains(p)) {
                continue;
            }
            const auto& portionCross = Owner.GetPortionVerified(p);
            if (!portionCross.CrossSSWith(inputPortion)) {
                continue;
            }
            if (!nextKey || *nextKey < portionCross.IndexKeyEnd()) {
                nextKey = portionCross.IndexKeyEnd();
            }
        }
        if (nextKey) {
            nextKey = std::min(inputPortion.IndexKeyEnd(), *nextKey);
            portionExcludeIntervals.Add(itFrom->first, *nextKey);
            auto itFromNext = Points.find(*nextKey);
            AFL_VERIFY(itFromNext != Points.end());
            if (itFromNext == itTo) {
                break;
            }
            if (itFromNext == itFrom) {
                ++itFrom;
            } else {
                itFrom = itFromNext;
            }
            AFL_VERIFY(itFrom != Points.end());
        } else {
            if (itFrom == itTo) {
                break;
            }
            ++itFrom;
            AFL_VERIFY(itFrom != Points.end());
        }

    }
    return portionExcludeIntervals;
}

void TPortionsIndex::RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
    auto itFrom = Points.find(p->IndexKeyStart());
    AFL_VERIFY(itFrom != Points.end());
    auto itTo = Points.find(p->IndexKeyEnd());
    AFL_VERIFY(itTo != Points.end());
    {
        const TPortionInfoStat stat(p);
        auto it = itFrom;
        while (true) {
            RemoveFromMemoryUsageControl(it->second.GetIntervalStats());
            it->second.RemoveContained(stat);
            RawMemoryUsage.Add(it->second.GetIntervalStats().GetMinRawBytes());
            BlobMemoryUsage.Add(it->second.GetIntervalStats().GetBlobBytes());
            if (it == itTo) {
                break;
            }
            AFL_VERIFY(++it != Points.end());
        }
    }
    if (itFrom != itTo) {
        itFrom->second.RemoveStart(p);
        if (itFrom->second.IsEmpty()) {
            RemoveFromMemoryUsageControl(itFrom->second.GetIntervalStats());
            Points.erase(itFrom);
        }
        itTo->second.RemoveFinish(p);
        if (itTo->second.IsEmpty()) {
            RemoveFromMemoryUsageControl(itTo->second.GetIntervalStats());
            Points.erase(itTo);
        }
    } else {
        itTo->second.RemoveStart(p);
        itTo->second.RemoveFinish(p);
        if (itTo->second.IsEmpty()) {
            RemoveFromMemoryUsageControl(itTo->second.GetIntervalStats());
            Points.erase(itTo);
        }
    }
    RawMemoryUsage.FlushCounters();
    BlobMemoryUsage.FlushCounters();
}

void TPortionsIndex::AddPortion(const std::shared_ptr<TPortionInfo>& p) {
    auto itFrom = InsertPoint(p->IndexKeyStart());
    itFrom->second.AddStart(p);
    auto itTo = InsertPoint(p->IndexKeyEnd());
    itTo->second.AddFinish(p);

    auto it = itFrom;
    const TPortionInfoStat stat(p);
    while (true) {
        RemoveFromMemoryUsageControl(it->second.GetIntervalStats());
        it->second.AddContained(stat);
        RawMemoryUsage.Add(it->second.GetIntervalStats().GetMinRawBytes());
        BlobMemoryUsage.Add(it->second.GetIntervalStats().GetBlobBytes());
        if (it == itTo) {
            break;
        }
        AFL_VERIFY(++it != Points.end());
    }
    RawMemoryUsage.FlushCounters();
    BlobMemoryUsage.FlushCounters();
}

}