#include "abstract.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

NArrow::NMerger::TIntervalPositions TCompactionTaskData::GetCheckPositions(
    const std::shared_ptr<arrow::Schema>& /*pkSchema*/, const bool withMoved) {
    NArrow::NMerger::TIntervalPositions result;
    for (auto&& i : GetFinishPoints(withMoved)) {
        result.AddPosition(i, false);
    }
    return result;
}

std::vector<NArrow::NMerger::TSortableBatchPosition> TCompactionTaskData::GetFinishPoints(const bool withMoved) {
    std::vector<NArrow::NMerger::TSortableBatchPosition> points;
    if (MemoryUsage > ((ui64)1 << 30)) {
        for (auto&& i : Portions) {
            if (!CurrentLevelPortionIds.contains(i->GetPortionId())) {
                points.emplace_back(i->IndexKeyStart().BuildSortablePosition());
            }
        }
        std::sort(points.begin(), points.end());
        return points;
    }

    THashSet<ui64> middlePortions;
    for (auto&& i : Chains) {
        for (auto&& p : i.GetPortions()) {
            middlePortions.emplace(p->GetPortionId());
        }
    }
    THashSet<ui64> endPortions;
    for (auto&& i : Chains) {
        if (!i.GetNotIncludedNextPortion()) {
            continue;
        }
        if (middlePortions.contains(i.GetNotIncludedNextPortion()->GetPortionId())) {
            continue;
        }
        if (!endPortions.emplace(i.GetNotIncludedNextPortion()->GetPortionId()).second) {
            continue;
        }
        points.emplace_back(i.GetNotIncludedNextPortion()->IndexKeyStart().BuildSortablePosition());
    }
    if (withMoved) {
        for (auto&& i : GetMovePortions()) {
            points.emplace_back(i->IndexKeyStart().BuildSortablePosition());
        }
    }
    if (StopSeparation) {
        points.emplace_back(StopSeparation->BuildSortablePosition());
    }
    std::sort(points.begin(), points.end());
    points.erase(std::unique(points.begin(), points.end()), points.end());
    return points;
}

}
