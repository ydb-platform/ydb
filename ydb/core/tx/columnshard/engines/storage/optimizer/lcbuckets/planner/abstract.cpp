#include "abstract.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

NKikimr::NArrow::NMerger::TIntervalPositions TCompactionTaskData::GetCheckPositions(
    const std::shared_ptr<arrow::Schema>& pkSchema, const bool withMoved) {
    NArrow::NMerger::TIntervalPositions result;
    for (auto&& i : GetFinishPoints(withMoved)) {
        result.AddPosition(NArrow::NMerger::TSortableBatchPosition(i.ToBatch(pkSchema), 0, pkSchema->field_names(), {}, false), false);
    }
    return result;
}

std::vector<NArrow::TReplaceKey> TCompactionTaskData::GetFinishPoints(const bool withMoved) {
    THashSet<ui64> middlePortions;
    for (auto&& i : Chains) {
        for (auto&& p : i.GetPortions()) {
            middlePortions.emplace(p->GetPortionId());
        }
    }
    std::vector<NArrow::TReplaceKey> points;
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
        points.emplace_back(i.GetNotIncludedNextPortion()->IndexKeyStart());
    }
    if (withMoved) {
        for (auto&& i : GetMovePortions()) {
            points.emplace_back(i->IndexKeyStart());
        }
    }
    std::sort(points.begin(), points.end());
    return points;
}

}
