#include "one_layer.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

std::vector<TPortionInfo::TPtr> TOneLayerPortions::DoModifyPortions(
    const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) {
    std::vector<TPortionInfo::TPtr> problems;
    for (auto&& i : remove) {
        Portions.Erase(i);
    }
    TStringBuilder sb;
    for (auto&& i : add) {
        sb << i->GetPortionId() << ",";
        auto it = Portions.Emplace(i);
        TOrderedPortion info = *it;
        if (StrictOneLayer) {
            {
                ++it;
                if (it != Portions.GetPortions().end() && it->GetStart() <= i->IndexKeyEnd()) {
                    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_COMPACTION)("start", i->IndexKeyStart().DebugString())(
                        "end", i->IndexKeyEnd().DebugString())("next", it->GetStart().DebugString())("next1", it->GetStart().DebugString())(
                        "next2", it->GetPortion()->IndexKeyEnd().DebugString())("level_id", GetLevelId())("portion_id_new", i->GetPortionId())(
                        "portion_id_old", it->GetPortion()->GetPortionId())("portion_old", it->GetPortion()->DebugString())("add", sb);
                    problems.emplace_back(i);
                    Portions.Erase(info);
                    continue;
                }
            }
            {
                if (it != Portions.GetPortions().begin()) {
                    --it;
                    if (i->IndexKeyStart() <= it->GetPortion()->IndexKeyEnd()) {
                        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_COMPACTION)("start", i->IndexKeyStart().DebugString())(
                            "finish", i->IndexKeyEnd().DebugString())("pred_start", it->GetPortion()->IndexKeyStart().DebugString())(
                            "pred_finish", it->GetPortion()->IndexKeyEnd().DebugString())("level_id", GetLevelId())(
                            "portion_id_new", i->GetPortionId())("portion_id_old", it->GetPortion()->GetPortionId())("add", sb);
                        problems.emplace_back(i);
                        Portions.Erase(info);
                        continue;
                    }
                }
            }
        }
    }
    return problems;
}

std::vector<TCompactionTaskData> TOneLayerPortions::DoGetOptimizationTasks() const {
    AFL_VERIFY(GetNextLevel());
    ui64 compactedData = 0;
    TCompactionTaskData result(GetNextLevel()->GetLevelId());
    auto itFwd = Portions.GetPortions().begin();
    AFL_VERIFY(itFwd != Portions.GetPortions().end());
    auto itBkwd = itFwd;
    if (itFwd != Portions.GetPortions().begin()) {
        --itBkwd;
    }
    while (GetLevelBlobBytesLimit() * 0.5 + compactedData < (ui64)GetPortionsInfo().GetBlobBytes() &&
           (itBkwd != Portions.GetPortions().begin() || itFwd != Portions.GetPortions().end()) && result.CanTakeMore()) {
        if (itFwd != Portions.GetPortions().end() &&
            (itBkwd == Portions.GetPortions().begin() || itBkwd->GetPortion()->GetTotalBlobBytes() <= itFwd->GetPortion()->GetTotalBlobBytes())) {
            auto portion = itFwd->GetPortion();
            compactedData += portion->GetTotalBlobBytes();
            result.AddCurrentLevelPortion(portion, GetNextLevel()->GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd()), false);
            ++itFwd;
        } else if (itBkwd != Portions.GetPortions().begin() &&
                   (itFwd == Portions.GetPortions().end() || itFwd->GetPortion()->GetTotalBlobBytes() < itBkwd->GetPortion()->GetTotalBlobBytes())) {
            auto portion = itBkwd->GetPortion();
            compactedData += portion->GetTotalBlobBytes();
            result.AddCurrentLevelPortion(portion, GetNextLevel()->GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd()), false);
            --itBkwd;
        }
    }
    return { result };
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
