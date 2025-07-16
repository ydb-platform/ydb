#include "common_level.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

std::vector<TPortionInfo::TPtr> TOneLayerPortions::DoModifyPortions(
    const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) {
    std::vector<TPortionInfo::TPtr> problems;
    for (auto&& i : remove) {
        auto it = Portions.find(TOrderedPortion(i));
        AFL_VERIFY(it != Portions.end());
        AFL_VERIFY(it->GetPortion()->GetPortionId() == i->GetPortionId());
        Portions.erase(it);
    }
    TStringBuilder sb;
    for (auto&& i : add) {
        sb << i->GetPortionId() << ",";
        auto info = Portions.emplace(i);
        AFL_VERIFY(info.second);
        if (StrictOneLayer) {
            {
                auto it = info.first;
                ++it;
                if (it != Portions.end() && it->GetStart() <= i->IndexKeyEnd()) {
                    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_COMPACTION)("start", i->IndexKeyStart().DebugString())(
                        "end", i->IndexKeyEnd().DebugString())("next", it->GetStart().DebugString())("next1", it->GetStart().DebugString())(
                        "next2", it->GetPortion()->IndexKeyEnd().DebugString())("level_id", GetLevelId())("portion_id_new", i->GetPortionId())(
                        "portion_id_old", it->GetPortion()->GetPortionId())("portion_old", it->GetPortion()->DebugString())("add", sb);
                    problems.emplace_back(i);
                    Portions.erase(info.first);
                    continue;
                }
            }
            {
                auto it = info.first;
                if (it != Portions.begin()) {
                    --it;
                    if (i->IndexKeyStart() <= it->GetPortion()->IndexKeyEnd()) {
                        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_COMPACTION)("start", i->IndexKeyStart().DebugString())(
                            "finish", i->IndexKeyEnd().DebugString())("pred_start", it->GetPortion()->IndexKeyStart().DebugString())(
                            "pred_finish", it->GetPortion()->IndexKeyEnd().DebugString())("level_id", GetLevelId())(
                            "portion_id_new", i->GetPortionId())("portion_id_old", it->GetPortion()->GetPortionId())("add", sb);
                        problems.emplace_back(i);
                        Portions.erase(info.first);
                        continue;
                    }
                }
            }
        }
    }
    return problems;
}

TCompactionTaskData TOneLayerPortions::DoGetOptimizationTask() const {
    AFL_VERIFY(GetNextLevel());
    ui64 compactedData = 0;
    TCompactionTaskData result(GetNextLevel()->GetLevelId());
    auto itFwd = Portions.begin();
    AFL_VERIFY(itFwd != Portions.end());
    auto itBkwd = itFwd;
    if (itFwd != Portions.begin()) {
        --itBkwd;
    }
    while (GetLevelBlobBytesLimit() * 0.5 + compactedData < (ui64)GetPortionsInfo().GetBlobBytes() &&
           (itBkwd != Portions.begin() || itFwd != Portions.end()) && result.CanTakeMore()) {
        if (itFwd != Portions.end() &&
            (itBkwd == Portions.begin() || itBkwd->GetPortion()->GetTotalBlobBytes() <= itFwd->GetPortion()->GetTotalBlobBytes())) {
            auto portion = itFwd->GetPortion();
            compactedData += portion->GetTotalBlobBytes();
            result.AddCurrentLevelPortion(portion, GetNextLevel()->GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd()), false);
            ++itFwd;
        } else if (itBkwd != Portions.begin() &&
                   (itFwd == Portions.end() || itFwd->GetPortion()->GetTotalBlobBytes() < itBkwd->GetPortion()->GetTotalBlobBytes())) {
            auto portion = itBkwd->GetPortion();
            compactedData += portion->GetTotalBlobBytes();
            result.AddCurrentLevelPortion(portion, GetNextLevel()->GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd()), false);
            --itBkwd;
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
