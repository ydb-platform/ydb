#include "common_level.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

void TLevelPortions::DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) {
    for (auto&& i : remove) {
        auto it = Portions.find(i);
        AFL_VERIFY(it != Portions.end());
        AFL_VERIFY(it->GetPortion()->GetPortionId() == i->GetPortionId());
        PortionsInfo.RemovePortion(i);
        Portions.erase(it);
        LevelCounters.Portions->RemovePortion(i);
    }
    TStringBuilder sb;
    for (auto&& i : add) {
        sb << i->GetPortionId() << ",";
        auto info = Portions.emplace(i);
        i->AddRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        AFL_VERIFY(info.second);
        PortionsInfo.AddPortion(i);
        if (StrictOneLayer) {
            {
                auto it = info.first;
                ++it;
                if (it != Portions.end()) {
                    AFL_VERIFY(i->IndexKeyEnd() < it->GetStart())("start", i->IndexKeyStart().DebugString())("end", i->IndexKeyEnd().DebugString())(
                                                      "next", it->GetStart().DebugString())("next1", it->GetStart().DebugString())(
                                                      "next2", it->GetPortion()->IndexKeyEnd().DebugString())("level_id", GetLevelId())(
                                                      "portion_id_new", i->GetPortionId())("portion_id_old", it->GetPortion()->GetPortionId())(
                                                      "portion_old", it->GetPortion()->DebugString())("add", sb);
                }
            }
            {
                auto it = info.first;
                if (it != Portions.begin()) {
                    --it;
                    AFL_VERIFY(it->GetPortion()->IndexKeyEnd() < i->IndexKeyStart())
                    ("start", i->IndexKeyStart().DebugString())("finish", i->IndexKeyEnd().DebugString())("pred_start",
                        it->GetPortion()->IndexKeyStart().DebugString())("pred_finish", it->GetPortion()->IndexKeyEnd().DebugString())("level_id", GetLevelId())(
                        "portion_id_new", i->GetPortionId())("portion_id_old", it->GetPortion()->GetPortionId())("add", sb);
                }
            }
        }
        LevelCounters.Portions->AddPortion(i);
    }
}

TCompactionTaskData TLevelPortions::DoGetOptimizationTask() const {
    AFL_VERIFY(GetNextLevel());
    ui64 compactedData = 0;
    TCompactionTaskData result(GetNextLevel()->GetLevelId());
    auto itFwd = Portions.begin();
    AFL_VERIFY(itFwd != Portions.end());
    auto itBkwd = itFwd;
    if (itFwd != Portions.begin()) {
        --itBkwd;
    }
    while (GetLevelBlobBytesLimit() * 0.5 + compactedData < (ui64)PortionsInfo.GetBlobBytes() &&
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

}
