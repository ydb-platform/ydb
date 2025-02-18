#include "zero_level.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TCompactionTaskData TZeroLevelPortions::DoGetOptimizationTask() const {
    AFL_VERIFY(Portions.size());
    TCompactionTaskData result(NextLevel->GetLevelId());
    for (auto&& i : Portions) {
        result.AddCurrentLevelPortion(
            i.GetPortion(), NextLevel->GetAffectedPortions(i.GetPortion()->IndexKeyStart(), i.GetPortion()->IndexKeyEnd()), true);
        if (!result.CanTakeMore()) {
//            result.SetStopSeparation(i.GetPortion()->IndexKeyStart());
            break;
        }
    }
    if (result.CanTakeMore()) {
        PredOptimization = TInstant::Now();
    } else {
        PredOptimization = std::nullopt;
    }
    return result;
}

ui64 TZeroLevelPortions::DoGetWeight() const {
    if (!NextLevel || Portions.size() < PortionsCountAvailable) {
        return 0;
    }
    if (PredOptimization && TInstant::Now() - *PredOptimization < DurationToDrop) {
        if (PortionsInfo.PredictPackedBlobBytes(GetPackKff()) < ExpectedBlobsSize) {
            return 0;
        }
    }

    const ui64 affectedRawBytes =
        NextLevel->GetAffectedPortionBytes(Portions.begin()->GetPortion()->IndexKeyStart(), Portions.rbegin()->GetPortion()->IndexKeyEnd());
    /*
    THashSet<ui64> portionIds;
    auto chain =
        targetLevel->GetAffectedPortions(Portions.begin()->GetPortion()->IndexKeyStart(), Portions.rbegin()->GetPortion()->IndexKeyEnd());
    ui64 affectedRawBytes = 0;
    if (chain) {
        auto it = Portions.begin();
        auto itNext = chain->GetPortions().begin();
        while (it != Portions.end() && itNext != chain->GetPortions().end()) {
            const auto& nextLevelPortion = *itNext;
            if (nextLevelPortion->IndexKeyEnd() < it->GetPortion()->IndexKeyStart()) {
                ++itNext;
            } else if (it->GetPortion()->IndexKeyEnd() < nextLevelPortion->IndexKeyStart()) {
                ++it;
            } else {
                if (portionIds.emplace(nextLevelPortion->GetPortionId()).second) {
                    affectedRawBytes += nextLevelPortion->GetTotalRawBytes();
                }
                ++itNext;
            }
        }
    }
*/

    const ui64 mb = (affectedRawBytes + PortionsInfo.GetRawBytes()) / 1000000 + 1;
    return 1000.0 * PortionsInfo.GetCount() * PortionsInfo.GetCount() / mb;
}

TInstant TZeroLevelPortions::DoGetWeightExpirationInstant() const {
    if (!PredOptimization) {
        return TInstant::Max();
    }
    return *PredOptimization + DurationToDrop;
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
