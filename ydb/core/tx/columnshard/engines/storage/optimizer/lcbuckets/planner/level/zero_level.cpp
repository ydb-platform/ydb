#include "zero_level.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

std::vector<TCompactionTaskData> TZeroLevelPortions::DoGetOptimizationTasks() const {
    std::vector<TCompactionTaskData> result;
    AFL_VERIFY(Portions.size());
    result.emplace_back(NextLevel->GetLevelId(), CompactAtLevel ? NextLevel->GetExpectedPortionSize() : std::optional<ui64>());
    i64 tasksLeft = GetMaxConcurrency();
    for (auto&& i : Portions) {
        result.back().AddCurrentLevelPortion(
            i.GetPortion(), NextLevel->GetAffectedPortions(i.GetPortion()->IndexKeyStart(), i.GetPortion()->IndexKeyEnd()), true);
        if (!result.back().CanTakeMore()) {
            //            result.SetStopSeparation(i.GetPortion()->IndexKeyStart());
            if (--tasksLeft <= 0) {
                break;
            }
            result.emplace_back(NextLevel->GetLevelId(), CompactAtLevel ? NextLevel->GetExpectedPortionSize() : std::optional<ui64>());
        }
    }
    
    if (result.back().IsEmpty()) {
        result.pop_back();
    }
    AFL_VERIFY(!result.empty());

    if (result.back().CanTakeMore()) {
        PredOptimization = TInstant::Now();
    } else {
        PredOptimization = std::nullopt;
    }
    return result;
}

ui64 TZeroLevelPortions::GetMaxConcurrency() const {
    return std::clamp(ui64(GetPortionsInfo().PredictPackedBlobBytes(GetPackKff()) / std::max(NextLevel->GetExpectedPortionSize(), GetExpectedPortionSize())), ui64(1), Concurrency);
}

ui64 TZeroLevelPortions::DoGetWeight(bool highPriority) const {
    if (NYDBTest::TControllers::GetColumnShardController()->GetCompactionControl() == NYDBTest::EOptimizerCompactionWeightControl::Disable) {
        return 0;
    }
    if (!NextLevel || Portions.size() < PortionsCountAvailable || Portions.empty()) {
        return 0;
    }
    if (PredOptimization && TInstant::Now() - *PredOptimization < DurationToDrop) {
        if (GetPortionsInfo().PredictPackedBlobBytes(GetPackKff()) < std::max(NextLevel->GetExpectedPortionSize(), GetExpectedPortionSize())) {
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

    const ui64 mb = (affectedRawBytes + GetPortionsInfo().GetRawBytes()) / 1000000 + 1;
    return (highPriority ? HighPriorityContribution : 0) | ui64(1000.0 * GetPortionsInfo().GetCount() * GetPortionsInfo().GetCount() / mb);
}

TInstant TZeroLevelPortions::DoGetWeightExpirationInstant() const {
    if (!PredOptimization) {
        return TInstant::Max();
    }
    return *PredOptimization + DurationToDrop;
}

TZeroLevelPortions::TZeroLevelPortions(const ui32 levelIdx, const std::shared_ptr<IPortionsLevel>& nextLevel,
    const TLevelCounters& levelCounters, const std::shared_ptr<IOverloadChecker>& overloadChecker, const TDuration durationToDrop,
    const ui64 expectedBlobsSize, const ui64 portionsCountAvailable, const std::vector<std::shared_ptr<IPortionsSelector>>& selectors,
    const TString& defaultSelectorName, const ui64 concurrency, const ui64 highPriorityContribution, bool compactAtLevel)
    : TBase(levelIdx, nextLevel, overloadChecker, levelCounters, selectors, defaultSelectorName)
    , DurationToDrop(durationToDrop)
    , ExpectedBlobsSize(expectedBlobsSize)
    , PortionsCountAvailable(portionsCountAvailable)
    , HighPriorityContribution(highPriorityContribution)
    , CompactAtLevel(compactAtLevel)
    , Concurrency(concurrency) {
    if (DurationToDrop != TDuration::Max() && PredOptimization) {
        *PredOptimization -= TDuration::Seconds(RandomNumber<ui32>(DurationToDrop.Seconds()));
    }
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
