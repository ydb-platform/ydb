#include "zero_level.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TCompactionTaskData TZeroLevelPortions::DoGetOptimizationTask() const {
    AFL_VERIFY(Portions.size());
    auto targetLevel = GetTargetLevelVerified();
    TCompactionTaskData result(targetLevel->GetLevelId());
    AFL_VERIFY(targetLevel);
    for (auto&& i : Portions) {
        result.AddCurrentLevelPortion(
            i.GetPortion(), targetLevel->GetAffectedPortions(i.GetPortion()->IndexKeyStart(), i.GetPortion()->IndexKeyEnd()), true);
        if (!result.CanTakeMore()) {
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
    if (PortionsInfo.GetCount() <= 10) {
        return 0;
    }

    THashSet<ui64> portionIds;
    TSimplePortionsGroupInfo portionsInfo;
    auto targetLevel = GetTargetLevelVerified();

    for (auto&& i : Portions) {
        auto chain = targetLevel->GetAffectedPortions(i.GetPortion()->IndexKeyStart(), i.GetPortion()->IndexKeyEnd());
        if (chain) {
            for (auto&& p : chain->GetPortions()) {
                if (portionIds.emplace(p->GetPortionId()).second) {
                    portionsInfo.AddPortion(p);
                }
            }
        }
    }

    const ui64 mb = (portionsInfo.GetRawBytes() + PortionsInfo.GetRawBytes()) / 1000000 + 1;
    return 1000000000.0 * PortionsInfo.GetCount() * PortionsInfo.GetCount() / mb;
}

}
