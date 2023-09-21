#include "optimizer.h"
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>

namespace NKikimr::NOlap::NStorageOptimizer {

std::shared_ptr<TColumnEngineChanges> IOptimizerPlanner::GetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("granule_id", GranuleId));
    auto result = DoGetOptimizationTask(limits, granule, busyPortions);
    if (!!result) {
        auto portions = result->GetTouchedPortions();
        for (auto&& i : portions) {
            AFL_VERIFY(!busyPortions.contains(i))("portion_address", i.DebugString());
        }
    }
    return result;
}

} // namespace NKikimr::NOlap
