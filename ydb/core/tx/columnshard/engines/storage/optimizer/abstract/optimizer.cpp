#include "optimizer.h"
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>

namespace NKikimr::NOlap::NStorageOptimizer {

std::shared_ptr<TColumnEngineChanges> IOptimizerPlanner::GetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("path_id", PathId));
    return DoGetOptimizationTask(limits, granule, dataLocksManager);
}

} // namespace NKikimr::NOlap
