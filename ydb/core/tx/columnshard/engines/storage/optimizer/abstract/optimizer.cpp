#include "optimizer.h"
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NStorageOptimizer {

std::shared_ptr<TColumnEngineChanges> IOptimizerPlanner::GetOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    NActors::TLogContextGuard g(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("path_id", PathId));
    return DoGetOptimizationTask(granule, dataLocksManager);
}

IOptimizerPlanner::TModificationGuard& IOptimizerPlanner::TModificationGuard::AddPortion(const std::shared_ptr<TPortionInfo>& portion) {
    AFL_VERIFY(AddPortions.emplace(portion->GetPortionId(), portion).second);
    return*this;
}

IOptimizerPlanner::TModificationGuard& IOptimizerPlanner::TModificationGuard::RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
    AFL_VERIFY(RemovePortions.emplace(portion->GetPortionId(), portion).second);
    return*this;
}

} // namespace NKikimr::NOlap
