#include "optimizer.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NStorageOptimizer {

std::vector<std::shared_ptr<TColumnEngineChanges>> IOptimizerPlanner::GetOptimizationTasks(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
        {"pathId", PathId});
    return DoGetOptimizationTasks(granule, dataLocksManager);
}

std::shared_ptr<TColumnEngineChanges> IOptimizerPlanner::GetNextOptimizationTask(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
        {"pathId", PathId});
    return DoGetNextOptimizationTask(granule, dataLocksManager);
}

std::shared_ptr<TColumnEngineChanges> IOptimizerPlanner::DoGetNextOptimizationTask(
    std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
    const auto tasks = DoGetOptimizationTasks(granule, dataLocksManager);
    return tasks.empty() ? nullptr : tasks.front();
}

IOptimizerPlanner::TModificationGuard& IOptimizerPlanner::TModificationGuard::AddPortion(const std::shared_ptr<TPortionInfo>& portion) {
    AddPortions.emplace_back(portion);
    return *this;
}

IOptimizerPlanner::TModificationGuard& IOptimizerPlanner::TModificationGuard::RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
    RemovePortions.emplace_back(portion);
    return *this;
}

ui64 IOptimizerPlanner::GetBadPortionsLimit() const {
    if (AppDataVerified().ColumnShardConfig.GetBadPortionsLimit()) {
        return AppDataVerified().ColumnShardConfig.GetBadPortionsLimit();
    }
    return 2 * GetNodePortionsCountLimit();
}

std::shared_ptr<IOptimizerPlannerConstructor> IOptimizerPlannerConstructor::BuildDefault() {
    return BuildDefault(NKikimrConfig::TColumnShardConfig::default_instance().GetDefaultCompactionPreset());
}

}   // namespace NKikimr::NOlap::NStorageOptimizer
