
#include "yql_yt_merge_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

TPartitionResult TMergeStageOperationManager::PartitionOperationImpl(const TPrepareOperationStageContext& context) {
    const auto& operationParams = std::get<TMergeOperationParams>(context.OperationParams);
    const auto& fmrOperationSpec = context.FmrOperationSpec;
    const auto& clusterConnections = context.ClusterConnections;
    const auto& partIdsForTables = context.PartIdsForTables;
    const auto& partIdStats = context.PartIdStats;
    auto ytCoordinatorService = context.YtCoordinatorService;

    auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
    auto ytPartitionerSettings = GetYtPartitionerSettings(fmrOperationSpec);
    auto fmrPartitioner = TFmrPartitioner(partIdsForTables, partIdStats, fmrPartitionerSettings);

    std::vector<TYtTableRef> ytInputTables;
    std::vector<TFmrTableRef> fmrInputTables;
    for (auto& table: operationParams.Input) {
        if (auto ytTable = std::get_if<TYtTableRef>(&table)) {
            ytInputTables.emplace_back(*ytTable);
        } else {
            fmrInputTables.emplace_back(std::get<TFmrTableRef>(table));
        }
    }

    return PartitionInputTablesIntoTasks(ytInputTables, fmrInputTables, fmrPartitioner, ytCoordinatorService, clusterConnections, ytPartitionerSettings);
}

TGenerateTasksResult TMergeStageOperationManager::GenerateTasksImpl(
    const TGenerateTasksContext& context
) {
    const auto& mergeOperationParams = std::get<TMergeOperationParams>(context.OperationParams);

    std::vector<TGeneratedTaskInfo> generatedTasks;
    for (auto& task: context.PartitionResult.TaskInputs) {
        TMergeTaskParams mergeTaskParams;
        mergeTaskParams.Input = task;
        mergeTaskParams.Output = TFmrTableOutputRef(mergeOperationParams.Output);

        generatedTasks.push_back(TGeneratedTaskInfo{
            .TaskType = ETaskType::Merge,
            .TaskParams = std::move(mergeTaskParams)
        });
    }

    return TGenerateTasksResult{.Tasks = std::move(generatedTasks)};
}

IFmrStageOperationManager::TPtr MakeMergeStageOperationManager() {
    return MakeIntrusive<TMergeStageOperationManager>();
}

}
