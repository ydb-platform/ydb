
#include "yql_yt_map_stage_operation_manager.h"

#include <algorithm>

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

TPartitionResult TMapStageOperationManager::PartitionOperationImpl(const TPrepareOperationStageContext& context) {
    const auto& operationParams = std::get<TMapOperationParams>(context.OperationParams);
    const auto& fmrOperationSpec = context.FmrOperationSpec;
    const auto& clusterConnections = context.ClusterConnections;
    const auto& partIdsForTables = context.PartIdsForTables;
    const auto& partIdStats = context.PartIdStats;
    auto ytCoordinatorService = context.YtCoordinatorService;

    if (operationParams.IsOrdered) {
        // Ordered map -> ordered partition
        auto orderedPartitionerSettings = GetOrderedPartitionerSettings(fmrOperationSpec);
        auto orderedPartitioner = TOrderedPartitioner(partIdsForTables, partIdStats, orderedPartitionerSettings);

        std::vector<TOperationTableRef> inputTables = operationParams.Input;
        return PartitionInputTablesIntoTasksOrdered(inputTables, orderedPartitioner, ytCoordinatorService, clusterConnections);
    } else {
        // Unordered map -> unordered partition
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
}

TGenerateTasksResult TMapStageOperationManager::GenerateTasksImpl(
    const TGenerateTasksContext& context
) {
    const auto& mapOperationParams = std::get<TMapOperationParams>(context.OperationParams);

    std::vector<TGeneratedTaskInfo> generatedTasks;
    for (auto& task: context.PartitionResult.TaskInputs) {
        TMapTaskParams mapTaskParams;
        mapTaskParams.Input = task;
        std::vector<TFmrTableOutputRef> fmrTableOutputRefs;
        std::transform(mapOperationParams.Output.begin(), mapOperationParams.Output.end(), std::back_inserter(fmrTableOutputRefs), [] (const TFmrTableRef& fmrTableRef) {
            return TFmrTableOutputRef(fmrTableRef);
        });

        mapTaskParams.Output = fmrTableOutputRefs;
        mapTaskParams.SerializedMapJobState = mapOperationParams.SerializedMapJobState;
        mapTaskParams.IsOrdered = mapOperationParams.IsOrdered;

        generatedTasks.push_back(TGeneratedTaskInfo{
            .TaskType = ETaskType::Map,
            .TaskParams = std::move(mapTaskParams)
        });
    }

    return TGenerateTasksResult{.Tasks = std::move(generatedTasks)};
}

IFmrStageOperationManager::TPtr MakeMapStageOperationManager() {
    return MakeIntrusive<TMapStageOperationManager>();
}

}
