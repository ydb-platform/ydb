
#include "yql_yt_sorted_upload_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

TPartitionResult TSortedUploadStageOperationManager::PartitionOperationImpl(const TPrepareOperationStageContext& context) {
    YQL_ENSURE(context.ClusterConnections.size() == 1, "SortedUpload should have exactly one cluster connection");

    const auto& operationParams = std::get<TSortedUploadOperationParams>(context.OperationParams);
    const auto& fmrOperationSpec = context.FmrOperationSpec;
    const auto& clusterConnections = context.ClusterConnections;
    const auto& partIdsForTables = context.PartIdsForTables;
    const auto& partIdStats = context.PartIdStats;
    auto ytCoordinatorService = context.YtCoordinatorService;

    auto orderedPartitionerSettings = GetOrderedPartitionerSettings(fmrOperationSpec);
    auto orderedPartitioner = TOrderedPartitioner(partIdsForTables, partIdStats, orderedPartitionerSettings);

    std::vector<TOperationTableRef> inputTables = {operationParams.Input};
    return PartitionInputTablesIntoTasksOrdered(inputTables, orderedPartitioner, ytCoordinatorService, clusterConnections);
}

TGenerateTasksResult TSortedUploadStageOperationManager::GenerateTasksImpl(
    const TGenerateTasksContext& context
) {
    const auto& sortedUploadOperationParams = std::get<TSortedUploadOperationParams>(context.OperationParams);

    std::vector<TGeneratedTaskInfo> generatedTasks;
    ui64 taskOrder = 0;
    for (auto& task: context.PartitionResult.TaskInputs) {
        TSortedUploadTaskParams sortedUploadTaskParams;
        YQL_ENSURE(task.Inputs.size() == 1, "Distributed upload task should have exactly one fmr table partition input");
        auto& fmrTablePart = task.Inputs[0];
        sortedUploadTaskParams.Input = std::get<TFmrTableInputRef>(fmrTablePart);
        sortedUploadTaskParams.Output = sortedUploadOperationParams.Output;
        sortedUploadTaskParams.CookieYson = sortedUploadOperationParams.Cookies[taskOrder];
        sortedUploadTaskParams.Order = taskOrder;

        generatedTasks.push_back(TGeneratedTaskInfo{
            .TaskType = ETaskType::SortedUpload,
            .TaskParams = std::move(sortedUploadTaskParams)
        });
        taskOrder++;
    }

    FragmentResultsYson_.resize(generatedTasks.size());

    return TGenerateTasksResult{.Tasks = std::move(generatedTasks)};
}

void TSortedUploadStageOperationManager::OnTaskCompleted(const TStatistics& stats) {
    if (auto* taskSortedUploadResult = std::get_if<TTaskSortedUploadResult>(&stats.TaskResult)) {
        FragmentResultsYson_[taskSortedUploadResult->FragmentOrder] = taskSortedUploadResult->FragmentResultYson;
    }
}

std::vector<TString> TSortedUploadStageOperationManager::GetOperationResult() {
    return std::move(FragmentResultsYson_);
}

IFmrStageOperationManager::TPtr MakeSortedUploadStageOperationManager() {
    return MakeIntrusive<TSortedUploadStageOperationManager>();
}

}
