
#include "yql_yt_sorted_merge_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TSortedMergeStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
        const auto& operationParams = std::get<TSortedMergeOperationParams>(context.OperationParams);
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;

        auto sortedPartitionerSettings = GetSortedPartitionerSettings(fmrOperationSpec);

        TSortingColumns sortingColumns;
        sortingColumns.Columns = operationParams.Output.SortColumns;
        sortingColumns.SortOrders = operationParams.Output.SortOrder;

        auto sortedPartitioner = TSortedPartitioner(partIdsForTables, partIdStats, sortingColumns, sortedPartitionerSettings);

        std::vector<TOperationTableRef> inputTables = operationParams.Input;
        return PartitionInputTablesIntoTasksSorted(inputTables, sortedPartitioner);
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        const auto& sortedMergeOperationParams = std::get<TSortedMergeOperationParams>(context.OperationParams);

        std::vector<TGeneratedTaskInfo> generatedTasks;
        for (auto& task: context.PartitionResult.TaskInputs) {
            TSortedMergeTaskParams sortedMergeTaskParams;
            sortedMergeTaskParams.Input = task;
            sortedMergeTaskParams.Output = TFmrTableOutputRef(sortedMergeOperationParams.Output);

            if (sortedMergeTaskParams.Output.PartId.empty()) {
                TString newPartId = context.GenerateId();
                TString tableId = sortedMergeTaskParams.Output.TableId;
                sortedMergeTaskParams.Output.PartId = newPartId;
                context.PartIdsForTables[tableId].emplace_back(newPartId);
            }

            generatedTasks.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::SortedMerge,
                .TaskParams = std::move(sortedMergeTaskParams)
            });
        }

        return TGenerateTasksResult{.Tasks = std::move(generatedTasks)};
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeSortedMergeStageOperationManager() {
    return MakeIntrusive<TSortedMergeStageOperationManager>();
}

} // namespace NYql::NFmr
