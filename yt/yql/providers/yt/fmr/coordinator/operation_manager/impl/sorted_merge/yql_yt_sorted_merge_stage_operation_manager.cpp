
#include "yql_yt_sorted_merge_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TSortedMergeStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TSortedMergeStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
        const auto& operationParams = std::get<TSortedMergeOperationParams>(context.OperationParams);
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;
        const auto& clusterConnections = context.ClusterConnections;

        std::vector<TYtTableRef> ytInputTables;
        std::vector<TFmrTableRef> fmrInputTables;
        for (const auto& table : operationParams.Input) {
            if (const auto* ytTable = std::get_if<TYtTableRef>(&table)) {
                ytInputTables.emplace_back(*ytTable);
            } else {
                fmrInputTables.emplace_back(std::get<TFmrTableRef>(table));
            }
        }

        if (!ytInputTables.empty() && !fmrInputTables.empty()) {
            return TPartitionResult{.Error = TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::FallbackOperation,
                .ErrorMessage = "SortedMerge does not support mixed YT and FMR inputs"
            }};
        }

        if (!ytInputTables.empty()) {
            YQL_ENSURE(ytInputTables.size() == 1, "SortedMerge supports at most one YT input table");
            auto ytPartitionerSettings = GetYtPartitionerSettings(fmrOperationSpec);
            ytPartitionerSettings.PartitionMode = NYT::ETablePartitionMode::Ordered;
            auto [ytTasks, ytPartitionStatus] = context.YtCoordinatorService->PartitionYtTables(ytInputTables, clusterConnections, ytPartitionerSettings);
            if (!ytPartitionStatus) {
                YQL_CLOG(WARN, FastMapReduce) << "FMR fallback to YT: failed to partition single YT input table for SortedMerge";
                return TPartitionResult{.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::FallbackOperation,
                    .ErrorMessage = "Failed to partition single YT input table for SortedMerge"
                }};
            }
            std::vector<TTaskTableInputRef> taskInputs;
            taskInputs.reserve(ytTasks.size());
            for (auto& ytTask : ytTasks) {
                taskInputs.emplace_back(TTaskTableInputRef{.Inputs = {std::move(ytTask)}});
            }
            return TPartitionResult{.TaskInputs = std::move(taskInputs)};
        }

        auto sortedPartitionerSettings = GetSortedPartitionerSettings(fmrOperationSpec);

        TSortingColumns sortingColumns;
        sortingColumns.Columns = operationParams.Output.SortColumns;
        sortingColumns.SortOrders = operationParams.Output.SortOrder;

        auto sortedPartitioner = TSortedPartitioner(partIdsForTables, partIdStats, sortingColumns, sortedPartitionerSettings);

        std::vector<TOperationTableRef> inputTables = operationParams.Input;
        return sortedPartitioner.PartitionTablesIntoTasks(inputTables);
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        const auto& sortedMergeOperationParams = std::get<TSortedMergeOperationParams>(context.OperationParams);
        TGenerateTasksResult result;
        std::vector<TGeneratedTaskInfo> generatedTasks;

        YQL_CLOG(INFO, FastMapReduce) << "Starting SortedMerge operation";

        for (auto& task: context.PartitionResult.TaskInputs) {
            TSortedMergeTaskParams sortedMergeTaskParams;
            sortedMergeTaskParams.Input = task;
            sortedMergeTaskParams.Output = TFmrTableOutputRef(sortedMergeOperationParams.Output);

            if (sortedMergeTaskParams.Output.PartId.empty()) {
                TString newPartId = GenerateId();
                TString tableId = sortedMergeTaskParams.Output.TableId;
                sortedMergeTaskParams.Output.PartId = newPartId;
                result.PartIdsToUpdate[tableId].emplace_back(newPartId);
            }

            generatedTasks.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::SortedMerge,
                .TaskParams = std::move(sortedMergeTaskParams)
            });
        }
        result.Tasks = std::move(generatedTasks);

        return result;
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) override {
        TGetNewPartIdsForTaskResult result;
        TSortedMergeTaskParams& sortedMergeTaskParams = std::get<TSortedMergeTaskParams>(context.Task->TaskParams);

        if (sortedMergeTaskParams.Output.PartId.empty()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "SortedMerge task has empty output PartId, fallback to native gateway is required",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
            TString tableId = sortedMergeTaskParams.Output.TableId;
            TString partId = sortedMergeTaskParams.Output.PartId;

            TFmrError notFoundError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "SortedMerge task output PartId is missing in coordinator part list, fallback to native gateway is required",
                .TaskId = context.TaskId,
                .OperationId = context.OperationId
            };

            const auto& partIdsIter = context.PartIdsForTables.find(tableId);
            if (partIdsIter == context.PartIdsForTables.end()) {
                result.Error = notFoundError;
                return result;
            }

            const auto& partIds = partIdsIter->second;
            if (std::find(partIds.begin(), partIds.end(), partId) == partIds.end()) {
                result.Error = notFoundError;
            }
            return result;
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& params) const override {
        const auto& sortedMergeParams = std::get<TSortedMergeOperationParams>(params);
        return {sortedMergeParams.Output.FmrTableId.Id};
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& context) override {
        std::vector<TPartIdInfo> groupsToClear;
        TSortedMergeTaskParams& sortedMergeTaskParams = std::get<TSortedMergeTaskParams>(context.Task->TaskParams);
        TString tableId = sortedMergeTaskParams.Output.TableId;
        if (!sortedMergeTaskParams.Output.PartId.empty() && context.PartIdStats.contains(sortedMergeTaskParams.Output.PartId)) {
            auto prevPartId = sortedMergeTaskParams.Output.PartId;
            groupsToClear.emplace_back(tableId, prevPartId);
        }
        return groupsToClear;
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeSortedMergeStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TSortedMergeStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
