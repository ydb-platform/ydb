
#include "yql_yt_sort_stage_operation_manager.h"
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TSortStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TSortStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) {
        const auto currentStage = StagePlan_[CurrentStageIndex_];
        switch (currentStage) {
            case ETaskType::LocalSort:
                return DoUnorderedPartition(context);
            case ETaskType::SortedMerge:
                return DoSortedPartition(context);
            default:
                YQL_ENSURE(false, "Unknown stage type");
        }
        return TPartitionResult{};
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        const auto currentStage = StagePlan_[CurrentStageIndex_];
        switch (currentStage) {
            case ETaskType::LocalSort:
                return GenerateLocalSortTasksImpl(context);
            case ETaskType::SortedMerge:
                return GenerateMergeSortTasksImpl(context);
            default:
                YQL_ENSURE(false, "Unknown stage type");
        }
        return TGenerateTasksResult{};
    }


    TAdvanceStageResult AdvanceToNextStage() final {
        CurrentStageIndex_++;
        return TAdvanceStageResult{.HasNextStage = CurrentStageIndex_ < StagePlan_.size()};
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) {
        const auto currentStage = StagePlan_[CurrentStageIndex_];
        switch (currentStage) {
            case ETaskType::LocalSort:
                return GetNewPartIdsForLocalSort(context);
            case ETaskType::SortedMerge:
                return GetNewPartIdsForSortedMerge(context);
            default:
                YQL_ENSURE(false, "Unknown stage type");
        }
        return TGetNewPartIdsForTaskResult{};
    }

private:
    TGetNewPartIdsForTaskResult GetNewPartIdsForLocalSort(const TGetNewPartIdsForTaskContext& context) {
        TGetNewPartIdsForTaskResult result;
        TLocalSortTaskParams& localSortTaskParams = std::get<TLocalSortTaskParams>(context.Task->TaskParams);
        TString tableId = localSortTaskParams.Output.TableId;
        TString newPartId = localSortTaskParams.Output.PartId;

        result.NewPartIdsForTables[tableId].emplace_back(newPartId);
        return result;
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForSortedMerge(const TGetNewPartIdsForTaskContext& context) {
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

    TGenerateTasksResult GenerateLocalSortTasksImpl(const TGenerateTasksContext& context)  {
        const auto& sortOperationParams = std::get<TSortOperationParams>(context.OperationParams);
        GeneratedTasks_.clear();

        ui64 maxAllowedTables = GetMaxAllowedTablesForMerge(context.FmrOperationSpec);

        if (context.PartitionResult.TaskInputs.size() > maxAllowedTables) {
            TFmrError error = {
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::FallbackOperation,
                .ErrorMessage = "Too many tables for sort operation to merge"
            };
            return TGenerateTasksResult{.Error = TMaybe<TFmrError>(std::move(error))};
        }

        std::vector<TGeneratedTaskInfo> generatedTasks;
        for (auto& task: context.PartitionResult.TaskInputs) {
            TLocalSortTaskParams localSortTaskParams;
            TFmrTableRef output = GenerateOutputTable(sortOperationParams.Output);

            localSortTaskParams.Input = task;
            localSortTaskParams.Output = TFmrTableOutputRef(output);
            localSortTaskParams.Output.PartId = GenerateId();

            GeneratedTasks_.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::LocalSort,
                .TaskParams = std::move(localSortTaskParams)
            });
        }

        YQL_CLOG(INFO, FastMapReduce) << "Starting Sort.LocalSort operation stage";

        return TGenerateTasksResult{.Tasks = GeneratedTasks_};
    }

    TGenerateTasksResult GenerateMergeSortTasksImpl(const TGenerateTasksContext& context)  {
        const auto& sortOperationParams = std::get<TSortOperationParams>(context.OperationParams);
        GeneratedTasks_.clear();
        TGenerateTasksResult result;

        for (auto& task: context.PartitionResult.TaskInputs) {
            TSortedMergeTaskParams sortedMergeTaskParams;
            TString newPartId = GenerateId();
            sortedMergeTaskParams.Input = task;
            sortedMergeTaskParams.Output = TFmrTableOutputRef(sortOperationParams.Output);
            TString tableId = sortedMergeTaskParams.Output.TableId;
            sortedMergeTaskParams.Output.PartId = newPartId;
            result.PartIdsToUpdate[tableId].emplace_back(newPartId);

            GeneratedTasks_.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::SortedMerge,
                .TaskParams = std::move(sortedMergeTaskParams)
            });
        }
        result.Tasks = GeneratedTasks_;

        YQL_CLOG(INFO, FastMapReduce) << "Starting Sort.SortedMerge operation stage";

        return result;
    }

    TPartitionResult DoSortedPartition(const TPrepareOperationStageContext& context) {
        const auto& sortOperationParams = std::get<TSortOperationParams>(context.OperationParams);

        std::vector<TOperationTableRef> inputTables;

        for (const auto& task: GeneratedTasks_) {
            const auto& prevStageParams = std::get<TLocalSortTaskParams>(task.TaskParams);

            TFmrTableRef input;
            input.FmrTableId = prevStageParams.Output.TableId;
            input.SerializedColumnGroups = sortOperationParams.Output.SerializedColumnGroups;
            input.Columns = sortOperationParams.Output.Columns;
            input.SortOrder = sortOperationParams.Output.SortOrder;
            input.SortColumns = sortOperationParams.Output.SortColumns;

            inputTables.push_back(input);
        }

        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;

        auto sortedPartitionerSettings = GetSortedPartitionerSettings(fmrOperationSpec);

        TSortingColumns sortingColumns;
        const auto& operationParams = std::get<TSortOperationParams>(context.OperationParams);
        sortingColumns.Columns = operationParams.Output.SortColumns;
        sortingColumns.SortOrders = operationParams.Output.SortOrder;

        auto sortedPartitioner = TSortedPartitioner(partIdsForTables, partIdStats, sortingColumns, sortedPartitionerSettings);

        return PartitionInputTablesIntoTasksSorted(inputTables, sortedPartitioner);
    }

    TPartitionResult DoUnorderedPartition(const TPrepareOperationStageContext& context) {
        const auto& operationParams = std::get<TSortOperationParams>(context.OperationParams);
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& clusterConnections = context.ClusterConnections;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;
        auto ytCoordinatorService = context.YtCoordinatorService;

        auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
        auto ytPartitionerSettings = GetYtPartitionerSettings(fmrOperationSpec);
        auto fmrPartitioner = TFmrPartitioner(partIdsForTables, partIdStats, fmrPartitionerSettings);

        std::vector<TFmrTableRef> fmrInputTables;
        std::vector<TYtTableRef> ytInputTables;

        for (auto& table: operationParams.Input) {
            if (auto ytTable = std::get_if<TYtTableRef>(&table)) {
                ytInputTables.emplace_back(*ytTable);
            } else {
                fmrInputTables.emplace_back(std::get<TFmrTableRef>(table));
            }
        }

        return PartitionInputTablesIntoTasks(ytInputTables, fmrInputTables, fmrPartitioner, ytCoordinatorService, clusterConnections, ytPartitionerSettings);
    }

    bool CompareBoundaries(const NYT::TNode& lhs, const NYT::TNode& rhs, const TSortingColumns& sortingColumns) {
        TFmrTableKeysBoundary lhsBoundary = MakeKeyBound(lhs, sortingColumns);
        TFmrTableKeysBoundary rhsBoundary = MakeKeyBound(rhs, sortingColumns);
        return lhsBoundary < rhsBoundary;
    }

    TFmrTableRef GenerateOutputTable(TFmrTableRef output) {
        TString randomSuffix = GenerateId();
        TString path = output.FmrTableId.Id.substr(0, output.FmrTableId.Id.find_last_of('/') + 1) + "stage/" + randomSuffix;
        TFmrTableRef res;
        res.FmrTableId.Id = path;
        res.Columns = output.Columns;
        res.SerializedColumnGroups = output.SerializedColumnGroups;
        res.SortOrder = output.SortOrder;
        res.SortColumns = output.SortColumns;

        YQL_CLOG(INFO,FastMapReduce) << "Stage operation Sort manager: Generated FMR output table: " << res.FmrTableId.Id;
        return std::move(res);
    }

    ui64 GetMaxAllowedTablesForMerge(const NYT::TNode& fmrOperationSpec) {
        return fmrOperationSpec["sort"]["max_merge_tables_allowed"].AsInt64();
    }

private:
    ui64 CurrentStageIndex_ = 0;
    static constexpr std::array<ETaskType, 2> StagePlan_ = {ETaskType::LocalSort, ETaskType::SortedMerge};
    std::vector<TGeneratedTaskInfo> GeneratedTasks_;
};

} // namespace

IFmrStageOperationManager::TPtr MakeSortStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TSortStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
