
#include "yql_yt_map_reduce_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TMapReduceStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TMapReduceStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) override {
        const auto currentStage = StagePlan_[CurrentStageIndex_];
        switch (currentStage) {
            case ETaskType::MapReduceMap:
                return DoMapPartition(context);
            case ETaskType::Reduce:
                return DoReducePartition(context);
            default:
                YQL_ENSURE(false, "Unknown MapReduce stage type");
        }
        return TPartitionResult{};
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) override {
        const auto currentStage = StagePlan_[CurrentStageIndex_];
        switch (currentStage) {
            case ETaskType::MapReduceMap:
                return GenerateMapTasksImpl(context);
            case ETaskType::Reduce:
                return GenerateReduceTasksImpl(context);
            default:
                YQL_ENSURE(false, "Unknown MapReduce stage type");
        }
        return TGenerateTasksResult{};
    }

    TAdvanceStageResult AdvanceToNextStage() final {
        CurrentStageIndex_++;
        return TAdvanceStageResult{.HasNextStage = CurrentStageIndex_ < StagePlan_.size()};
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) override {
        const auto currentStage = StagePlan_[CurrentStageIndex_];
        switch (currentStage) {
            case ETaskType::MapReduceMap:
                return GetNewPartIdsForMapReduceMap(context);
            case ETaskType::Reduce:
                return GetNewPartIdsForReduce(context);
            default:
                YQL_ENSURE(false, "Unknown MapReduce stage type");
        }
        return TGetNewPartIdsForTaskResult{};
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& params) const override {
        const auto& mapReduceParams = std::get<TMapReduceOperationParams>(params);
        std::vector<TString> ids;
        // Direct (map-bypass) outputs come first, matching the execCtx->OutTables_ convention
        // ([0..K) direct outputs, [K..N) reduce outputs) the gateway uses to split them.
        for (const auto& directOutput : mapReduceParams.DirectMapOutput) {
            ids.emplace_back(directOutput.FmrTableId.Id);
        }
        for (const auto& output : mapReduceParams.Output) {
            ids.emplace_back(output.FmrTableId.Id);
        }
        return ids;
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& context) override {
        std::vector<TPartIdInfo> groupsToClear;
        if (auto* mapTaskParams = std::get_if<TMapReduceMapTaskParams>(&context.Task->TaskParams)) {
            TString tableId = mapTaskParams->Output.TableId;
            if (!mapTaskParams->Output.PartId.empty() && context.PartIdStats.contains(mapTaskParams->Output.PartId)) {
                groupsToClear.emplace_back(tableId, mapTaskParams->Output.PartId);
            }
            for (auto& directOutputRef : mapTaskParams->DirectOutputs) {
                TString directTableId = directOutputRef.TableId;
                if (!directOutputRef.PartId.empty() && context.PartIdStats.contains(directOutputRef.PartId)) {
                    groupsToClear.emplace_back(directTableId, directOutputRef.PartId);
                }
            }
        } else if (auto* reduceTaskParams = std::get_if<TReduceTaskParams>(&context.Task->TaskParams)) {
            for (auto& fmrTableOutputRef : reduceTaskParams->Output) {
                TString tableId = fmrTableOutputRef.TableId;
                if (!fmrTableOutputRef.PartId.empty() && context.PartIdStats.contains(fmrTableOutputRef.PartId)) {
                    groupsToClear.emplace_back(tableId, fmrTableOutputRef.PartId);
                }
            }
        }
        return groupsToClear;
    }

private:
    // Generates a unique intermediate table path derived from the first final output table.
    TFmrTableRef GenerateIntermediateTable(const TMapReduceOperationParams& params) {
        YQL_ENSURE(!params.Output.empty(), "MapReduce operation must have at least one output table");
        const auto& firstOutput = params.Output[0];
        TString prefix = firstOutput.FmrTableId.Id.substr(
            0, firstOutput.FmrTableId.Id.find_last_of('/') + 1
        );
        TString path = prefix + "map_reduce_stage/" + GenerateId();

        // Always sort the intermediate table by [_yql_key_hash, ...sortBy]. sortBy (not reduceBy)
        // is required here: for joins it carries an extra tiebreaker column (e.g. "_yql_sort")
        // after the reduce-by columns that the reducer's compiled lambda relies on to order rows
        // correctly within a group (e.g. CommonJoinCore's build-then-probe side ordering) - using
        // reduceBy alone would sort rows into the right groups but leave their relative order
        // within a group unspecified. The gateway (yql_yt_fmr.cpp) already builds
        // ReduceOperationSpec.SortBy as MakeMapReduceIntermediateSortColumns(sortBy), i.e. with the
        // _yql_key_hash prefix already applied, so it's used as-is here rather than re-derived.
        // An empty SortBy means the caller didn't request extra tiebreaker columns beyond reduceBy
        // (mirrors the gateway's own "sortBy.empty() -> sortBy = reduceBy" fallback), so fall back
        // to the reduceBy-derived columns in that case.
        const auto intermediateSortColumns = params.ReduceOperationSpec.SortBy.Columns.empty()
            ? MakeMapReduceIntermediateSortColumns(params.ReduceOperationSpec.ReduceBy)
            : params.ReduceOperationSpec.SortBy;

        TFmrTableRef res;
        res.FmrTableId.Id = path;
        res.SortColumns = intermediateSortColumns.Columns;
        res.SortOrder = intermediateSortColumns.SortOrders;

        YQL_CLOG(INFO, FastMapReduce) << "MapReduce: generated intermediate table: " << res.FmrTableId.Id;
        return res;
    }

    // Stage 1: partition input like a regular unordered Map.
    TPartitionResult DoMapPartition(const TPrepareOperationStageContext& context) {
        const auto& operationParams = std::get<TMapReduceOperationParams>(context.OperationParams);
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
        for (const auto& table : operationParams.Input) {
            if (auto* ytTable = std::get_if<TYtTableRef>(&table)) {
                ytInputTables.emplace_back(*ytTable);
            } else {
                fmrInputTables.emplace_back(std::get<TFmrTableRef>(table));
            }
        }

        return PartitionInputTablesIntoTasks(
            ytInputTables, fmrInputTables, fmrPartitioner,
            ytCoordinatorService, clusterConnections, ytPartitionerSettings
        );
    }

    // Stage 2: partition the intermediate tables by _yql_key_hash + reduce-by columns.
    TPartitionResult DoReducePartition(const TPrepareOperationStageContext& context) {
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;

        // Collect intermediate FMR tables produced by all stage-1 map tasks.
        // Sort columns were determined at generate-time (identity mapper → [_yql_key_hash, ...reduceBy];
        // real mapper → [...reduceBy]) and are stored in each task's output ref.
        YQL_ENSURE(!GeneratedMapTasks_.empty());
        const auto& firstOutputSortingColumns =
            std::get<TMapReduceMapTaskParams>(GeneratedMapTasks_[0].TaskParams).Output.SortingColumns;

        std::vector<TOperationTableRef> inputTables;
        for (const auto& task : GeneratedMapTasks_) {
            const auto& mapTaskParams = std::get<TMapReduceMapTaskParams>(task.TaskParams);
            TFmrTableRef input;
            input.FmrTableId = mapTaskParams.Output.TableId;
            input.SortColumns = mapTaskParams.Output.SortingColumns.Columns;
            input.SortOrder = mapTaskParams.Output.SortingColumns.SortOrders;
            inputTables.push_back(input);
        }

        auto reducePartitionSettings = GetReducePartitionSettings(fmrOperationSpec);
        auto reducePartitioner = TReducePartitioner(
            partIdsForTables, partIdStats, firstOutputSortingColumns, reducePartitionSettings
        );

        return reducePartitioner.PartitionTablesIntoTasks(inputTables);
    }

    TGenerateTasksResult GenerateMapTasksImpl(const TGenerateTasksContext& context) {
        const auto& operationParams = std::get<TMapReduceOperationParams>(context.OperationParams);
        GeneratedMapTasks_.clear();

        YQL_CLOG(INFO, FastMapReduce) << "Starting MapReduce.Map stage";

        TGenerateTasksResult result;
        for (const auto& taskInput : context.PartitionResult.TaskInputs) {
            TMapReduceMapTaskParams mapTaskParams;
            mapTaskParams.Input = taskInput;
            mapTaskParams.SerializedMapJobState = operationParams.SerializedMapJobState;
            mapTaskParams.ReduceOperationSpec = operationParams.ReduceOperationSpec;

            TFmrTableRef intermediateTable = GenerateIntermediateTable(operationParams);
            mapTaskParams.Output = TFmrTableOutputRef(intermediateTable);
            TString newPartId = GenerateId();
            mapTaskParams.Output.PartId = newPartId;
            result.PartIdsToUpdate[mapTaskParams.Output.TableId].emplace_back(newPartId);

            // Direct (map-bypass) outputs are written by the same job run as the intermediate
            // table, but each output is still a DISTINCT physical table and must get its own
            // PartId: the coordinator's PartIdStats_ is keyed by bare PartId (not (TableId,
            // PartId)), so reusing the intermediate table's PartId here would let a direct
            // output's (unsorted) chunk stats silently clobber the intermediate table's (sorted)
            // ones whenever they're reported in the same worker heartbeat.
            std::vector<TFmrTableOutputRef> directOutputRefs;
            std::transform(
                operationParams.DirectMapOutput.begin(), operationParams.DirectMapOutput.end(),
                std::back_inserter(directOutputRefs),
                [](const TFmrTableRef& ref) { return TFmrTableOutputRef(ref); }
            );
            for (auto& directOutputRef : directOutputRefs) {
                TString directPartId = GenerateId();
                directOutputRef.PartId = directPartId;
                result.PartIdsToUpdate[directOutputRef.TableId].emplace_back(directPartId);
            }
            mapTaskParams.DirectOutputs = std::move(directOutputRefs);

            YQL_CLOG(INFO, FastMapReduce) << "MapReduce.Map task: intermediate table="
                << mapTaskParams.Output.TableId << " partId=" << newPartId;

            GeneratedMapTasks_.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::MapReduceMap,
                .TaskParams = std::move(mapTaskParams)
            });
        }

        result.Tasks = GeneratedMapTasks_;
        return result;
    }

    TGenerateTasksResult GenerateReduceTasksImpl(const TGenerateTasksContext& context) {
        const auto& operationParams = std::get<TMapReduceOperationParams>(context.OperationParams);

        YQL_CLOG(INFO, FastMapReduce) << "Starting MapReduce.Reduce stage";

        TGenerateTasksResult result;
        std::vector<TGeneratedTaskInfo> generatedTasks;
        for (const auto& taskInput : context.PartitionResult.TaskInputs) {
            TReduceTaskParams reduceTaskParams;
            reduceTaskParams.Input = taskInput;
            reduceTaskParams.SerializedReduceJobState = operationParams.SerializedReduceJobState;
            reduceTaskParams.ReduceOperationSpec = operationParams.ReduceOperationSpec;

            std::vector<TFmrTableOutputRef> outputRefs;
            std::transform(
                operationParams.Output.begin(), operationParams.Output.end(),
                std::back_inserter(outputRefs),
                [](const TFmrTableRef& ref) { return TFmrTableOutputRef(ref); }
            );

            TString newPartId = GenerateId();
            for (auto& outputRef : outputRefs) {
                outputRef.PartId = newPartId;
                result.PartIdsToUpdate[outputRef.TableId].emplace_back(newPartId);
            }

            reduceTaskParams.Output = std::move(outputRefs);

            YQL_CLOG(INFO, FastMapReduce) << "MapReduce.Reduce task: partId=" << newPartId;

            generatedTasks.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::Reduce,
                .TaskParams = std::move(reduceTaskParams)
            });
        }

        result.Tasks = std::move(generatedTasks);
        return result;
    }

    TMaybe<TFmrError> ValidateMapReduceMapOutputRef(
        const TFmrTableOutputRef& outputRef,
        const TGetNewPartIdsForTaskContext& context
    ) {
        if (outputRef.PartId.empty()) {
            return TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "MapReduceMap task has empty output PartId",
                .TaskId = context.TaskId,
                .OperationId = context.OperationId
            };
        }

        TString tableId = outputRef.TableId;
        TString partId = outputRef.PartId;

        const auto partIdsIter = context.PartIdsForTables.find(tableId);
        if (partIdsIter == context.PartIdsForTables.end()) {
            return TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "MapReduceMap task output PartId is missing in coordinator part list",
                .TaskId = context.TaskId,
                .OperationId = context.OperationId
            };
        }

        const auto& partIds = partIdsIter->second;
        if (std::find(partIds.begin(), partIds.end(), partId) == partIds.end()) {
            return TFmrError{
                .Component = EFmrComponent::Coordinator,
                .Reason = EFmrErrorReason::RestartQuery,
                .ErrorMessage = "MapReduceMap task output PartId is missing in coordinator part list",
                .TaskId = context.TaskId,
                .OperationId = context.OperationId
            };
        }

        return Nothing();
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForMapReduceMap(const TGetNewPartIdsForTaskContext& context) {
        TMapReduceMapTaskParams& mapTaskParams = std::get<TMapReduceMapTaskParams>(context.Task->TaskParams);

        if (auto error = ValidateMapReduceMapOutputRef(mapTaskParams.Output, context)) {
            return {.Error = error};
        }

        for (auto& directOutputRef : mapTaskParams.DirectOutputs) {
            if (auto error = ValidateMapReduceMapOutputRef(directOutputRef, context)) {
                return {.Error = error};
            }
        }

        return TGetNewPartIdsForTaskResult{};
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForReduce(const TGetNewPartIdsForTaskContext& context) {
        TGetNewPartIdsForTaskResult result;
        TReduceTaskParams& reduceTaskParams = std::get<TReduceTaskParams>(context.Task->TaskParams);

        for (auto& outputRef : reduceTaskParams.Output) {
            if (outputRef.PartId.empty()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "MapReduce.Reduce task has empty output PartId",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }

            TString tableId = outputRef.TableId;
            TString partId = outputRef.PartId;

            const auto partIdsIter = context.PartIdsForTables.find(tableId);
            if (partIdsIter == context.PartIdsForTables.end()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "MapReduce.Reduce task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }

            const auto& partIds = partIdsIter->second;
            if (std::find(partIds.begin(), partIds.end(), partId) == partIds.end()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "MapReduce.Reduce task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
        }

        return result;
    }

private:
    ui64 CurrentStageIndex_ = 0;
    static constexpr std::array<ETaskType, 2> StagePlan_ = {ETaskType::MapReduceMap, ETaskType::Reduce};
    std::vector<TGeneratedTaskInfo> GeneratedMapTasks_;
};

} // namespace

IFmrStageOperationManager::TPtr MakeMapReduceStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TMapReduceStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
