
#include "yql_yt_reduce_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TReduceStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TReduceStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
        const auto& operationParams = std::get<TReduceOperationParams>(context.OperationParams);
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;

        TSortingColumns reduceBy = operationParams.ReduceOperationSpec.ReduceBy;
        TSortingColumns sortBy = operationParams.ReduceOperationSpec.SortBy;


        auto reduceParitionSettings = GetReducePartitionSettings(fmrOperationSpec);
        auto reducePartitioner = TReducePartitioner(partIdsForTables, partIdStats, reduceBy, reduceParitionSettings);

        std::vector<TOperationTableRef> inputTables = operationParams.Input;

        return reducePartitioner.PartitionTablesIntoTasks(inputTables);
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        const auto& reduceOperationParams = std::get<TReduceOperationParams>(context.OperationParams);

        YQL_CLOG(INFO, FastMapReduce) << "Starting Reduce operation";

        TGenerateTasksResult result;
        std::vector<TGeneratedTaskInfo> generatedTasks;
        for (auto& task: context.PartitionResult.TaskInputs) {
            TReduceTaskParams reduceTaskParams;
            reduceTaskParams.Input = task;

            std::vector<TFmrTableOutputRef> fmrTableOutputRefs;
            std::transform(reduceOperationParams.Output.begin(), reduceOperationParams.Output.end(), std::back_inserter(fmrTableOutputRefs), [] (const TFmrTableRef& fmrTableRef) {
                return TFmrTableOutputRef(fmrTableRef);
            });

            TString newPartId = GenerateId();
            for (auto& fmrTableOutputRef: fmrTableOutputRefs) {
                fmrTableOutputRef.PartId = newPartId;
                result.PartIdsToUpdate[fmrTableOutputRef.TableId].emplace_back(newPartId);
            }

            reduceTaskParams.Output = fmrTableOutputRefs;
            reduceTaskParams.SerializedReduceJobState = reduceOperationParams.SerializedReduceJobState;
            reduceTaskParams.ReduceOperationSpec = reduceOperationParams.ReduceOperationSpec;

            generatedTasks.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::Reduce,
                .TaskParams = std::move(reduceTaskParams)
            });
        }
        result.Tasks = std::move(generatedTasks);

        return result;
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) final {
        TGetNewPartIdsForTaskResult result;
        TReduceTaskParams& reduceTaskParams = std::get<TReduceTaskParams>(context.Task->TaskParams);

        for (auto& fmrTableOutputRef: reduceTaskParams.Output) {
            if (fmrTableOutputRef.PartId.empty()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Reduce task has empty output PartId",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
            TString tableId = fmrTableOutputRef.TableId;
            TString partId = fmrTableOutputRef.PartId;

            const auto& partIdsIter = context.PartIdsForTables.find(tableId);
            if (partIdsIter == context.PartIdsForTables.end()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Reduce task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }

            const auto& partIds = partIdsIter->second;
            if (std::find(partIds.begin(), partIds.end(), partId) == partIds.end()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Reduce task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
        }

        return result;
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& context) final {
        std::vector<TPartIdInfo> groupsToClear;
        TReduceTaskParams& reduceTaskParams = std::get<TReduceTaskParams>(context.Task->TaskParams);
        for (auto& fmrTableOutputRef: reduceTaskParams.Output) {
            TString tableId = fmrTableOutputRef.TableId;
            if (!fmrTableOutputRef.PartId.empty() && context.PartIdStats.contains(fmrTableOutputRef.PartId)) {
                auto prevPartId = fmrTableOutputRef.PartId;
                groupsToClear.emplace_back(tableId, prevPartId);
            }
        }
        return groupsToClear;
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& params) const override {
        const auto& reduceParams = std::get<TReduceOperationParams>(params);
        std::vector<TString> ids;
        for (const auto& output : reduceParams.Output) {
            ids.emplace_back(output.FmrTableId.Id);
        }
        return ids;
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeReduceStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TReduceStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
