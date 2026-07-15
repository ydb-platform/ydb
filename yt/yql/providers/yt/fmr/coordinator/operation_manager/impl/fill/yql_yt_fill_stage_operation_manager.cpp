
#include "yql_yt_fill_stage_operation_manager.h"

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TFillStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TFillStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& /* context */) final {
        // Fill has no input tables — always produces exactly one task.
        TPartitionResult result;
        result.TaskInputs = {TTaskTableInputRef{}};
        return result;
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        const auto& fillOperationParams = std::get<TFillOperationParams>(context.OperationParams);

        YQL_CLOG(INFO, FastMapReduce) << "Starting Fill operation";

        TGenerateTasksResult result;
        std::vector<TGeneratedTaskInfo> generatedTasks;

        YQL_ENSURE(context.PartitionResult.TaskInputs.size() == 1, "Fill must produce exactly one task");

        TFillTaskParams fillTaskParams;
        std::vector<TFmrTableOutputRef> fmrTableOutputRefs;
        std::transform(fillOperationParams.Output.begin(), fillOperationParams.Output.end(), std::back_inserter(fmrTableOutputRefs), [](const TFmrTableRef& fmrTableRef) {
            return TFmrTableOutputRef(fmrTableRef);
        });

        TString newPartId = GenerateId();
        for (auto& fmrTableOutputRef: fmrTableOutputRefs) {
            fmrTableOutputRef.PartId = newPartId;
            result.PartIdsToUpdate[fmrTableOutputRef.TableId].emplace_back(newPartId);
        }

        fillTaskParams.Output = fmrTableOutputRefs;
        fillTaskParams.SerializedFillJobState = fillOperationParams.SerializedFillJobState;

        generatedTasks.push_back(TGeneratedTaskInfo{
            .TaskType = ETaskType::Fill,
            .TaskParams = std::move(fillTaskParams)
        });

        result.Tasks = std::move(generatedTasks);
        return result;
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) override {
        TGetNewPartIdsForTaskResult result;
        const TFillTaskParams& fillTaskParams = std::get<TFillTaskParams>(context.Task->TaskParams);

        for (const auto& fmrTableOutputRef: fillTaskParams.Output) {
            if (fmrTableOutputRef.PartId.empty()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Fill task has empty output PartId",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
            const auto& partIdsIter = context.PartIdsForTables.find(fmrTableOutputRef.TableId);
            if (partIdsIter == context.PartIdsForTables.end()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Fill task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
            const auto& partIds = partIdsIter->second;
            if (std::find(partIds.begin(), partIds.end(), fmrTableOutputRef.PartId) == partIds.end()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Fill task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
        }

        return result;
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& params) const override {
        const auto& fillParams = std::get<TFillOperationParams>(params);
        std::vector<TString> ids;
        for (const auto& output : fillParams.Output) {
            ids.emplace_back(output.FmrTableId.Id);
        }
        return ids;
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& context) override {
        std::vector<TPartIdInfo> groupsToClear;
        const TFillTaskParams& fillTaskParams = std::get<TFillTaskParams>(context.Task->TaskParams);
        for (const auto& fmrTableOutputRef: fillTaskParams.Output) {
            if (!fmrTableOutputRef.PartId.empty() && context.PartIdStats.contains(fmrTableOutputRef.PartId)) {
                groupsToClear.emplace_back(fmrTableOutputRef.TableId, fmrTableOutputRef.PartId);
            }
        }
        return groupsToClear;
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeFillStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TFillStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
