
#include "yql_yt_pull_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/base/yql_yt_base_stage_operation_manager.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TPullStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TPullStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    // Pull always maps all inputs into exactly one task.
    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
        const auto& operationParams = std::get<TPullOperationParams>(context.OperationParams);
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;

        TTaskTableInputRef singleTask;
        for (const auto& inputRef : operationParams.Input) {
            if (const auto* fmrTableRef = std::get_if<TFmrTableRef>(&inputRef)) {
                const TFmrTableId& tableId = fmrTableRef->FmrTableId;
                auto partIdsIt = partIdsForTables.find(tableId);
                YQL_ENSURE(partIdsIt != partIdsForTables.end(), "Pull: no part ids for table " << tableId);

                TFmrTableInputRef tableInputRef;
                tableInputRef.TableId = tableId.Id;
                tableInputRef.Columns = fmrTableRef->Columns;
                tableInputRef.SerializedColumnGroups = fmrTableRef->SerializedColumnGroups;
                for (const auto& partId : partIdsIt->second) {
                    ui64 numChunks = 0;
                    auto statsIt = partIdStats.find(partId);
                    if (statsIt != partIdStats.end()) {
                        numChunks = statsIt->second.size();
                    }
                    tableInputRef.TableRanges.push_back(TTableRange{.PartId = partId, .MinChunk = 0, .MaxChunk = numChunks});
                }
                singleTask.Inputs.emplace_back(std::move(tableInputRef));
            } else {
                ythrow yexception() << "Pull operation does not support YT table inputs";
            }
        }

        return TPartitionResult{.TaskInputs = {std::move(singleTask)}};
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        YQL_CLOG(INFO, FastMapReduce) << "Starting Pull operation";

        YQL_ENSURE(context.PartitionResult.TaskInputs.size() == 1, "Pull must produce exactly one task");

        TPullTaskParams pullTaskParams;
        pullTaskParams.Input = context.PartitionResult.TaskInputs[0];

        return TGenerateTasksResult{.Tasks = {{.TaskType = ETaskType::Pull, .TaskParams = std::move(pullTaskParams)}}};
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& /* params */) const override {
        return {}; // Pull returns data directly, no FMR output tables
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& /* context */) override {
        return {}; // Pull reads only, nothing to clean in TDS
    }

    void OnTaskCompleted(const TStatistics& stats) override {
        if (const auto* pullResult = std::get_if<TTaskPullResult>(&stats.TaskResult)) {
            PullData_ = pullResult->Data;
        }
    }

    std::vector<TString> GetOperationResult() override {
        return {std::move(PullData_)};
    }

private:
    TString PullData_;
};

} // namespace

IFmrStageOperationManager::TPtr MakePullStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TPullStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
