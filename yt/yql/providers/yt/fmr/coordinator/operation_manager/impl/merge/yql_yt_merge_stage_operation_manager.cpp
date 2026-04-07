
#include "yql_yt_merge_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TMergeStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TMergeStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
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

    TGenerateTasksResult GenerateTasksImpl (const TGenerateTasksContext& context) final {
        const auto& mergeOperationParams = std::get<TMergeOperationParams>(context.OperationParams);

        YQL_CLOG(INFO, FastMapReduce) << "Starting Merge operation";

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

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) override {
        TGetNewPartIdsForTaskResult result;
        TMergeTaskParams& mergeTaskParams = std::get<TMergeTaskParams>(context.Task->TaskParams);
        TString tableId = mergeTaskParams.Output.TableId;
        TString newPartId = GenerateId();

        mergeTaskParams.Output.PartId = newPartId;
        result.NewPartIdsForTables[tableId].emplace_back(newPartId);
        return result;
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& context) override {
        std::vector<TPartIdInfo> groupsToClear;
        TMergeTaskParams& mergeTaskParams = std::get<TMergeTaskParams>(context.Task->TaskParams);
        TString tableId = mergeTaskParams.Output.TableId;
        if (!mergeTaskParams.Output.PartId.empty() && context.PartIdStats.contains(mergeTaskParams.Output.PartId)) {
            auto prevPartId = mergeTaskParams.Output.PartId;
            groupsToClear.emplace_back(tableId, prevPartId);
        }
        return groupsToClear;
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeMergeStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TMergeStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
