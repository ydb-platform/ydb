
#include "yql_yt_download_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TDownloadStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TDownloadStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
        const auto& operationParams = std::get<TDownloadOperationParams>(context.OperationParams);
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& clusterConnections = context.ClusterConnections;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;
        auto ytCoordinatorService = context.YtCoordinatorService;

        auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
        auto ytPartitionerSettings = GetYtPartitionerSettings(fmrOperationSpec);
        auto fmrPartitioner = TFmrPartitioner(partIdsForTables, partIdStats, fmrPartitionerSettings);

        std::vector<TYtTableRef> ytInputTables = {operationParams.Input};
        std::vector<TFmrTableRef> fmrInputTables;

        return PartitionInputTablesIntoTasks(ytInputTables, fmrInputTables, fmrPartitioner, ytCoordinatorService, clusterConnections, ytPartitionerSettings);
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        const auto& downloadOperationParams = std::get<TDownloadOperationParams>(context.OperationParams);

        YQL_CLOG(INFO, FastMapReduce) << "Starting Download operation";

        std::vector<TGeneratedTaskInfo> generatedTasks;
        for (auto& task: context.PartitionResult.TaskInputs) {
            TDownloadTaskParams downloadTaskParams;
            YQL_ENSURE(task.Inputs.size() == 1, "Download task should have exactly one yt table partition input");
            auto& ytTablePart = task.Inputs[0];
            downloadTaskParams.Input = std::get<TYtTableTaskRef>(ytTablePart);
            downloadTaskParams.Output = TFmrTableOutputRef(downloadOperationParams.Output);
            // PartId for tasks which write to table data service will be set later

            generatedTasks.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::Download,
                .TaskParams = std::move(downloadTaskParams)
            });
        }

        return TGenerateTasksResult{.Tasks = std::move(generatedTasks)};
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) override {
        TGetNewPartIdsForTaskResult result;
        TDownloadTaskParams& downloadTaskParams = std::get<TDownloadTaskParams>(context.Task->TaskParams);
        TString tableId = downloadTaskParams.Output.TableId;
        TString newPartId = GenerateId();

        downloadTaskParams.Output.PartId = newPartId;
        result.NewPartIdsForTables[tableId].emplace_back(newPartId);
        return result;
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& params) const override {
        const auto& downloadParams = std::get<TDownloadOperationParams>(params);
        return {downloadParams.Output.FmrTableId.Id};
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& context) override {
        std::vector<TPartIdInfo> groupsToClear;
        TDownloadTaskParams& downloadTaskParams = std::get<TDownloadTaskParams>(context.Task->TaskParams);
        TString tableId = downloadTaskParams.Output.TableId;
        if (!downloadTaskParams.Output.PartId.empty() && context.PartIdStats.contains(downloadTaskParams.Output.PartId)) {
            auto prevPartId = downloadTaskParams.Output.PartId;
            groupsToClear.emplace_back(tableId, prevPartId);
        }
        return groupsToClear;
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeDownloadStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TDownloadStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
