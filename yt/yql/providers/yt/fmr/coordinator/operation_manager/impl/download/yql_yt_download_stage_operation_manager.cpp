
#include "yql_yt_download_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TDownloadStageOperationManager: public TFmrStageOperationManagerBase {
public:
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

};

} // namespace

IFmrStageOperationManager::TPtr MakeDownloadStageOperationManager() {
    return MakeIntrusive<TDownloadStageOperationManager>();
}

} // namespace NYql::NFmr
