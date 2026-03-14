
#include "yql_yt_upload_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TUploadStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TUploadStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
        const auto& operationParams = std::get<TUploadOperationParams>(context.OperationParams);
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& clusterConnections = context.ClusterConnections;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;
        auto ytCoordinatorService = context.YtCoordinatorService;

        auto fmrPartitionerSettings = GetFmrPartitionerSettings(fmrOperationSpec);
        auto ytPartitionerSettings = GetYtPartitionerSettings(fmrOperationSpec);
        auto fmrPartitioner = TFmrPartitioner(partIdsForTables, partIdStats, fmrPartitionerSettings);

        std::vector<TFmrTableRef> fmrInputTables = {operationParams.Input};
        std::vector<TYtTableRef> ytInputTables;

        return PartitionInputTablesIntoTasks(ytInputTables, fmrInputTables, fmrPartitioner, ytCoordinatorService, clusterConnections, ytPartitionerSettings);
    }

    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) final {
        const auto& uploadOperationParams = std::get<TUploadOperationParams>(context.OperationParams);

        std::vector<TGeneratedTaskInfo> generatedTasks;
        for (auto& task: context.PartitionResult.TaskInputs) {
            TUploadTaskParams uploadTaskParams;
            YQL_ENSURE(task.Inputs.size() == 1, "Upload task should have exactly one fmr table partition input");
            auto& fmrTablePart = task.Inputs[0];
            uploadTaskParams.Input = std::get<TFmrTableInputRef>(fmrTablePart);
            uploadTaskParams.Output = uploadOperationParams.Output;

            generatedTasks.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::Upload,
                .TaskParams = std::move(uploadTaskParams)
            });
        }

        return TGenerateTasksResult{.Tasks = std::move(generatedTasks)};
    }
};

} // namespace

IFmrStageOperationManager::TPtr MakeUploadStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TUploadStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
