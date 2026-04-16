
#include "yql_yt_map_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TMapStageOperationManager: public TFmrStageOperationManagerBase {
public:
    TMapStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider)
        : TFmrStageOperationManagerBase(randomProvider)
    {
    }

    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) final {
        const auto& operationParams = std::get<TMapOperationParams>(context.OperationParams);
        const auto& fmrOperationSpec = context.FmrOperationSpec;
        const auto& clusterConnections = context.ClusterConnections;
        const auto& partIdsForTables = context.PartIdsForTables;
        const auto& partIdStats = context.PartIdStats;
        auto ytCoordinatorService = context.YtCoordinatorService;

        if (operationParams.IsOrdered) {
            // Ordered map -> ordered partition
            auto orderedPartitionerSettings = GetOrderedPartitionerSettings(fmrOperationSpec);
            auto orderedPartitioner = TOrderedPartitioner(partIdsForTables, partIdStats, orderedPartitionerSettings);

            std::vector<TOperationTableRef> inputTables = operationParams.Input;
            return PartitionInputTablesIntoTasksOrdered(inputTables, orderedPartitioner, ytCoordinatorService, clusterConnections);
        } else {
            // Unordered map -> unordered partition
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
    }

    TGenerateTasksResult GenerateTasksImpl(
        const TGenerateTasksContext& context
    ) override {
        const auto& mapOperationParams = std::get<TMapOperationParams>(context.OperationParams);

        if (mapOperationParams.IsOrdered) {
            YQL_CLOG(INFO, FastMapReduce) << "Starting Ordered Map operation";
        } else {
            YQL_CLOG(INFO, FastMapReduce) << "Starting Map operation";
        }

        TGenerateTasksResult result;
        std::vector<TGeneratedTaskInfo> generatedTasks;
        for (auto& task: context.PartitionResult.TaskInputs) {
            TMapTaskParams mapTaskParams;
            mapTaskParams.Input = task;
            std::vector<TFmrTableOutputRef> fmrTableOutputRefs;
            std::transform(mapOperationParams.Output.begin(), mapOperationParams.Output.end(), std::back_inserter(fmrTableOutputRefs), [] (const TFmrTableRef& fmrTableRef) {
                return TFmrTableOutputRef(fmrTableRef);
            });

            TString newPartId = GenerateId();
            for (auto& fmrTableOutputRef: fmrTableOutputRefs) {
                fmrTableOutputRef.PartId = newPartId;
                result.PartIdsToUpdate[fmrTableOutputRef.TableId].emplace_back(newPartId);
            }

            mapTaskParams.Output = fmrTableOutputRefs;
            mapTaskParams.SerializedMapJobState = mapOperationParams.SerializedMapJobState;
            mapTaskParams.IsOrdered = mapOperationParams.IsOrdered;

            generatedTasks.push_back(TGeneratedTaskInfo{
                .TaskType = ETaskType::Map,
                .TaskParams = std::move(mapTaskParams)
            });
        }

        result.Tasks = std::move(generatedTasks);
        return result;
    }

    TGetNewPartIdsForTaskResult GetNewPartIdsForTask(const TGetNewPartIdsForTaskContext& context) override {
        TGetNewPartIdsForTaskResult result;
        TMapTaskParams& mapTaskParams = std::get<TMapTaskParams>(context.Task->TaskParams);

        for (auto& fmrTableOutputRef: mapTaskParams.Output) {
            if (fmrTableOutputRef.PartId.empty()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Map task has empty output PartId",
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
                    .ErrorMessage = "Map task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }

            const auto& partIds = partIdsIter->second;
            if (std::find(partIds.begin(), partIds.end(), partId) == partIds.end()) {
                return {.Error = TFmrError{
                    .Component = EFmrComponent::Coordinator,
                    .Reason = EFmrErrorReason::RestartQuery,
                    .ErrorMessage = "Map task output PartId is missing in coordinator part list",
                    .TaskId = context.TaskId,
                    .OperationId = context.OperationId
                }};
            }
        }

        return result;
    }

    std::vector<TString> GetExpectedOutputTableIds(const TOperationParams& params) const override {
        const auto& mapParams = std::get<TMapOperationParams>(params);
        std::vector<TString> ids;
        for (const auto& output : mapParams.Output) {
            ids.emplace_back(output.FmrTableId.Id);
        }
        return ids;
    }

    std::vector<TPartIdInfo> GetPartIdsForTask(const GetPartIdsForTaskContext& context) override {
        std::vector<TPartIdInfo> groupsToClear;
        TMapTaskParams& mapTaskParams = std::get<TMapTaskParams>(context.Task->TaskParams);
        for (auto& fmrTableOutputRef: mapTaskParams.Output) {
            TString tableId = fmrTableOutputRef.TableId;
            if (!fmrTableOutputRef.PartId.empty() && context.PartIdStats.contains(fmrTableOutputRef.PartId)) {
                auto prevPartId = fmrTableOutputRef.PartId;
                groupsToClear.emplace_back(tableId, prevPartId);
            }
        }
        return groupsToClear;
    }
};



} // namespace

IFmrStageOperationManager::TPtr MakeMapStageOperationManager(TIntrusivePtr<IRandomProvider> randomProvider) {
    return MakeIntrusive<TMapStageOperationManager>(randomProvider);
}

} // namespace NYql::NFmr
