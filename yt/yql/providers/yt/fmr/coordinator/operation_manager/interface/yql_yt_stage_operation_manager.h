
#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>

namespace NYql::NFmr {

struct TGeneratedTaskInfo {
    ETaskType TaskType;
    TTaskParams TaskParams;
};

struct TGenerateTasksResult {
    std::vector<TGeneratedTaskInfo> Tasks;
    std::vector<TFmrResourceTaskInfo> FmrResourceTasks;
    std::unordered_map<TFmrTableId, std::vector<TString>> PartIdsToUpdate;
    TMaybe<TFmrError> Error;
};

struct TPrepareStageResult {
    TPartitionResult PartitionResult;
    TMaybe<TFmrError> Error;
};

struct TPrepareOperationStageContext {
    const TOperationParams& OperationParams;
    const NYT::TNode& FmrOperationSpec;
    const std::unordered_map<TFmrTableId, TClusterConnection>& ClusterConnections;
    const std::unordered_map<TFmrTableId, std::vector<TString>>& PartIdsForTables;
    const std::unordered_map<TString, std::vector<TChunkStats>>& PartIdStats;
    IYtCoordinatorService::TPtr YtCoordinatorService;
};

struct TGenerateTasksContext {
    const TPartitionResult& PartitionResult;
    const TOperationParams& OperationParams;
    const std::vector<TFmrResourceOperationInfo>& FmrResources;
    const NYT::TNode& FmrOperationSpec;
    const std::unordered_map<TFmrTableId, std::vector<TString>>& PartIdsForTables;
    const std::unordered_map<TString, std::vector<TChunkStats>>& PartIdStats;
};

struct TGetNewPartIdsForTaskContext {
    TTask::TPtr Task;
    const TString TaskId;
    const TString OperationId;
    const std::unordered_map<TFmrTableId, std::vector<TString>>& PartIdsForTables;
};

struct GetPartIdsForTaskContext {
    TTask::TPtr Task;
    const std::unordered_map<TString, std::vector<TChunkStats>>& PartIdStats;
};

struct TGetNewPartIdsForTaskResult {
    std::unordered_map<TFmrTableId, std::vector<TString>> NewPartIdsForTables;
    TMaybe<TFmrError> Error;
};

struct TAdvanceStageResult {
    bool HasNextStage = false;
    TMaybe<TFmrError> Error;
};

class IFmrStageOperationManager: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrStageOperationManager>;

    virtual ~IFmrStageOperationManager() = default;

    virtual TPrepareStageResult PrepareOperationStage(
        const TPrepareOperationStageContext& context
    ) = 0;

    virtual TGenerateTasksResult GenerateTasksForCurrentStage(
        const TGenerateTasksContext& context
    ) = 0;

    virtual void OnTaskCompleted(const TStatistics& /* stats */) {}

    virtual std::vector<TString> GetOperationResult() { return {}; }

    virtual TGetNewPartIdsForTaskResult GetNewPartIdsForTask(
        const TGetNewPartIdsForTaskContext& /* context */
    ) { return {}; }

    virtual std::vector<TPartIdInfo> GetPartIdsForTask(
        const GetPartIdsForTaskContext& context
    ) = 0;

    virtual TAdvanceStageResult AdvanceToNextStage() = 0;
};

} // namespace NYql::NFmr
