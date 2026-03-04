
#pragma once

#include <functional>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>

namespace NYql::NFmr {

struct TGeneratedTaskInfo {
    ETaskType TaskType;
    TTaskParams TaskParams;
};

struct TGenerateTasksResult {
    std::vector<TGeneratedTaskInfo> Tasks;
    std::vector<TFmrResourceTaskInfo> FmrResourceTasks;
    TMaybe<TFmrError> Error;
};

struct TPrepareStageResult {
    TPartitionResult PartitionResult;
    TMaybe<TFmrError> Error;
};

struct TPrepareOperationStageContext {
    TOperationParams OperationParams;
    NYT::TNode FmrOperationSpec;
    std::unordered_map<TFmrTableId, TClusterConnection> ClusterConnections;
    const std::unordered_map<TFmrTableId, std::vector<TString>>& PartIdsForTables;
    const std::unordered_map<TString, std::vector<TChunkStats>>& PartIdStats;
    IYtCoordinatorService::TPtr YtCoordinatorService;
};

struct TGenerateTasksContext {
    TPartitionResult PartitionResult;
    TOperationParams OperationParams;
    std::vector<TFmrResourceOperationInfo> FmrResources;
    NYT::TNode FmrOperationSpec;
    std::unordered_map<TFmrTableId, std::vector<TString>>& PartIdsForTables;
    const std::unordered_map<TString, std::vector<TChunkStats>>& PartIdStats;
    std::function<TString()> GenerateId;
};

struct TAdvanceStageContext {
    TString OperationId;
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

    virtual TAdvanceStageResult AdvanceToNextStage(const TAdvanceStageContext& context) = 0;
};

}
