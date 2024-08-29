#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/vector.h>


namespace NKikimr::NKqp {

class TKqpPlanner {

    struct TRequestData {
        TVector<ui64> TaskIds;
        ui32 Flag;
        ui64 NodeId;
        ui32 RetryNumber = 0;
        ui32 CurrentDelay = 0;
        std::unique_ptr<TEvKqpNode::TEvStartKqpTasksRequest> SerializedRequest;

        explicit TRequestData(TVector<ui64>&& taskIds, ui64 flag, ui64 nodeId)
            : TaskIds(std::move(taskIds))
            , Flag(flag)
            , NodeId(nodeId)
        {}
    };

    using TLogFunc = std::function<void(TStringBuf message)>;

public:
    struct TArgs {
        TKqpTasksGraph& TasksGraph;
        const ui64 TxId;
        const ui64 LockTxId;
        const ui32 LockNodeId;
        const TActorId& Executer;
        const IKqpGateway::TKqpSnapshot& Snapshot;
        const TString& Database;
        const TIntrusiveConstPtr<NACLib::TUserToken>& UserToken;
        const TInstant Deadline;
        const Ydb::Table::QueryStatsCollection::Mode& StatsMode;
        const bool WithSpilling;
        const TMaybe<NKikimrKqp::TRlPath>& RlPath;
        NWilson::TSpan& ExecuterSpan;
        TVector<NKikimrKqp::TKqpNodeResources> ResourcesSnapshot;
        const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& ExecuterRetriesConfig;
        const bool UseDataQueryPool;
        const bool LocalComputeTasks;
        const ui64 MkqlMemoryLimit;
        const NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
        const bool AllowSinglePartitionOpt;
        const TIntrusivePtr<TUserRequestContext>& UserRequestContext;
        const std::optional<TKqpFederatedQuerySetup>& FederatedQuerySetup;
        const ui64 OutputChunkMaxSize = 0;
        const TGUCSettings::TPtr GUCSettings;
        const bool MayRunTasksLocally = false;
        const std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager>& ResourceManager_;
        const std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory>& CaFactory_;
    };

    TKqpPlanner(TKqpPlanner::TArgs&& args);
    bool SendStartKqpTasksRequest(ui32 requestId, const TActorId& target);
    std::unique_ptr<IEventHandle> PlanExecution();
    std::unique_ptr<IEventHandle> AssignTasksToNodes();
    bool AcknowledgeCA(ui64 taskId, TActorId computeActor, const NYql::NDqProto::TEvComputeActorState* state);
    void CompletedCA(ui64 taskId, TActorId computeActor);
    void TaskNotStarted(ui64 taskId);
    TProgressStat::TEntry CalculateConsumptionUpdate();
    void ShiftConsumption();
    void Submit();
    ui32 GetCurrentRetryDelay(ui32 requestId);
    void Unsubscribe();

    const THashMap<TActorId, TProgressStat>& GetPendingComputeActors();
    const THashSet<ui64>& GetPendingComputeTasks();

    ui32 GetnScanTasks();
    ui32 GetnComputeTasks();

private:

    const IKqpGateway::TKqpSnapshot& GetSnapshot() const;
    TString ExecuteDataComputeTask(ui64 taskId, ui32 computeTasksSize);
    void PrepareToProcess();
    TString GetEstimationsInfo() const;

    std::unique_ptr<TEvKqpNode::TEvStartKqpTasksRequest> SerializeRequest(const TRequestData& requestData);
    ui32 CalcSendMessageFlagsForNode(ui32 nodeId);

    void LogMemoryStatistics(const TLogFunc& logFunc);

private:
    const ui64 TxId;
    const ui64 LockTxId;
    const ui32 LockNodeId;
    const TActorId ExecuterId;
    TVector<ui64> ComputeTasks;
    THashMap<ui64, TVector<ui64>> TasksPerNode;
    const IKqpGateway::TKqpSnapshot Snapshot;
    TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TInstant Deadline;
    const Ydb::Table::QueryStatsCollection::Mode StatsMode;
    const bool WithSpilling;
    const TMaybe<NKikimrKqp::TRlPath> RlPath;
    THashSet<ui32> TrackingNodes;
    TVector<NKikimrKqp::TKqpNodeResources> ResourcesSnapshot;
    NWilson::TSpan& ExecuterSpan;
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& ExecuterRetriesConfig;
    ui64 LocalRunMemoryEst = 0;
    TVector<TTaskResourceEstimation> ResourceEstimations;
    TVector<TRequestData> Requests;
    TKqpTasksGraph& TasksGraph;
    const bool UseDataQueryPool;
    const bool LocalComputeTasks;
    ui64 MkqlMemoryLimit;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    ui32 nComputeTasks = 0;
    ui32 nScanTasks = 0;
    bool AllowSinglePartitionOpt;

    THashMap<TActorId, TProgressStat> PendingComputeActors; // Running compute actors (pure and DS)
    THashSet<ui64> PendingComputeTasks; // Not started yet, waiting resources

    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const ui64 OutputChunkMaxSize;
    const TGUCSettings::TPtr GUCSettings;
    const bool MayRunTasksLocally;
    TString SerializedGUCSettings;
    std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    TIntrusivePtr<NRm::TTxState> TxInfo;
    TVector<TProgressStat> LastStats;

public:
    static bool UseMockEmptyPlanner;  // for tests: if true then use TKqpMockEmptyPlanner that leads to the error
};

std::unique_ptr<TKqpPlanner> CreateKqpPlanner(TKqpPlanner::TArgs args);

} // namespace NKikimr::NKqp
