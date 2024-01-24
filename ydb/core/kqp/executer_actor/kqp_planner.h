#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
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
    TKqpPlanner(TKqpTasksGraph& tasksGraph, ui64 txId, const TActorId& executer, const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
        const Ydb::Table::QueryStatsCollection::Mode& statsMode,
        bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& ExecuterSpan,
        TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot, const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
        bool useDataQueryPool, bool localComputeTasks, ui64 mkqlMemoryLimit, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, bool allowSinglePartitionOpt,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext);

    bool SendStartKqpTasksRequest(ui32 requestId, const TActorId& target);
    std::unique_ptr<IEventHandle> PlanExecution();
    std::unique_ptr<IEventHandle> AssignTasksToNodes();
    void Submit();
    ui32 GetCurrentRetryDelay(ui32 requestId);
    void Unsubscribe();

    THashMap<TActorId, TProgressStat>& GetPendingComputeActors();
    THashSet<ui64>& GetPendingComputeTasks();

    ui32 GetnScanTasks();
    ui32 GetnComputeTasks();

private:
    
    const IKqpGateway::TKqpSnapshot& GetSnapshot() const;
    void ExecuteDataComputeTask(ui64 taskId, bool shareMailbox, bool optimizeProtoForLocalExecution);
    void PrepareToProcess();
    TString GetEstimationsInfo() const;

    std::unique_ptr<TEvKqpNode::TEvStartKqpTasksRequest> SerializeRequest(const TRequestData& requestData);
    ui32 CalcSendMessageFlagsForNode(ui32 nodeId);

    void LogMemoryStatistics(const TLogFunc& logFunc);

private:
    const ui64 TxId;
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
    const TVector<NKikimrKqp::TKqpNodeResources> ResourcesSnapshot;
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

public:
    static bool UseMockEmptyPlanner;  // for tests: if true then use TKqpMockEmptyPlanner that leads to the error
};

std::unique_ptr<TKqpPlanner> CreateKqpPlanner(TKqpTasksGraph& tasksGraph, ui64 txId, const TActorId& executer,
    const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& executerSpan,
    TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& ExecuterRetriesConfig,
    bool useDataQueryPool, bool localComputeTasks,
    ui64 mkqlMemoryLimit, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, bool doOptimization,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext);

} // namespace NKikimr::NKqp
