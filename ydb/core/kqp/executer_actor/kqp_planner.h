#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

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

public:
    TKqpPlanner(const TKqpTasksGraph& tasksGraph, ui64 txId, const TActorId& executer, TVector<ui64>&& tasks,
        THashMap<ui64, TVector<ui64>>&& scanTasks, const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
        const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages,
        bool enableLlvm, bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& ExecuterSpan,
        TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot, const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
        bool isDataQuery);

    bool SendStartKqpTasksRequest(ui32 requestId, const TActorId& target);
    std::unique_ptr<IEventHandle> PlanExecution();
    std::unique_ptr<IEventHandle> AssignTasksToNodes();
    void Submit();
    ui32 GetCurrentRetryDelay(ui32 requestId);

private:

    void PrepareToProcess();
    TString GetEstimationsInfo() const;

    std::unique_ptr<TEvKqpNode::TEvStartKqpTasksRequest> SerializeRequest(const TRequestData& requestData) const;
    ui32 CalcSendMessageFlagsForNode(ui32 nodeId);

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
    const bool DisableLlvmForUdfStages;
    const bool EnableLlvm;
    const bool WithSpilling;
    const TMaybe<NKikimrKqp::TRlPath> RlPath;
    THashSet<ui32> TrackingNodes;
    const TVector<NKikimrKqp::TKqpNodeResources> ResourcesSnapshot;
    NWilson::TSpan& ExecuterSpan;
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& ExecuterRetriesConfig;
    ui64 LocalRunMemoryEst;
    TVector<TTaskResourceEstimation> ResourceEstimations;
    TVector<TRequestData> Requests;
    const TKqpTasksGraph& TasksGraph;
    const bool IsDataQuery;
};

std::unique_ptr<TKqpPlanner> CreateKqpPlanner(const TKqpTasksGraph& tasksGraph, ui64 txId, const TActorId& executer, TVector<ui64>&& tasks,
    THashMap<ui64, TVector<ui64>>&& scanTasks, const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages, bool enableLlvm,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& executerSpan,
    TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& ExecuterRetriesConfig, bool isDataQuery);

} // namespace NKikimr::NKqp
