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

    struct RequestData {
        NKikimrKqp::TEvStartKqpTasksRequest request;
        ui32 flag;
        ui32 RetryNumber = 0;
        ui32 CurrentDelay = 0;
    };

public:
    TKqpPlanner(ui64 txId, const TActorId& executer, TVector<NYql::NDqProto::TDqTask>&& tasks,
        THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& scanTasks, const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
        const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages,
        bool enableLlvm, bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& ExecuterSpan,
        TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot, const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig);
    bool SendStartKqpTasksRequest(ui32 requestId, const TActorId& target);

    void Unsubscribe();

    void ProcessTasksForScanExecuter();
    void ProcessTasksForDataExecuter();

    ui64 GetComputeTasksNumber() const;
    ui64 GetMainTasksNumber() const;

    ui32 GetCurrentRetryDelay(ui32 requestId);
private:
    void PrepareToProcess();

    void RunLocal(const TVector<NKikimrKqp::TKqpNodeResources>& snapshot);

    void PrepareKqpNodeRequest(NKikimrKqp::TEvStartKqpTasksRequest& request, THashSet<ui64> taskIds);
    void AddScansToKqpNodeRequest(NKikimrKqp::TEvStartKqpTasksRequest& request, ui64 nodeId);
    void AddSnapshotInfoToTaskInputs(NYql::NDqProto::TDqTask& task);

    ui32 CalcSendMessageFlagsForNode(ui32 nodeId);

private:
    const ui64 TxId;
    const TActorId ExecuterId;
    TVector<NYql::NDqProto::TDqTask> ComputeTasks;
    THashMap<ui64, TVector<NYql::NDqProto::TDqTask>> MainTasksPerNode;
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
    TVector<RequestData> Requests;
};

std::unique_ptr<TKqpPlanner> CreateKqpPlanner(ui64 txId, const TActorId& executer, TVector<NYql::NDqProto::TDqTask>&& tasks,
    THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& scanTasks, const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages, bool enableLlvm,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TSpan& executerSpan,
    TVector<NKikimrKqp::TKqpNodeResources>&& resourcesSnapshot,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& ExecuterRetriesConfig);

} // namespace NKikimr::NKqp
