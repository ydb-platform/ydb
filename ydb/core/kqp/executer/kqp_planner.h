#pragma once

#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/kqp.h>
#include <ydb/core/kqp/node/kqp_node.h>
#include <ydb/core/kqp/rm/kqp_rm.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/vector.h>


namespace NKikimr::NKqp {

class TKqpPlanner : public TActorBootstrapped<TKqpPlanner> {
    using TBase = TActorBootstrapped<TKqpPlanner>;

    struct TEvPrivate {
        enum EEv {
            EvResourcesSnapshot = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TEvResourcesSnapshot : public TEventLocal<TEvResourcesSnapshot, EEv::EvResourcesSnapshot> {
            TVector<NKikimrKqp::TKqpNodeResources> Snapshot;

            TEvResourcesSnapshot(TVector<NKikimrKqp::TKqpNodeResources>&& snapshot)
                : Snapshot(std::move(snapshot)) {}
        };
    };

public:
    TKqpPlanner(ui64 txId, const TActorId& executer, TVector<NYql::NDqProto::TDqTask>&& tasks,
        THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& scanTasks, const IKqpGateway::TKqpSnapshot& snapshot,
        const TString& database, const TMaybe<TString>& userToken, TInstant deadline,
        const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages,
        bool enableLlvm, bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TTraceId traceId);

    void Bootstrap(const TActorContext& ctx);

private:
    STATEFN(WaitState);

    void HandleWait(TEvPrivate::TEvResourcesSnapshot::TPtr& ev);
    void HandleWait(TEvKqp::TEvAbortExecution::TPtr& ev);

    void Process(const TVector<NKikimrKqp::TKqpNodeResources>& snapshot);
    void RunLocal(const TVector<NKikimrKqp::TKqpNodeResources>& snapshot);

    void PassAway() override;

    THolder<TEvKqpNode::TEvStartKqpTasksRequest> PrepareKqpNodeRequest(const TVector<ui64>& taskIds);
    void AddScansToKqpNodeRequest(THolder<TEvKqpNode::TEvStartKqpTasksRequest>& ev, ui64 nodeId);
    void AddSnapshotInfoToTaskInputs(NYql::NDqProto::TDqTask& task);

    ui32 CalcSendMessageFlagsForNode(ui32 nodeId);

private:
    const ui64 TxId;
    const TActorId ExecuterId;
    TVector<NYql::NDqProto::TDqTask> Tasks;
    THashMap<ui64, TVector<NYql::NDqProto::TDqTask>> ScanTasks;
    const IKqpGateway::TKqpSnapshot Snapshot;
    TString Database;
    const TMaybe<TString> UserToken;
    const TInstant Deadline;
    const Ydb::Table::QueryStatsCollection::Mode StatsMode;
    const bool DisableLlvmForUdfStages;
    const bool EnableLlvm;
    const bool WithSpilling;
    const TMaybe<NKikimrKqp::TRlPath> RlPath;
    THashSet<ui32> TrackingNodes;
    NWilson::TSpan KqpPlannerSpan;
};

IActor* CreateKqpPlanner(ui64 txId, const TActorId& executer, TVector<NYql::NDqProto::TDqTask>&& tasks,
    THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& scanTasks, const IKqpGateway::TKqpSnapshot& snapshot,
    const TString& database, const TMaybe<TString>& userToken, TInstant deadline,
    const Ydb::Table::QueryStatsCollection::Mode& statsMode, bool disableLlvmForUdfStages, bool enableLlvm,
    bool withSpilling, const TMaybe<NKikimrKqp::TRlPath>& rlPath, NWilson::TTraceId traceId = {});

} // namespace NKikimr::NKqp
