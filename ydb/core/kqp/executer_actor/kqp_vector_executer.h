#pragma once

#include "kqp_tasks_graph.h"

#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NKikimr::NKqp {

// A single local coordinator replaces the three DQ stages of a checked vector
// transaction. The owning data executer keeps authorization, snapshot selection,
// cancellation, reply limits and session lifecycle. No tablet addresses are cached.
class TKqpVectorExecution {
public:
    static std::unique_ptr<TKqpVectorExecution> Prepare(
        const IKqpGateway::TExecPhysicalRequest& request, TKqpTasksGraph& graph,
        const TIntrusivePtr<TUserRequestContext>& context, bool hasBuffer);

    ~TKqpVectorExecution();

    // Returns an admission failure before creating any read actor.
    TMaybe<TString> Start(const TActorId& parent, NYql::NDq::IDqAsyncIoFactory& factory,
        const IKqpGateway::TKqpSnapshot& snapshot, ui64 querySpanId, const NWilson::TTraceId& traceId);
    bool Poll(NYql::NDq::TDqSerializedBatch& result);
    void FinishStats(TQueryExecutionStats& stats);

private:
    TKqpVectorExecution(const IKqpGateway::TExecPhysicalRequest& request,
        NKikimrTxDataShard::TKqpVectorSearchSettings settings, TString target,
        TIntrusivePtr<TUserRequestContext> context, ui64 txId);

    const IKqpGateway::TExecPhysicalRequest& Request;
    NKikimrTxDataShard::TKqpVectorSearchSettings Settings;
    const NKqpProto::TKqpPhyVectorExecution& Plan;
    ui64 TxId;
    double CpuTimeUs = 0;
    TString Target;
    TIntrusivePtr<TUserRequestContext> Context;
    TQueryQuotaManagerPtr Quota;
    TTxAllocatorState::TPtr Alloc;
    const NMiniKQL::TType* ResultType = nullptr;
    NYql::NDq::IDqComputeActorAsyncInput* Input = nullptr;
};

} // namespace NKikimr::NKqp
