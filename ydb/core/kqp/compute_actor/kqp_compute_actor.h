#pragma once
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr {

namespace NMiniKQL {

class TKqpScanComputeContext;

TComputationNodeFactory GetKqpActorComputeFactory(TKqpScanComputeContext* computeCtx);

} // namespace NMiniKQL

namespace NKqp {

class TShardsScanningPolicy {
private:
    const NKikimrConfig::TTableServiceConfig::TShardsScanningPolicy ProtoConfig;
public:
    TShardsScanningPolicy(const NKikimrConfig::TTableServiceConfig::TShardsScanningPolicy& pbConfig)
        : ProtoConfig(pbConfig)
    {

    }

    bool IsParallelScanningAvailable() const {
        return ProtoConfig.GetParallelScanningAvailable();
    }

    bool GetShardSplitFactor() const {
        return ProtoConfig.GetShardSplitFactor();
    }

    void FillRequestScanFeatures(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta,
        ui32& maxInFlight, bool& isAggregationRequest) const;

};

IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask&& task,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits,
    NWilson::TTraceId traceId = {});

IActor* CreateKqpScanComputeActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TActorId& executerId, ui64 txId,
    NYql::NDqProto::TDqTask&& task, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits,
    const TShardsScanningPolicy& shardsScanningPolicy, TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId);

NYql::NDq::IDqAsyncIoFactory::TPtr CreateKqpAsyncIoFactory(TIntrusivePtr<TKqpCounters> counters);

} // namespace NKqp
} // namespace NKikimr
