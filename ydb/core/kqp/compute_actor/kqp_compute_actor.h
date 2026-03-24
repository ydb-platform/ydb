#pragma once

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_actor.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

namespace NKikimr {

namespace NMiniKQL {
    class TKqpScanComputeContext;
    TComputationNodeFactory GetKqpActorComputeFactory(TKqpScanComputeContext* computeCtx, const std::optional<NKqp::TKqpFederatedQuerySetup>& federatedQuerySetup);
} // namespace NMiniKQL

namespace NKqp {

class TShardsScanningPolicy {
public:
    TShardsScanningPolicy(ui32 aggregationGroupByLimit, ui32 aggregationNoGroupLimit, ui32 scanLimit,
      bool isParallelScanningAvailable, ui32 shardSplitFactor, ui32 criticalTotalRetriesCount, ui32 reaskShardRetriesCount)
        : AggregationGroupByLimit(aggregationGroupByLimit)
        , AggregationNoGroupLimit(aggregationNoGroupLimit)
        , ScanLimit(scanLimit)
        , IsParallelScanningAvailable(isParallelScanningAvailable)
        , ShardSplitFactor(shardSplitFactor)
        , CriticalTotalRetriesCount(criticalTotalRetriesCount)
        , ReaskShardRetriesCount(reaskShardRetriesCount)
    {
    }

    TShardsScanningPolicy()
        : AggregationGroupByLimit(256)
        , AggregationNoGroupLimit(1024)
        , ScanLimit(3)
        , IsParallelScanningAvailable(false)
        , ShardSplitFactor(5)
        , CriticalTotalRetriesCount(20)
        , ReaskShardRetriesCount(5)
    {
    }

    void FillRequestScanFeatures(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta,
        ui32& maxInFlight, bool& isAggregationRequest) const;

    const ui32 AggregationGroupByLimit;
    const ui32 AggregationNoGroupLimit;
    const ui32 ScanLimit;
    const bool IsParallelScanningAvailable;
    const ui32 ShardSplitFactor;
    const ui32 CriticalTotalRetriesCount;
    const ui32 ReaskShardRetriesCount;
};

class TCPULimits {
    YDB_OPT(double, CPUGroupThreadsLimit);
    YDB_READONLY_DEF(TString, CPUGroupName);
public:
    TConclusionStatus DeserializeFromProto(const NKikimrKqp::TEvStartKqpTasksRequest& config);
};

IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask* task,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const NYql::NDq::TComputeRuntimeSettings& settings,
    const NYql::NDq::TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId,
    TIntrusivePtr<NActors::TProtoArenaHolder> arena,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    NScheduler::TSchedulableActorOptions schedulableOptions,
    NKikimrConfig::TTableServiceConfig::EBlockTrackingMode mode,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database
);

IActor* CreateKqpScanComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask* task,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const NYql::NDq::TComputeRuntimeSettings& settings,
    const NYql::NDq::TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId,
    TIntrusivePtr<NActors::TProtoArenaHolder> arena,
    NScheduler::TSchedulableActorOptions schedulableOptions,
    NKikimrConfig::TTableServiceConfig::EBlockTrackingMode mode);

IActor* CreateKqpScanFetcher(const NKikimrKqp::TKqpSnapshot& snapshot, std::vector<NActors::TActorId>&& computeActors,
    const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const NYql::NDq::TComputeRuntimeSettings& settings,
    const TString& database, const ui64 txId, TMaybe<ui64> lockTxId, ui32 lockNodeId,
    TMaybe<NKikimrDataEvents::ELockMode> lockMode, const TShardsScanningPolicy& shardsScanningPolicy,
    TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId, const TCPULimits& cpuLimits);

NYql::NDq::IDqAsyncIoFactory::TPtr CreateKqpAsyncIoFactory(
    TIntrusivePtr<TKqpCounters> counters,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory);

} // namespace NKqp
} // namespace NKikimr
