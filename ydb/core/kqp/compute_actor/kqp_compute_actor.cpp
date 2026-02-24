#include "kqp_compute_actor.h"

#include "kqp_scan_compute_actor.h"
#include "kqp_scan_fetcher_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/kqp/runtime/kqp_sequencer_factory.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_factory.h>
#include <ydb/core/kqp/runtime/kqp_vector_actor.h>
#include <ydb/core/kqp/runtime/kqp_write_actor.h>
#include <ydb/core/kqp/runtime/kqp_full_text_source.h>
#include <ydb/core/kqp/runtime/kqp_sys_view_source.h>
#include <ydb/library/formats/arrow/protos/ssa.pb.h>
#include <ydb/library/yql/dq/actors/input_transforms/dq_input_transform_lookup_factory.h>
#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/providers/generic/actors/yql_generic_provider_factories.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/solomon/actors/dq_solomon_read_actor.h>
#include <ydb/library/yql/providers/solomon/actors/dq_solomon_write_actor.h>

namespace NKikimr {
namespace NMiniKQL {

using TCallableActorBuilderFunc = std::function<
    IComputationNode*(
        TCallable& callable, const TComputationNodeFactoryContext& ctx, TKqpScanComputeContext& computeCtx)>;

TComputationNodeFactory GetKqpActorComputeFactory(TKqpScanComputeContext* computeCtx, const std::optional<NKqp::TKqpFederatedQuerySetup>& federatedQuerySetup) {
    MKQL_ENSURE_S(computeCtx);

    auto computeFactory = NKqp::MakeKqpFederatedQueryComputeFactory(
        GetKqpBaseComputeFactory(computeCtx),
        federatedQuerySetup
    );

    return [computeFactory, computeCtx]
        (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (auto compute = computeFactory(callable, ctx)) {
                return compute;
            }

            auto name = callable.GetType()->GetName();

            if (name == "KqpWideReadTable"sv) {
                return WrapKqpScanWideReadTable(callable, ctx, *computeCtx);
            }

            if (name == "KqpWideReadTableRanges"sv) {
                return WrapKqpScanWideReadTableRanges(callable, ctx, *computeCtx);
            }

            if (name == "KqpBlockReadTableRanges"sv) {
                return WrapKqpScanBlockReadTableRanges(callable, ctx, *computeCtx);
            }

            // only for _pure_ compute actors!
            if (name == "KqpEnsure"sv) {
                return WrapKqpEnsure(callable, ctx);
            }

            if (name == "KqpIndexLookupJoin"sv) {
                return WrapKqpIndexLookupJoin(callable, ctx);
            }

            if (name == "DqBlockHashJoin"sv) {
                return WrapDqBlockHashJoin(callable, ctx);
            }

            if (name == "DqHashCombine"sv) {
                return WrapDqHashCombine(callable, ctx);
            }

            if (name == "DqHashAggregate"sv) {
                return WrapDqHashAggregate(callable, ctx);
            }

            if (name == "FulltextAnalyze"sv) {
                return WrapFulltextAnalyze(callable, ctx);
            }

            return nullptr;
        };
}
} // namespace NMiniKQL

namespace NKqp {

NYql::NDq::IDqAsyncIoFactory::TPtr CreateKqpAsyncIoFactory(
    TIntrusivePtr<TKqpCounters> counters,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory
    ) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterStreamLookupActorFactory(*factory, counters);
    RegisterKqpReadActor(*factory, counters);
    RegisterKqpWriteActor(*factory, counters);
    RegisterSequencerActorFactory(*factory, counters);
    RegisterKqpVectorResolveActor(*factory, counters);
    RegisterKqpFullTextSource(*factory, counters);
    RegisterKqpSysViewSource(*factory, counters);
    NYql::NDq::RegisterDqInputTransformLookupActorFactory(*factory);

    if (federatedQuerySetup) {
        auto s3HttpRetryPolicy = NYql::GetHTTPDefaultRetryPolicy(NYql::THttpRetryPolicyOptions{.RetriedCurlCodes = NYql::FqRetriedCurlCodes()});
        s3ActorsFactory->RegisterS3ReadActorFactory(*factory, federatedQuerySetup->CredentialsFactory, federatedQuerySetup->HttpGateway, s3HttpRetryPolicy, federatedQuerySetup->S3ReadActorFactoryConfig, nullptr, federatedQuerySetup->S3GatewayConfig.GetAllowLocalFiles());
        s3ActorsFactory->RegisterS3WriteActorFactory(*factory,  federatedQuerySetup->CredentialsFactory, federatedQuerySetup->HttpGateway, s3HttpRetryPolicy);

        if (federatedQuerySetup->ConnectorClient) {
            RegisterGenericProviderFactories(*factory, federatedQuerySetup->CredentialsFactory, federatedQuerySetup->ConnectorClient);
        }

        NYql::NDq::RegisterDQSolomonReadActorFactory(*factory, federatedQuerySetup->CredentialsFactory);
        bool enableStreamingQueriesCounters = NKikimr::AppData()->FeatureFlags.GetEnableStreamingQueriesCounters();
        NYql::NDq::RegisterDQSolomonWriteActorFactory(*factory, federatedQuerySetup->CredentialsFactory, counters->GetKqpCounters()->GetSubgroup("subsystem", "DqSinkTracker"), enableStreamingQueriesCounters);
        NYql::NDq::RegisterDqPqReadActorFactory(*factory, *federatedQuerySetup->Driver, federatedQuerySetup->CredentialsFactory, federatedQuerySetup->PqGateway, counters->GetKqpCounters()->GetSubgroup("subsystem", "DqSourceTracker"), {}, enableStreamingQueriesCounters);
        NYql::NDq::RegisterDqPqWriteActorFactory(*factory, *federatedQuerySetup->Driver, federatedQuerySetup->CredentialsFactory, federatedQuerySetup->PqGateway, counters->GetKqpCounters()->GetSubgroup("subsystem", "DqSinkTracker"), enableStreamingQueriesCounters, NKikimr::AppData()->FeatureFlags.GetEnableStreamingQueriesPqSinkDeduplication());
    }

    return factory;
}

void TShardsScanningPolicy::FillRequestScanFeatures(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta,
    ui32& maxInFlight, bool& isAggregationRequest) const {
    const bool enableShardsSequentialScan = (meta.HasEnableShardsSequentialScan() ? meta.GetEnableShardsSequentialScan() : true);

    isAggregationRequest = false;
    maxInFlight = 1;

    NKikimrSSA::TProgram program;
    bool hasGroupByWithFields = false;
    bool hasGroupByWithNoFields = false;
    if (meta.HasOlapProgram()) {
        Y_ABORT_UNLESS(program.ParseFromString(meta.GetOlapProgram().GetProgram()));
        for (auto&& command : program.GetCommand()) {
            if (!command.HasGroupBy()) {
                continue;
            }
            if (command.GetGroupBy().GetKeyColumns().size()) {
                hasGroupByWithFields = true;
            } else {
                hasGroupByWithNoFields = true;
            }
        }
    }
    isAggregationRequest = hasGroupByWithFields || hasGroupByWithNoFields;
    if (enableShardsSequentialScan) {
        maxInFlight = 1;
    } else if (hasGroupByWithFields) {
        maxInFlight = ProtoConfig.GetAggregationGroupByLimit();
    } else if (hasGroupByWithNoFields) {
        maxInFlight = ProtoConfig.GetAggregationNoGroupLimit();
    } else {
        maxInFlight = ProtoConfig.GetScanLimit();
    }
}

TConclusionStatus TCPULimits::DeserializeFromProto(const NKikimrKqp::TEvStartKqpTasksRequest& config) {
    const static auto maxThreadsCount = TActivationContext::ActorSystem()->GetPoolMaxThreadsCount(TActivationContext::AsActorContext().SelfID.PoolID());
    const auto share = config.GetPoolMaxCpuShare();
    if (share <= 0 || 1 < share) {
        return TConclusionStatus::Fail("cpu share have to be in (0, 1] interval");
    }
    CPUGroupThreadsLimit = Max<ui64>(1, maxThreadsCount) * share;
    CPUGroupName = config.GetPoolId();
    return TConclusionStatus::Success();
}

} // namespace NKqp
} // namespace NKikimr

namespace NKikimr::NKqp {

using namespace NYql::NDq;
using namespace NYql::NDqProto;

IActor* CreateKqpScanComputeActor(const TActorId& executerId, ui64 txId,
    TDqTask* task, IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NYql::NDq::TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId,
    TIntrusivePtr<NActors::TProtoArenaHolder> arena, NScheduler::TSchedulableActorOptions schedulableOptions,
    NKikimrConfig::TTableServiceConfig::EBlockTrackingMode mode)
{
    return new NScanPrivate::TKqpScanComputeActor(std::move(schedulableOptions), executerId, txId, task, std::move(asyncIoFactory),
        settings, memoryLimits, std::move(traceId), std::move(arena), mode);
}

IActor* CreateKqpScanFetcher(const NKikimrKqp::TKqpSnapshot& snapshot, std::vector<NActors::TActorId>&& computeActors,
    const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const NYql::NDq::TComputeRuntimeSettings& settings,
    const TString& database, const ui64 txId, TMaybe<ui64> lockTxId, ui32 lockNodeId,
    TMaybe<NKikimrDataEvents::ELockMode> lockMode, const TShardsScanningPolicy& shardsScanningPolicy,
    TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId, const TCPULimits& cpuLimits) {
    return new NScanPrivate::TKqpScanFetcherActor(snapshot, settings, std::move(computeActors), txId, lockTxId, lockNodeId, lockMode,
        database, meta, shardsScanningPolicy, counters, std::move(traceId), cpuLimits);
}

} // namespace NKikimr::NKqp
