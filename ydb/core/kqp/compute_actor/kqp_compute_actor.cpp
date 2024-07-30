#include "kqp_compute_actor.h"
#include "kqp_scan_compute_actor.h"
#include "kqp_scan_fetcher_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_write_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/kqp/runtime/kqp_sequencer_factory.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_factory.h>
#include <ydb/library/yql/providers/generic/actors/yql_generic_source_factory.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_sink_factory.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_source_factory.h>


namespace NKikimr {
namespace NMiniKQL {

using TCallableActorBuilderFunc = std::function<
    IComputationNode*(
        TCallable& callable, const TComputationNodeFactoryContext& ctx, TKqpScanComputeContext& computeCtx)>;

TComputationNodeFactory GetKqpActorComputeFactory(TKqpScanComputeContext* computeCtx) {
    MKQL_ENSURE_S(computeCtx);

    auto computeFactory = GetKqpBaseComputeFactory(computeCtx);

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

            return nullptr;
        };
}
} // namespace NMiniKQL

namespace NKqp {

NYql::NDq::IDqAsyncIoFactory::TPtr CreateKqpAsyncIoFactory(
    TIntrusivePtr<TKqpCounters> counters,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterStreamLookupActorFactory(*factory, counters);
    RegisterKqpReadActor(*factory, counters);
    RegisterKqpWriteActor(*factory, counters);
    RegisterSequencerActorFactory(*factory, counters);

    if (federatedQuerySetup) {
        auto s3HttpRetryPolicy = NYql::GetHTTPDefaultRetryPolicy(NYql::THttpRetryPolicyOptions{.RetriedCurlCodes = NYql::FqRetriedCurlCodes()});
        RegisterS3ReadActorFactory(*factory, federatedQuerySetup->CredentialsFactory, federatedQuerySetup->HttpGateway, s3HttpRetryPolicy, federatedQuerySetup->S3ReadActorFactoryConfig);
        RegisterS3WriteActorFactory(*factory,  federatedQuerySetup->CredentialsFactory, federatedQuerySetup->HttpGateway, s3HttpRetryPolicy);

        if (federatedQuerySetup->ConnectorClient) {
            RegisterGenericReadActorFactory(*factory, federatedQuerySetup->CredentialsFactory, federatedQuerySetup->ConnectorClient);
        }
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
}
} // namespace NKikimr

namespace NKikimr::NKqp {

using namespace NYql::NDq;
using namespace NYql::NDqProto;

IActor* CreateKqpScanComputeActor(const TActorId& executerId, ui64 txId,
    TDqTask* task, IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const NYql::NDq::TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, NWilson::TTraceId traceId,
    TIntrusivePtr<NActors::TProtoArenaHolder> arena) {
    return new NScanPrivate::TKqpScanComputeActor(executerId, txId, task, std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, std::move(traceId), std::move(arena));
}

IActor* CreateKqpScanFetcher(const NKikimrKqp::TKqpSnapshot& snapshot, std::vector<NActors::TActorId>&& computeActors,
    const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const NYql::NDq::TComputeRuntimeSettings& settings,
    const ui64 txId, const TShardsScanningPolicy& shardsScanningPolicy, TIntrusivePtr<TKqpCounters> counters, NWilson::TTraceId traceId) {
    return new NScanPrivate::TKqpScanFetcherActor(snapshot, settings, std::move(computeActors), txId, meta, shardsScanningPolicy, counters, std::move(traceId));
}

}
