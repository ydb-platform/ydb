#include "kqp_compute_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_factory.h>

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

            // only for _pure_ compute actors!
            if (name == "KqpEnsure"sv) {
                return WrapKqpEnsure(callable, ctx);
            }

            return nullptr;
        };
}
} // namespace NMiniKQL

namespace NKqp {

NYql::NDq::IDqAsyncIoFactory::TPtr CreateKqpAsyncIoFactory(TIntrusivePtr<TKqpCounters> counters) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterStreamLookupActorFactory(*factory, counters);
    RegisterKqpReadActor(*factory, counters);
    return factory;
}

void TShardsScanningPolicy::FillRequestScanFeatures(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta,
    ui32& maxInFlight, bool& isAggregationRequest) const {
    const bool isSorted = (meta.HasSorted() ? meta.GetSorted() : true);

    isAggregationRequest = false;
    maxInFlight = 1;

    NKikimrSSA::TProgram program;
    bool hasGroupByWithFields = false;
    bool hasGroupByWithNoFields = false;
    if (meta.HasOlapProgram()) {
        Y_VERIFY(program.ParseFromString(meta.GetOlapProgram().GetProgram()));
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
    if (isSorted) {
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

