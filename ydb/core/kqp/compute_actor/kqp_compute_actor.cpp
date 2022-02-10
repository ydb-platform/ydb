#include "kqp_compute_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>

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
} // namespace NKikimr

