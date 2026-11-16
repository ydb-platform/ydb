#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TExprBase KqpRewriteTakeSortToTopSort(TExprBase node, TExprContext& ctx, const TParentsMap& parents) {
    const auto maybeTake = node.Maybe<TCoTake>();
    if (!maybeTake) {
        return node;
    }

    const auto take = maybeTake.Cast();
    const auto maybeSkip = take.Input().Maybe<TCoSkip>();
    if (maybeSkip && (!IsKqpRepeatableExpr(take.Count()) || !IsKqpRepeatableExpr(maybeSkip.Cast().Count()))) {
        // The rewrite copies both counts into a partial TopSort.
        return node;
    }

    const auto output = DqRewriteTakeSortToTopSort(node, ctx, parents);
    if (!maybeSkip || output.Raw() == node.Raw()) {
        return output;
    }

    // DQ produces [ExtractMembers] -> Take -> Skip -> TopSort. Keep its purity
    // and consumer checks, but use a saturating count for KQP pagination.
    const auto maybeExtract = output.Maybe<TCoExtractMembers>();
    const auto result = maybeExtract ? maybeExtract.Cast().Input() : output;
    const auto maybeTopSort = result.Maybe<TCoTake>().Input().Maybe<TCoSkip>().Input().Maybe<TCoTopSort>();
    if (!maybeTopSort) {
        return node;
    }

    const auto count = BuildKqpTakeSkipCount(take.Count(), maybeSkip.Cast().Count(), ctx);
    const auto topSort = ctx.ChangeChild(maybeTopSort.Cast().Ref(), TCoTopSort::idx_Count, count.Ptr());
    return TExprBase(ctx.ReplaceNode(output.Ptr(), maybeTopSort.Cast().Ref(), topSort));
}

TExprBase KqpTopSortOverExtend(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parents) {
    if (!node.Maybe<TCoTopBase>().Input().Maybe<TCoExtend>()) {
        return node;
    }

    auto topBase = node.Cast<TCoTopBase>();
    auto extend = topBase.Input().Cast<TCoExtend>();

    if (!IsSingleConsumer(extend, parents)) {
        return node;
    }

    TVector<TExprBase> inputs;
    inputs.reserve(extend.ArgCount());
    for (const auto& arg : extend) {
        auto input = Build<TCoTopBase>(ctx, node.Pos())
            .CallableName(node.Ref().Content())
            .Input(arg)
            .Count(topBase.Count())
            .SortDirections(topBase.SortDirections())
            .KeySelectorLambda(topBase.KeySelectorLambda())
            .Done();

        inputs.push_back(input);
    }

    // Decompose TopSort to Sort + Limit to avoid optimizer loops.
    return Build<TCoLimit>(ctx, node.Pos())
        .Input<TCoSort>()
            .Input<TCoExtend>()
                .Add(inputs)
                .Build()
            .SortDirections(topBase.SortDirections())
            .KeySelectorLambda(topBase.KeySelectorLambda())
            .Build()
        .Count(topBase.Count())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

