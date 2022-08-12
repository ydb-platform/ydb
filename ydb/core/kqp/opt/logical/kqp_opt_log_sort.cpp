#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TExprBase KqpTopSortOverExtend(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parents) {
    if (!node.Maybe<TCoTopSort>().Input().Maybe<TCoExtend>()) {
        return node;
    }

    auto topSort = node.Cast<TCoTopSort>();
    auto extend = topSort.Input().Cast<TCoExtend>();

    if (!IsSingleConsumer(extend, parents)) {
        return node;
    }

    TVector<TExprBase> inputs;
    inputs.reserve(extend.ArgCount());
    for (const auto& arg : extend) {
        auto input = Build<TCoTopSort>(ctx, node.Pos())
            .Input(arg)
            .Count(topSort.Count())
            .SortDirections(topSort.SortDirections())
            .KeySelectorLambda(topSort.KeySelectorLambda())
            .Done();

        inputs.push_back(input);
    }

    // Decompose TopSort to Sort + Limit to avoid optimizer loops.
    return Build<TCoLimit>(ctx, node.Pos())
        .Input<TCoSort>()
            .Input<TCoExtend>()
                .Add(inputs)
                .Build()
            .SortDirections(topSort.SortDirections())
            .KeySelectorLambda(topSort.KeySelectorLambda())
            .Build()
        .Count(topSort.Count())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

