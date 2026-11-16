#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

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

