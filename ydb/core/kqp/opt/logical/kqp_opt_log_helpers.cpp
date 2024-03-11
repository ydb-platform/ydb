#include "kqp_opt_log_impl.h"

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

TExprBase TKqpMatchReadResult::BuildProcessNodes(TExprBase input, TExprContext& ctx) const {
    auto expr = input;

    if (ExtractMembers) {
        expr = Build<TCoExtractMembers>(ctx, ExtractMembers.Cast().Pos())
            .Input(expr)
            .Members(ExtractMembers.Cast().Members())
            .Done();
    }

    if (FilterNullMembers) {
        expr = Build<TCoFilterNullMembers>(ctx, FilterNullMembers.Cast().Pos())
            .Input(expr)
            .Members(FilterNullMembers.Cast().Members())
            .Done();
    }

    if (SkipNullMembers) {
        expr = Build<TCoSkipNullMembers>(ctx, SkipNullMembers.Cast().Pos())
            .Input(expr)
            .Members(SkipNullMembers.Cast().Members())
            .Done();
    }

    if (FlatMap) {
        expr = Build<TCoFlatMap>(ctx, FlatMap.Cast().Pos())
            .Input(expr)
            .Lambda(FlatMap.Cast().Lambda())
            .Done();
    }

    return expr;
}

TMaybe<TKqpMatchReadResult> MatchRead(TExprBase node, std::function<bool(TExprBase)> matchFunc) {
    auto expr = node;

    TMaybeNode<TCoFlatMap> flatmap;
    if (auto maybeNode = expr.Maybe<TCoFlatMap>()) {
        flatmap = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoSkipNullMembers> skipNullMembers;
    if (auto maybeNode = expr.Maybe<TCoSkipNullMembers>()) {
        skipNullMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoFilterNullMembers> filterNullMembers;
    if (auto maybeNode = expr.Maybe<TCoFilterNullMembers>()) {
        filterNullMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoExtractMembers> extractMembers;
    if (auto maybeNode = expr.Maybe<TCoExtractMembers>()) {
        extractMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    if (!matchFunc(expr)) {
        return {};
    }

    return TKqpMatchReadResult {
        .Read = expr,
        .ExtractMembers = extractMembers,
        .FilterNullMembers = filterNullMembers,
        .SkipNullMembers = skipNullMembers,
        .FlatMap = flatmap
    };
}

} // namespace NKikimr::NKqp::NOpt
