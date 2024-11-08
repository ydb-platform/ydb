#include "predicate_node.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>

namespace NYql::NPushdown {

using namespace NNodes;

namespace {

TPredicateNode SplitForPartialPushdown(
    const NPushdown::TPredicateNode& predicateTree,
    TExprContext& ctx,
    TPositionHandle pos) {
    if (predicateTree.CanBePushed) {
        return predicateTree;
    }

    if (predicateTree.Op != NPushdown::EBoolOp::And) {
        return NPushdown::TPredicateNode(); // Not valid, => return the same node from optimizer
    }

    std::vector<NPushdown::TPredicateNode> pushable;
    for (auto& predicate : predicateTree.Children) {
        if (predicate.CanBePushed) {
            pushable.emplace_back(predicate);
        }
    }
    NPushdown::TPredicateNode predicateToPush;
    predicateToPush.SetPredicates(pushable, ctx, pos);
    return predicateToPush;
}

}

TMaybeNode<TCoLambda> MakePushdownPredicate(const TCoLambda& lambda, TExprContext& ctx, const TPositionHandle& pos, const TSettings& settings) {
    auto lambdaArg = lambda.Args().Arg(0).Ptr();

    YQL_LOG(TRACE) << "Push filter. Initial filter lambda: " << NCommon::ExprToPrettyString(ctx, lambda.Ref());

    auto maybeOptionalIf = lambda.Body().Maybe<TCoOptionalIf>();
    if (!maybeOptionalIf.IsValid()) { // Nothing to push
        return {};
    }

    TCoOptionalIf optionalIf = maybeOptionalIf.Cast();
    NPushdown::TPredicateNode predicateTree(optionalIf.Predicate());
    NPushdown::CollectPredicates(optionalIf.Predicate(), predicateTree, lambdaArg.Get(), TExprBase(lambdaArg), settings);
    YQL_ENSURE(predicateTree.IsValid(), "Collected filter predicates are invalid");

    NPushdown::TPredicateNode predicateToPush = SplitForPartialPushdown(predicateTree, ctx, pos);
    if (!predicateToPush.IsValid()) {
        return {};
    }

    // clang-format off
    auto newFilterLambda = Build<TCoLambda>(ctx, pos)
        .Args({"filter_row"})
        .Body<TExprApplier>()
            .Apply(predicateToPush.ExprNode.Cast())
            .With(TExprBase(lambdaArg), "filter_row")
            .Build()
        .Done();
    // clang-format on

    YQL_LOG(INFO) << "Push filter lambda: " << NCommon::ExprToPrettyString(ctx, *newFilterLambda.Ptr());
    return newFilterLambda;
}

} // namespace NYql::NPushdown
