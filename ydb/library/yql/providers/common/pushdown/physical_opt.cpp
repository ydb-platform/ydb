#include "predicate_node.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>

namespace NYql::NPushdown {

using namespace NNodes;

namespace {

TPredicateNode SplitForPartialPushdown(const NPushdown::TPredicateNode& predicateTree, TExprContext& ctx, TPositionHandle pos, const TSettings& settings) {
    if (predicateTree.CanBePushed) {
        return predicateTree;
    }

    if (predicateTree.Op != NPushdown::EBoolOp::And && (!settings.IsEnabled(TSettings::EFeatureFlag::SplitOrOperator) || predicateTree.Op != NPushdown::EBoolOp::Or)) {
        // Predicate can't be split, so return invalid value and skip this branch
        return NPushdown::TPredicateNode();
    }

    std::vector<NPushdown::TPredicateNode> pushable;
    for (auto& predicate : predicateTree.Children) {
        NPushdown::TPredicateNode pushablePredicate = SplitForPartialPushdown(predicate, ctx, pos, settings);
        if (pushablePredicate.IsValid()) {
            pushable.emplace_back(pushablePredicate);
        } else if (predicateTree.Op == NPushdown::EBoolOp::Or) {
            // One of the OR branch was invalid, so the whole predicate is invalid
            return NPushdown::TPredicateNode();
        }
    }

    NPushdown::TPredicateNode predicateToPush;
    predicateToPush.SetPredicates(pushable, ctx, pos, predicateTree.Op);
    return predicateToPush;
}

}

NPushdown::TPredicateNode MakePushdownNode(const NNodes::TCoLambda& lambda, TExprContext& ctx, const TPositionHandle& pos, const TSettings& settings) {
    auto lambdaArg = lambda.Args().Arg(0).Ptr();

    YQL_LOG(TRACE) << "Push filter. Initial filter lambda: " << NCommon::ExprToPrettyString(ctx, lambda.Ref());

    auto maybeOptionalIf = lambda.Body().Maybe<TCoOptionalIf>();
    if (!maybeOptionalIf.IsValid()) { // Nothing to push
        return {};
    }

    TCoOptionalIf optionalIf = maybeOptionalIf.Cast();
    NPushdown::TPredicateNode predicateTree(optionalIf.Predicate());
    NPushdown::CollectPredicates(ctx, optionalIf.Predicate(), predicateTree, TExprBase(lambdaArg), TExprBase(lambdaArg), settings);
    YQL_ENSURE(predicateTree.IsValid(), "Collected filter predicates are invalid");

    return SplitForPartialPushdown(predicateTree, ctx, pos, settings);
}

TMaybeNode<TCoLambda> MakePushdownPredicate(const TCoLambda& lambda, TExprContext& ctx, const TPositionHandle& pos, const TSettings& settings) {
    NPushdown::TPredicateNode predicateToPush = MakePushdownNode(lambda, ctx, pos, settings);
    if (!predicateToPush.IsValid()) {
        return {};
    }

    // clang-format off
    auto newFilterLambda = Build<TCoLambda>(ctx, pos)
        .Args({"filter_row"})
        .Body<TExprApplier>()
            .Apply(predicateToPush.ExprNode.Cast())
            .With(lambda.Args().Arg(0), "filter_row")
            .Build()
        .Done();
    // clang-format on

    YQL_LOG(INFO) << "Push filter lambda: " << NCommon::ExprToPrettyString(ctx, *newFilterLambda.Ptr());
    return newFilterLambda;
}

} // namespace NYql::NPushdown
