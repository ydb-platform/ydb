#include <ydb/core/kqp/opt/physical/kqp_opt_phy_olap_filter.h>
#include <ydb/core/kqp/opt/physical/predicate_collector.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

bool IsSuitableToPushPredicateToColumnTables(IOperator* input) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto predicate = TCoLambda(filter->GetFilterExpression().Node).Body();
    if (predicate.Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        return false;
    }

    const auto maybeRead = filter->GetInput().Get();
    if (maybeRead->Kind != EOperator::Source){
        return false;
    }

    const auto read = CastOperator<TOpRead>(maybeRead);
    return read->GetTableStorageType() == NYql::EStorageType::ColumnStorage && !read->OlapFilterLambda;
}

bool IsValidPredicateToKeep(TMaybeNode<TExprBase> node) {
    if (!node.IsValid()) {
        return false;
    }

    if (auto predicate = node.Cast().Maybe<TCoBool>()) {
        return predicate.Cast().Literal().StringValue() != "true";
    }
    return true;
}

// The lambda after peephole takes the row first, then `extraArgs` in the same order.
TExprNode::TPtr ApplyPeephole(TExprNode::TPtr input, TExprNode::TPtr lambdaArg, const TExprNode::TListType& extraArgs, TRBOContext& ctx) {
    Y_ENSURE(lambdaArg->GetTypeAnn());
    const auto pos = input->Pos();
    auto newArg = Build<TCoArgument>(ctx.ExprCtx, pos)
        .Name("new_arg").Done().Ptr();
    newArg->SetTypeAnn(lambdaArg->GetTypeAnn());
    lambdaArg->CopyConstraints(*newArg);

    TExprNode::TListType args{newArg};
    TVector<const TTypeAnnotationNode *> argTypes{lambdaArg->GetTypeAnn()};
    for (const auto& arg : extraArgs) {
        Y_ENSURE(arg->GetTypeAnn());
        args.push_back(arg);
        argTypes.push_back(arg->GetTypeAnn());
    }
    auto body = ctx.ExprCtx.ReplaceNode(std::move(input), *lambdaArg, newArg);

    // clang-format off
    auto olapPredicateClosure = Build<TKqpPredicateClosure>(ctx.ExprCtx, pos)
        .Lambda(ctx.ExprCtx.NewLambda(pos, ctx.ExprCtx.NewArguments(pos, std::move(args)), std::move(body)))
        .ArgsType(ExpandType(pos, *ctx.ExprCtx.MakeType<TTupleExprType>(argTypes), ctx.ExprCtx))
    .Done();
    // clang-format on

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO OLAP FILTER] Before peephole: " << KqpExprToPrettyString(olapPredicateClosure, ctx.ExprCtx);

    TPeepholeSettings peepholeSettings;
    peepholeSettings.WithFinalStageRules = false;
    TExprNode::TPtr afterPeephole;
    bool hasNonDeterministicFunctions;
    if (const auto status = PeepHoleOptimizeNode(olapPredicateClosure.Ptr(), afterPeephole, ctx.ExprCtx, ctx.TypeCtx, nullptr,
                                                 hasNonDeterministicFunctions);
        status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(ERROR, ProviderKqp) << "[NEW RBO OLAP FILTER] Peephole failed with status: " << status << Endl;
        Y_ENSURE(false, "Peephole failed.");
    }
    Y_ENSURE(afterPeephole);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO OLAP FILTER] After peephole: " << KqpExprToPrettyString(TExprBase(afterPeephole), ctx.ExprCtx);
    return TExprBase(afterPeephole).Cast<TKqpPredicateClosure>().Lambda().Ptr();
}

// A call the column shard computes itself, as `KqpOlapJsonValue`, `KqpOlapJsonExists` or an ILIKE kernel.
bool IsNativeOlapCall(const TExprNode::TPtr& node, const TExprNode& lambdaArg) {
    if (const auto jsonValue = TMaybeNode<TCoJsonValue>(node)) {
        return CanBePushedAsOlapJsonValue(jsonValue.Cast(), &lambdaArg);
    }
    if (const auto jsonExists = TMaybeNode<TCoJsonExists>(node)) {
        return CanBePushedAsOlapJsonExists(jsonExists.Cast(), &lambdaArg);
    }
    return node->IsCallable() && IgnoreCaseSubstringMatchFunctions.contains(node->Content());
}

struct TNativeOlapCall {
    TExprNode::TPtr Placeholder;
    TExprNode::TPtr Call;
};

// Peephole expands the native calls into UDF calls, which the column shard can only run through `KqpOlapApply`.
// So each of them becomes a placeholder argument of the same type, as the old optimizer collects predicates before
// any peephole. A call that reads anything but the row stays, because a placeholder cannot carry other arguments.
TExprNode::TPtr HideNativeOlapCalls(const TExprNode::TPtr& predicate, const TExprNode& lambdaArg, TExprContext& ctx,
                                    TVector<TNativeOlapCall>& calls) {
    TNodeOnNodeOwnedMap replaces;
    VisitExpr(predicate, [&](const TExprNode::TPtr& node) {
        if (!IsNativeOlapCall(node, lambdaArg) || !node->GetTypeAnn()) {
            return true;
        }
        if (FindNode(node, [&](const TExprNode::TPtr& child) { return child->IsArgument() && child.Get() != &lambdaArg; }) != nullptr) {
            return true;
        }
        auto placeholder = ctx.NewArgument(node->Pos(), TStringBuilder() << "native_olap_call_" << calls.size());
        placeholder->SetTypeAnn(node->GetTypeAnn());
        replaces.emplace(node.Get(), placeholder);
        calls.push_back({std::move(placeholder), node});
        return false;
    });
    return ctx.ReplaceNodes(TExprNode::TPtr(predicate), replaces);
}

// Runs the peephole over the predicate with the native calls kept as they are.
// Returns the row argument of the predicate after the peephole and the predicate itself.
std::pair<TExprNode::TPtr, TExprNode::TPtr> ApplyPeepholeKeepingNativeOlapCalls(TExprNode::TPtr predicate, TExprNode::TPtr lambdaArg, TRBOContext& ctx) {
    TVector<TNativeOlapCall> calls;
    auto hidden = HideNativeOlapCalls(predicate, *lambdaArg, ctx.ExprCtx, calls);
    TExprNode::TListType placeholders;
    for (const auto& call : calls) {
        placeholders.push_back(call.Placeholder);
    }

    const auto lambda = TCoLambda(ApplyPeephole(std::move(hidden), lambdaArg, placeholders, ctx));
    if (lambda.Args().Size() != calls.size() + 1) {
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO OLAP FILTER] Unexpected number of lambda arguments after peephole, the native calls are not kept";
        const auto plainLambda = TCoLambda(ApplyPeephole(std::move(predicate), lambdaArg, {}, ctx));
        return {plainLambda.Args().Arg(0).Ptr(), plainLambda.Body().Ptr()};
    }

    // Peephole may rebuild the lambda, so the placeholders are matched by their positions. The calls read the new row argument.
    const auto rowArg = lambda.Args().Arg(0).Ptr();
    const TNodeOnNodeOwnedMap rowReplaces{{lambdaArg.Get(), rowArg}};
    TNodeOnNodeOwnedMap callReplaces(calls.size());
    for (size_t i = 0; i < calls.size(); ++i) {
        auto call = ctx.ExprCtx.ReplaceNodes<true>(TExprNode::TListType{calls[i].Call}, rowReplaces).front();
        callReplaces.emplace(lambda.Args().Arg(i + 1).Raw(), std::move(call));
    }
    auto body = ctx.ExprCtx.ReplaceNodes<true>(TExprNode::TListType{lambda.Body().Ptr()}, callReplaces).front();
    return {rowArg, std::move(body)};
}

} // anonymous namespace

bool TPushOlapFilterRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->GetChildren().front()->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushOlapFilterRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (!ctx.KqpCtx.Config->HasOptEnableOlapPushdown()) {
        return input;
    }

    // Input expressions and their row schemas must already use IU IDs. Keep
    // those spellings when constructing OLAP, including its embedded schemas.
    const TPushdownOptions pushdownOptions(ctx.KqpCtx.Config->GetEnableOlapScalarApply(), ctx.KqpCtx.Config->GetEnableOlapSubstringPushdown(),
                                           /*StripAliasPrefixForColumnName=*/false, ctx.KqpCtx.Config->GetEnableOlapPushdownRegexp(),
                                           ctx.KqpCtx.Config->GetEnableOlapFastAsciiIgnoreCase());
    if (!IsSuitableToPushPredicateToColumnTables(input.get())) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput().Get());
    const auto lambda = TCoLambda(filter->GetFilterExpression().Node);
    auto predicate = lambda.Body();
    auto lambdaArg = lambda.Args().Arg(0).Ptr();

    // Here we want to apply coalesce and propagate it to optional predicates.
    // clang-format off
    predicate = Build<TCoCoalesce>(ctx.ExprCtx, input->Pos)
        .Predicate(predicate)
        .Value<TCoBool>()
            .Literal().Value("false").Build()
        .Build()
    .Done();
    // clang-format on

    auto [rowArg, predicateAfterRewrite] = ApplyPeepholeKeepingNativeOlapCalls(predicate.Ptr(), lambda.Args().Arg(0).Ptr(), ctx);
    lambdaArg = std::move(rowArg);
    predicate = TExprBase(predicateAfterRewrite);

    TOLAPPredicateNode predicateTree;
    predicateTree.ExprNode = predicate.Ptr();
    CollectPredicates(predicate, predicateTree, lambdaArg.Get(), lambdaArg->GetTypeAnn(), pushdownOptions);
    YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");

    auto [pushable, remaining] = SplitForPartialPushdown(predicateTree, false);
    TVector<TFilterOpsLevels> pushedPredicates;
    for (const auto& predicate : pushable) {
        pushedPredicates.emplace_back(PredicatePushdown(TExprBase(predicate.ExprNode), *lambdaArg, ctx.ExprCtx, input->Pos, pushdownOptions));
    }

    if (pushdownOptions.AllowOlapApply) {
        TVector<TOLAPPredicateNode> remainingAfterApply;
        for (const auto& predicateExprHolder : remaining) {
            // As in the old optimizer, the column shard computes each pushable JSON_VALUE with `KqpOlapJsonValue` and
            // passes its value into `KqpOlapApply`, instead of the whole JSON column.
            TVector<TOlapExternalArg> externalArgs;
            const auto predicateWithExternalArgs =
                ReplaceJsonValuesWithExternalArgs(predicateExprHolder.ExprNode, *lambdaArg, ctx.ExprCtx, pushdownOptions, externalArgs);
            TExprNode::TListType externalArgNodes;
            for (const auto& externalArg : externalArgs) {
                externalArg.Arg->SetTypeAnn(externalArg.Type);
                externalArgNodes.push_back(externalArg.Arg);
            }

            auto lambdaAfterPeephole = TExprBase(ApplyPeephole(predicateWithExternalArgs, lambdaArg, externalArgNodes, ctx)).Cast<TCoLambda>();
            auto lArg = lambdaAfterPeephole.Args().Arg(0);
            auto predicate = lambdaAfterPeephole.Body();

            // Peephole may rebuild the lambda, so external arguments are matched by their positions.
            YQL_ENSURE(lambdaAfterPeephole.Args().Size() == externalArgs.size() + 1, "Unexpected number of lambda arguments after peephole");
            TNodeOnNodeOwnedMap externalArgsMap(externalArgs.size());
            for (size_t i = 0; i < externalArgs.size(); ++i) {
                externalArgsMap.emplace(lambdaAfterPeephole.Args().Arg(i + 1).Raw(), externalArgs[i].OlapExpression);
            }
            const auto applyPushdownOptions = pushdownOptions.WithExternalArgs(&externalArgsMap);

            TOLAPPredicateNode predicateTree;
            predicateTree.ExprNode = predicate.Ptr();
            CollectPredicates(predicate, predicateTree, &lArg.Ref(), lArg.Ptr()->GetTypeAnn(),
                applyPushdownOptions.WithAllowOlapApply(true));

            YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");
            auto [pushable, remaining] = SplitForPartialPushdown(predicateTree, true);
            for (const auto& predicate : pushable) {
                if (predicate.CanBePushed) {
                    auto pred = PredicatePushdown(TExprBase(predicate.ExprNode), lArg.Ref(), ctx.ExprCtx, input->Pos, applyPushdownOptions);
                    pushedPredicates.emplace_back(pred);
                } else {
                    auto expr = YqlApplyPushdown(TExprBase(predicate.ExprNode), lArg.Ref(), ctx.ExprCtx, applyPushdownOptions);
                    TFilterOpsLevels pred(expr);
                    pushedPredicates.emplace_back(pred);
                }
            }
            if (remaining.size()) {
                Y_ENSURE(remaining.size() == 1);
                // Use an original expr node if we cannot push to cs.
                remainingAfterApply.push_back(predicateExprHolder);
            }
        }
        remaining = std::move(remainingAfterApply);
    }

    if (pushedPredicates.empty()) {
        return input;
    }

    const auto pushedFilter = TFilterOpsLevels::Merge(pushedPredicates, ctx.ExprCtx, input->Pos);
    const auto remainingFilter = CombinePredicatesWithAnd(remaining, ctx.ExprCtx, input->Pos, false, true);

    TMaybeNode<TExprBase> olapFilter;
    if (pushedFilter.FirstLevelOps.IsValid()) {
        // clang-format off
        olapFilter = Build<TKqpOlapFilter>(ctx.ExprCtx, input->Pos)
            .Input(lambda.Args().Arg(0))
            .Condition(pushedFilter.FirstLevelOps.Cast())
        .Done();
        // clang-format on
    }

    if (pushedFilter.SecondLevelOps.IsValid()) {
        // clang-format off
        olapFilter = Build<TKqpOlapFilter>(ctx.ExprCtx, input->Pos)
            .Input(olapFilter.IsValid() ? olapFilter.Cast() : lambda.Args().Arg(0))
            .Condition(pushedFilter.SecondLevelOps.Cast())
        .Done();
        // clang-format on
    }

    if (!olapFilter.IsValid()) {
        YQL_CLOG(TRACE, ProviderKqp) << "KqpOlapFilter was not constructed";
        return input;
    }

    // clang-format off
    auto newOlapFilterLambda = Build<TCoLambda>(ctx.ExprCtx, input->Pos)
        .Args({"olap_filter_row"})
        .Body<TExprApplier>()
            .Apply(olapFilter.Cast())
            .With(lambda.Args().Arg(0), "olap_filter_row")
            .Build()
        .Done();
    // clang-format on
    YQL_CLOG(TRACE, ProviderKqp) << "Pushed OLAP lambda: " << KqpExprToPrettyString(newOlapFilterLambda, ctx.ExprCtx);

    // Preserve the original ID-bound predicate for statistics.
    auto originalPredicate = read->OriginalPredicate;
    if (!originalPredicate) {
        originalPredicate = filter->GetFilterExpression();
    }
    auto newRead =
        MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), read->StorageType, read->TableCallable, newOlapFilterLambda.Ptr(), read->Limit,
                               read->RangeInfo, originalPredicate, read->SortDir, read->Props, read->Pos, read->RequireSortedRead);
    if (IsValidPredicateToKeep(remainingFilter)) {
        // Part of the predicate could not be pushed down and the remaining TOpFilter survives.
        return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(remainingFilter.Cast().Ptr(), &ctx.ExprCtx, &props), true);
    }

    // The whole predicate was pushed in and no TOpFilter remains.
    return newRead;
}
} // namespace NKikimr::NKqp
