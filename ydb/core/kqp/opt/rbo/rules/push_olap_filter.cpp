#include "kqp_rules_include.h"

namespace {
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

bool IsSuitableToPushPredicateToColumnTables(const TIntrusivePtr<IOperator>& input) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto predicate = TCoLambda(filter->FilterExpr.Node).Body();
    if (predicate.Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        return false;
    }

    const auto maybeRead = filter->GetInput();
    if (maybeRead->Kind != EOperator::Source){
        return false;
    }

    const auto read = CastOperator<TOpRead>(maybeRead);
    return read->GetTableStorageType() == NYql::EStorageType::ColumnStorage && !read->OlapFilterLambda && read->IsSingleConsumer();
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

TExprNode::TPtr ApplyPeephole(TExprNode::TPtr input, TExprNode::TPtr lambdaArg, TRBOContext& ctx) {
    Y_ENSURE(lambdaArg->GetTypeAnn());
    auto newArg = Build<TCoArgument>(ctx.ExprCtx, input->Pos())
        .Name("new_arg").Done().Ptr();
    newArg->SetTypeAnn(lambdaArg->GetTypeAnn());
    lambdaArg->CopyConstraints(*newArg);

    // clang-format off
    TVector<const TTypeAnnotationNode *> argTypes{lambdaArg->GetTypeAnn()};
    auto olapPredicateClosure = Build<TKqpPredicateClosure>(ctx.ExprCtx, input->Pos())
        .Lambda<TCoLambda>()
            .Args({newArg})
            .Body<TExprApplier>()
                .Apply(TExprBase(input))
                .With(TExprBase(lambdaArg), TExprBase(newArg))
            .Build()
        .Build()
        .ArgsType(ExpandType(input->Pos(), *ctx.ExprCtx.MakeType<TTupleExprType>(argTypes), ctx.ExprCtx))
    .Done();
    // clang-format on

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO OLAP FILTER] Before peephole: " << KqpExprToPrettyString(olapPredicateClosure, ctx.ExprCtx);

    TPeepholeSettings peepholeSettings;
    peepholeSettings.WithFinalStageRules = false;
    TExprNode::TPtr afterPeephole;
    bool hasNonDeterministicFunctions;
    if (const auto status = PeepHoleOptimizeNode(olapPredicateClosure.Ptr(), afterPeephole, ctx.ExprCtx, ctx.TypeCtx, &(ctx.PeepholeTypeAnnTransformer),
                                                 hasNonDeterministicFunctions);
        status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(ERROR, ProviderKqp) << "[NEW RBO OLAP FILTER] Peephole failed with status: " << status << Endl;
        Y_ENSURE(false, "Peephole failed.");
    }
    Y_ENSURE(afterPeephole);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO OLAP FILTER] After peephole: " << KqpExprToPrettyString(TExprBase(afterPeephole), ctx.ExprCtx);
    return TExprBase(afterPeephole).Cast<TKqpPredicateClosure>().Lambda().Ptr();
}

void StripAliasesFromInputType(TExprNode::TPtr input, TExprContext& ctx) {
    const TTypeAnnotationNode* type = input->GetTypeAnn();
    Y_ENSURE(type && type->GetKind() == ETypeAnnotationKind::Struct);
    const auto structType = type->Cast<TStructExprType>();
    TVector<const TItemExprType*> newItemTypes;
    for (const auto itemType : structType->GetItems()) {
        auto colName = TString(itemType->GetName());
        const auto it = colName.find(".");
        if (it != TString::npos) {
            colName = colName.substr(it + 1);
        }
        newItemTypes.push_back(ctx.MakeType<TItemExprType>(colName, itemType->GetItemType()));
    }
    input->SetTypeAnn(ctx.MakeType<TStructExprType>(newItemTypes));
}
}

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TPushOlapFilterRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(props);
    if (!ctx.KqpCtx.Config->HasOptEnableOlapPushdown()) {
        return input;
    }

    const TPushdownOptions pushdownOptions(ctx.KqpCtx.Config->GetEnableOlapScalarApply(), ctx.KqpCtx.Config->GetEnableOlapSubstringPushdown(),
                                           /*StripAliasPrefixForColumnName=*/true);
    if (!IsSuitableToPushPredicateToColumnTables(input)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput());
    const auto lambda = TCoLambda(filter->FilterExpr.Node);
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

    auto lambdaAfterRewrite = TExprBase(ApplyPeephole(predicate.Ptr(), lambda.Args().Arg(0).Ptr(), ctx)).Cast<TCoLambda>();
    lambdaArg = lambdaAfterRewrite.Args().Arg(0).Ptr();
    predicate = lambdaAfterRewrite.Body();

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
            auto lambdaAfterPeephole = TExprBase(ApplyPeephole(predicateExprHolder.ExprNode, lambdaArg, ctx)).Cast<TCoLambda>();
            auto lArg = lambdaAfterPeephole.Args().Arg(0);
            auto predicate = lambdaAfterPeephole.Body();
            TOLAPPredicateNode predicateTree;
            predicateTree.ExprNode = predicate.Ptr();
            StripAliasesFromInputType(lArg.Ptr(), ctx.ExprCtx);
            CollectPredicates(predicate, predicateTree, &lArg.Ref(), lArg.Ptr()->GetTypeAnn(), {true, pushdownOptions.PushdownSubstring});

            YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");
            auto [pushable, remaining] = SplitForPartialPushdown(predicateTree, true);
            for (const auto& predicate : pushable) {
                if (predicate.CanBePushed) {
                    auto pred = PredicatePushdown(TExprBase(predicate.ExprNode), lArg.Ref(), ctx.ExprCtx, input->Pos, pushdownOptions);
                    pushedPredicates.emplace_back(pred);
                } else {
                    auto expr = YqlApplyPushdown(TExprBase(predicate.ExprNode), lArg.Ref(), ctx.ExprCtx, pushdownOptions);
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

    // Saving original predicate for statistics.
    std::optional<TExpression> originalPredicate = read->OriginalPredicate.has_value() ? read->OriginalPredicate : filter->FilterExpr;
    const auto newRead =
        MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), read->StorageType, read->TableCallable, newOlapFilterLambda.Ptr(), read->Limit,
                               read->GetRanges(), originalPredicate, read->SortDir, read->Props, read->Pos);
    if (IsValidPredicateToKeep(remainingFilter)) {
        return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(remainingFilter.Cast().Ptr(), &ctx.ExprCtx, &props));
    }
    return newRead;
}
}
}

