#include "kqp_rules_include.h"

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

bool IsSuitableToApplyPeephole(const TIntrusivePtr<IOperator>& input) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto lambda = TCoLambda(filter->FilterExpr.Node);
    auto peepholeIsNeeded = [&](const TExprNode::TPtr& node) -> bool {
        // Here is a list of Callables for which peephole is needed.
        if (node->IsCallable({"SqlIn"})) {
            return true;
        }
        return false;
    };

    return !!FindNode(lambda.Body().Ptr(), peepholeIsNeeded);
}

}

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TPeepholePredicate::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(props);
    if (!IsSuitableToApplyPeephole(input)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto lambda = TCoLambda(filter->FilterExpr.Node);
    TVector<const TTypeAnnotationNode*> argTypes{lambda.Args().Arg(0).Ptr()->GetTypeAnn()};
    // Closure an original predicate, we cannot call `Peephole` for free args.
    // clang-format off
    auto predicateClosure = Build<TKqpPredicateClosure>(ctx.ExprCtx, input->Pos)
        .Lambda<TCoLambda>()
            .Args({"arg"})
            .Body<TExprApplier>()
                .Apply(lambda)
                .With(lambda.Args().Arg(0), "arg")
            .Build()
        .Build()
        .ArgsType(ExpandType(input->Pos, *ctx.ExprCtx.MakeType<TTupleExprType>(argTypes), ctx.ExprCtx))
    .Done();
    // clang-format on
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Before peephole: " << KqpExprToPrettyString(predicateClosure, ctx.ExprCtx);

    TExprNode::TPtr afterPeephole;
    bool hasNonDeterministicFunctions;
    // Using a special PeepholeTypeAnnTransformer.
    if (const auto status = PeepHoleOptimizeNode(predicateClosure.Ptr(), afterPeephole, ctx.ExprCtx, ctx.TypeCtx, &(ctx.PeepholeTypeAnnTransformer),
                                                 hasNonDeterministicFunctions);
        status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(ERROR, ProviderKqp) << "[NEW RBO] Peephole failed with status: " << status << Endl;
        return input;
    }
    Y_ENSURE(afterPeephole);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] After peephole: " << KqpExprToPrettyString(TExprBase(afterPeephole), ctx.ExprCtx);

    auto lambdaAfterPeephole = TExprBase(afterPeephole).Cast<TKqpPredicateClosure>().Lambda();
    // clang-format off
    auto newLambda = Build<TCoLambda>(ctx.ExprCtx, input->Pos)
        .Args({"arg"})
        .Body<TExprApplier>()
            .Apply(lambdaAfterPeephole.Body())
            .With(lambdaAfterPeephole.Args().Arg(0), "arg")
        .Build()
    .Done().Ptr();
    // clang-format on

    auto newFilterExpr = TExpression(newLambda, filter->FilterExpr.Ctx, filter->FilterExpr.PlanProps);
    return MakeIntrusive<TOpFilter>(filter->GetInput(), input->Pos, newFilterExpr);
}
}
}