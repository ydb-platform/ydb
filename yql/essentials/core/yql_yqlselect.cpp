#include "yql_yqlselect.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_module_helpers.h>

namespace NYql {

TExprNode::TPtr ExpandYqlTraitsFactory(
    const TExprNode::TPtr& factory,
    TExprNode::TPtr listType,
    TExprNode::TPtr extractor,
    TExprContext& ctxExpr,
    TTypeAnnotationContext& ctxTypes)
{
    TString name(factory->Child(0)->Content());

    TString module;
    if (factory->IsCallable("YqlAggFactory")) {
        module = "aggregate";
    } else if (factory->IsCallable("YqlWinFactory")) {
        module = "window";
    } else {
        YQL_ENSURE(false, "Unexpected " << factory->Content());
    }

    TString path = "/lib/yql/" + module + ".yqls";
    TString binding = name + "_traits_factory";

    TExprNode::TPtr traitsFactory = ImportDeeplyCopied(
        factory->Child(0)->Pos(ctxExpr),
        path, binding, ctxExpr, ctxTypes);
    YQL_ENSURE(traitsFactory);

    // clang-format off
    TExprNode::TPtr traits = ctxExpr.Builder(factory->Pos())
        .Apply(std::move(traitsFactory))
            .With(0, std::move(listType))
            .With(1, std::move(extractor))
        .Seal()
        .Build();
    // clang-format on

    ctxExpr.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    auto status = ExpandApplyNoRepeat(traits, traits, ctxExpr);
    YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

    return traits;
}

size_t DefValIndex(const TExprNode::TPtr& traits) {
    if (traits->IsCallable("AggregationTraits")) {
        return 7;
    }

    if (traits->IsCallable("WindowTraits")) {
        return 5;
    }

    YQL_ENSURE(false, "unexpected " << traits->Content());
}

TExprNode::TPtr ExpandResultType(
    const TExprNode::TPtr& traits,
    const TExprNode::TPtr& body,
    TExprContext& ctxExpr)
{
    const size_t defValIndex = DefValIndex(traits);

    // clang-format off
    TExprNode::TPtr init = ctxExpr.Builder(traits->Pos())
        .Apply(traits->Child(1))
            .With(0, std::move(body))
        .Seal()
        .Build();
    // clang-format on

    TExprNode::TPtr defVal = traits->ChildPtr(defValIndex);
    const bool isDefault = !defVal->IsCallable("Null");

    TExprNode::TPtr finish;
    if (!isDefault) {
        // clang-format off
        finish = ctxExpr.Builder(traits->Pos())
            .Apply(traits->Child(defValIndex - 1))
                .With(0, std::move(init))
            .Seal()
            .Build();
        // clang-format on
    } else if (defVal->IsLambda()) {
        ctxExpr.AddError(TIssue(
            traits->Pos(ctxExpr),
            TStringBuilder()
                << "An aggregation with a lambda DefVal "
                << "is not yet supported"));
        return nullptr;
    } else {
        finish = std::move(defVal);
    }

    // clang-format off
    return ctxExpr.Builder(traits->Pos())
        .Callable("TypeOf")
            .Add(0, std::move(finish))
        .Seal()
        .Build();
    // clang-format on
}

} // namespace NYql
