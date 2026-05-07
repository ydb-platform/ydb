#include "yql_sqlselect.h"

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

TExprNode::TPtr ExpandSqlWindowCall(
    const TExprNode::TPtr& call,
    TExprNode::TPtr listType,
    TExprNode::TPtr keyExtractor,
    TAggsRewriter rewrite,
    TExprContext& ctxExpr,
    TTypeAnnotationContext& ctxTypes)
{
    YQL_ENSURE(call->IsCallable({"PgWindowCall", "YqlWin"}));
    const bool isYql = call->IsCallable("YqlWin");

    TString name(call->Child(0)->Content());
    SubstGlobal(name, "_", "");

    const size_t argsOffset = isYql ? 4 : 3;
    YQL_ENSURE(argsOffset <= call->ChildrenSize());
    const size_t argsCount = call->ChildrenSize() - argsOffset;
    const auto argAt = [&](size_t index) -> TExprNode::TPtr {
        return call->Child(argsOffset + index);
    };

    if (name == "rownumber" && argsCount == 0) {
        // clang-format off
        return ctxExpr.Builder(call->Pos())
            .Callable("RowNumber")
                .Add(0, std::move(listType))
            .Seal()
            .Build();
        // clang-format on
    }

    if (name == "cumedist" && argsCount == 0) {
        // clang-format off
        return ctxExpr.Builder(call->Pos())
            .Callable("CumeDist")
                .Add(0, std::move(listType))
                .List(1)
                    .List(0)
                        .Atom(0, "ansi")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    }

    if (name == "ntile" && argsCount == 1) {
        TExprNode::TPtr arg = argAt(0);

        if (!isYql) {
            // clang-format off
            arg = ctxExpr.Builder(arg->Pos())
                .Callable("Unwrap")
                    .Callable(0, "FromPg")
                        .Add(0, std::move(arg))
                    .Seal()
                .Seal()
                .Build();
            // clang-format on
        }

        // clang-format off
        return ctxExpr.Builder(call->Pos())
            .Callable("NTile")
                .Add(0, std::move(listType))
                .Add(1, std::move(arg))
            .Seal()
            .Build();
        // clang-format on
    }

    if ((name == "rank" || name == "denserank" || name == "percentrank") &&
        ((isYql && argsCount <= 1) || (!isYql && argsCount == 0)))
    {
        TStringBuf callable;
        if (name == "rank") {
            callable = "Rank";
        } else if (name == "denserank") {
            callable = "DenseRank";
        } else if (name == "percentrank") {
            callable = "PercentRank";
        } else {
            YQL_ENSURE(false, "unexpected " << name);
        }

        // clang-format off
        return ctxExpr.Builder(call->Pos())
            .Callable(callable)
                .Add(0, std::move(listType))
                .Add(1, keyExtractor)
                .List(2)
                    .List(0)
                        .Atom(0, "ansi")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    }

    if ((name == "lead" || name == "lag") &&
        ((isYql && argsCount <= 2) || (!isYql && argsCount == 1)))
    {
        TExprNode::TPtr row = ctxExpr.NewArgument(call->Pos(), "row");

        TExprNode::TPtr arg = argAt(0);
        arg = rewrite(arg, row);

        TExprNode::TPtr offset = (argsCount == 2) ? argAt(1) : nullptr;

        TExprNode::TPtr extractor = ctxExpr.NewLambda(
            arg->Pos(), ctxExpr.NewArguments(call->Pos(), {row}), std::move(arg));

        if (offset) {
            // clang-format off
            return ctxExpr.Builder(call->Pos())
                .Callable(name == "lead" ? "Lead" : "Lag")
                    .Add(0, std::move(listType))
                    .Add(1, std::move(extractor))
                    .Add(2, std::move(offset))
                .Seal()
                .Build();
            // clang-format on
        }

        // clang-format off
        return ctxExpr.Builder(call->Pos())
            .Callable(name == "lead" ? "Lead" : "Lag")
                .Add(0, std::move(listType))
                .Add(1, std::move(extractor))
            .Seal()
            .Build();
        // clang-format on
    }

    if (((name == "firstvalue" || name == "lastvalue") && argsCount == 1) ||
        (name == "nthvalue" && argsCount == 2))
    {
        TString traitName;
        if (name == "firstvalue") {
            traitName = "first_value";
        } else if (name == "lastvalue") {
            traitName = "last_value";
        } else if (name == "nthvalue") {
            traitName = "nth_value";
        } else {
            YQL_ENSURE(false, "unexpected " << name);
        }

        TString path = "/lib/yql/window.yqls";
        TString binding = traitName + "_traits_factory";

        TExprNode::TPtr factory = ImportDeeplyCopied(
            call->Pos(ctxExpr),
            path, binding, ctxExpr, ctxTypes);
        YQL_ENSURE(factory);

        TExprNode::TPtr row = ctxExpr.NewArgument(call->Pos(), "row");

        TExprNode::TPtr arg = argAt(0);
        arg = rewrite(arg, row);

        TExprNode::TPtr extractor = ctxExpr.NewLambda(
            arg->Pos(), ctxExpr.NewArguments(call->Pos(), {row}), std::move(arg));

        TExprNode::TPtr traits;
        if (argsCount == 2) {
            TExprNode::TPtr offset = argAt(1);

            if (!isYql) {
                // clang-format off
                offset = ctxExpr.Builder(offset->Pos())
                    .Callable("FromPg")
                        .Add(0, std::move(offset))
                    .Seal()
                    .Build();
                // clang-format on
            }

            // clang-format off
            traits = ctxExpr.Builder(factory->Pos())
                .Apply(std::move(factory))
                    .With(0, std::move(listType))
                    .With(1, std::move(extractor))
                    .With(2, std::move(offset))
                .Seal()
                .Build();
            // clang-format on
        } else {
            // clang-format off
            traits = ctxExpr.Builder(factory->Pos())
                .Apply(std::move(factory))
                    .With(0, std::move(listType))
                    .With(1, std::move(extractor))
                .Seal()
                .Build();
            // clang-format on
        }

        ctxExpr.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        auto status = ExpandApplyNoRepeat(traits, traits, ctxExpr);
        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

        return traits;
    }

    ctxExpr.AddError(TIssue(
        call->Pos(ctxExpr),
        TStringBuilder()
            << "A window function " << name
            << "with " << argsCount << " arguments "
            << "is not yet supported"));
    return nullptr;
}

} // namespace NYql
