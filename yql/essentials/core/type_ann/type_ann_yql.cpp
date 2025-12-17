#include "type_ann_yql.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_module_helpers.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NYql::NTypeAnnImpl {

IGraphTransformer::TStatus YqlAggFactoryWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    Y_UNUSED(output);

    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 1, ctx.Expr)) {
        ctx.Expr.AddError(TIssue(
            input->Child(0)->Pos(ctx.Expr),
            TStringBuilder() << "Parameters are not implemented yet"));
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TExprNode::TPtr* factory = ImportFreezed(
        input->Child(0)->Pos(ctx.Expr),
        "/lib/yql/aggregate.yqls",
        TString(input->Child(0)->Content()) + "_traits_factory",
        ctx.Expr,
        ctx.Types);

    if (!factory) {
        return IGraphTransformer::TStatus::Error;
    }

    YQL_ENSURE((*factory)->IsLambda());

    const size_t expectedArgsCount = (*factory)->Head().ChildrenSize();
    YQL_ENSURE(2 <= expectedArgsCount);

    // `-1` for name, `+2` for `list_type` and `extractor`
    const size_t actualArgsCount = input->ChildrenSize() - 1 + 2;

    if (expectedArgsCount != actualArgsCount) {
        ctx.Expr.AddError(TIssue(
            input->Child(0)->Pos(ctx.Expr),
            TStringBuilder() << "Expected " << expectedArgsCount << " arguments, "
                             << "but got " << actualArgsCount));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TUnitExprType>());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus YqlAggWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    Y_UNUSED(output);

    if (!EnsureMinArgsCount(*input, 4, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 4, ctx.Expr)) {
        ctx.Expr.AddError(TIssue(
            input->Child(0)->Pos(ctx.Expr),
            TStringBuilder() << "2+ arguments are not implemented yet"));
        return IGraphTransformer::TStatus::Error;
    }

    if (!input->Child(0)->IsCallable("YqlAggFactory")) {
        ctx.Expr.AddError(TIssue(
            input->Child(0)->Pos(ctx.Expr),
            TStringBuilder() << "Expected YqlAggFactory, "
                             << "but got " << input->Child(0)->Type()));
        return IGraphTransformer::TStatus::Error;
    }

    YQL_ENSURE(input->Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Unit);

    if (!EnsureTuple(*input->Child(1), ctx.Expr)) {
        ctx.Expr.AddError(TIssue(
            input->Child(1)->Pos(ctx.Expr),
            TStringBuilder() << "Expected aggregation settings"));
        return IGraphTransformer::TStatus::Error;
    }

    for (const auto& setting : input->Child(1)->Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(setting->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TStringBuf content = setting->Head().Content();
        if (content == "distinct") {
            if (!EnsureTupleSize(*setting, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            YQL_ENSURE(GetSetting(*input->Child(1), "distinct"));
        } else {
            ctx.Expr.AddError(TIssue(
                input->Pos(ctx.Expr),
                TStringBuilder() << "Unexpected setting " << content));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!input->Child(2)->IsCallable("Void")) {
        if (!EnsureType(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const NYql::TTypeExprType* type =
            input->Child(2)->GetTypeAnn()->Cast<TTypeExprType>();

        input->SetTypeAnn(type->GetType());
        return IGraphTransformer::TStatus::Ok;
    }

    TExprNode::TPtr traitsFactory = ImportDeeplyCopied(
        input->Child(0)->Child(0)->Pos(ctx.Expr),
        "/lib/yql/aggregate.yqls",
        TString(input->Child(0)->Child(0)->Content()) + "_traits_factory",
        ctx.Expr,
        ctx.Types);
    YQL_ENSURE(traitsFactory);

    TExprNode::TPtr body = input->Child(3);
    YQL_ENSURE(input->ChildrenSize() <= 4);

    // clang-format off
    TExprNode::TPtr listType = ctx.Expr.Builder(input->Pos())
        .Callable("ListType")
            .Callable(0, "TypeOf")
                .Atom(0, "row") // comes from an enclosing `YqlSetItem`
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    TExprNode::TPtr extractor = ctx.Expr.Builder(input->Pos())
        .Lambda()
            .Param("row")
            .Set(body) // extractor body defined in terms of a `row`
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    TExprNode::TPtr traits = ctx.Expr.Builder(input->Pos())
        .Apply(std::move(traitsFactory))
            .With(0, std::move(listType))
            .With(1, std::move(extractor))
        .Seal()
        .Build();
    // clang-format on


    ctx.Expr.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    auto status = ExpandApplyNoRepeat(traits, traits, ctx.Expr);
    YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);

    // clang-format off
    TExprNode::TPtr init = ctx.Expr.Builder(input->Pos())
        .Apply(traits->Child(1))
            .With(0, std::move(body))
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    TExprNode::TPtr finish = ctx.Expr.Builder(input->Pos())
        .Apply(traits->Child(6))
            .With(0, std::move(init))
        .Seal()
        .Build();
    // clang-format on

    // clang-format off
    TExprNode::TPtr result = ctx.Expr.Builder(input->Pos())
        .Callable("TypeOf")
            .Add(0, std::move(finish))
        .Seal()
        .Build();
    // clang-format on

    output = ctx.Expr.ChangeChild(*input, 2, std::move(result));
    return IGraphTransformer::TStatus::Repeat;
}

} // namespace NYql::NTypeAnnImpl
