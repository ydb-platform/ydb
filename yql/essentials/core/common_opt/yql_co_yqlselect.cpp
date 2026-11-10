#include "yql_co_yqlselect.h"

#include "yql_co_sqlselect.h"

#include <yql/essentials/core/type_ann/type_ann_yql.h>
#include <yql/essentials/core/type_ann/type_ann_yql_flatten.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_module_helpers.h>
#include <yql/essentials/core/yql_sqlselect.h>

namespace NYql {

namespace {

std::pair<TExprNode::TPtr, TExprNode::TPtr> BuildYqlSubLinkRows(
    TPositionHandle pos,
    const TStructExprType& rowType,
    const TString& inputAlias,
    const TExprNode::TPtr& source,
    const TExprNode::TPtr& row,
    TExprContext& ctx)
{
    TExprNode::TListType aliasedItems;
    TExprNode::TListType originalItems;
    for (const auto* item : rowType.GetItems()) {
        auto name = ctx.NewAtom(pos, item->GetName());
        auto aliased = ctx.NewAtom(pos, NTypeAnnImpl::MakeAliasedColumn(inputAlias, item->GetName()));
        // clang-format off
        aliasedItems.push_back(ctx.Builder(pos)
            .List()
                .Add(0, aliased)
                .Callable(1, "Member")
                    .Add(0, source)
                    .Add(1, name)
                .Seal()
            .Seal()
            .Build());
        originalItems.push_back(ctx.Builder(pos)
            .List()
                .Add(0, name)
                .Callable(1, "Member")
                    .Add(0, row)
                    .Add(1, aliased)
                .Seal()
            .Seal()
            .Build());
        // clang-format on
    }
    return {
        ctx.NewCallable(pos, "AsStruct", std::move(aliasedItems)),
        ctx.NewCallable(pos, "AsStruct", std::move(originalItems))};
}

TExprNode::TPtr AdaptYqlLambdaInput(
    TPositionHandle pos,
    TExprNode::TPtr lambda,
    const TExprNode::TPtr& inputLambda,
    TExprContext& ctx)
{
    if (!inputLambda) {
        return lambda;
    }
    auto row = ctx.NewArgument(pos, "row");
    // clang-format off
    auto renamedRow = ctx.Builder(pos)
        .Apply(inputLambda)
            .With(0, row)
        .Seal()
        .Build();
    auto body = ctx.Builder(pos)
        .Apply(lambda)
            .With(0, std::move(renamedRow))
        .Seal()
        .Build();
    // clang-format on
    return ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> PrepareYqlLambdaSubLinkInput(
    TPositionHandle pos,
    TExprNode::TPtr list,
    const TExprNode::TPtr& lambda,
    const TExprNode::TPtr& row,
    const TString& inputAlias,
    const TExprNode::TPtr& inputLambda,
    TExprContext& ctx)
{
    const auto argumentType = lambda->Head().Head().GetTypeAnn();
    YQL_ENSURE(argumentType, "Expected a typed lambda argument");
    auto source = ctx.NewArgument(pos, "source");
    const auto* rowType = argumentType->Cast<TStructExprType>();
    auto [aliasedRow, originalRow] = BuildYqlSubLinkRows(pos, *rowType, inputAlias, source, row, ctx);
    auto prefixLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {source}), std::move(aliasedRow));
    prefixLambda = AdaptYqlLambdaInput(pos, std::move(prefixLambda), inputLambda, ctx);
    // clang-format off
    list = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, list)
            .Add(1, std::move(prefixLambda))
        .Seal()
        .Build();
    auto body = ctx.Builder(pos)
        .Apply(lambda).With(0, std::move(originalRow)).Seal()
        .Build();
    // clang-format on
    return {std::move(body), std::move(list)};
}

std::pair<TExprNode::TPtr, TExprNode::TPtr> RewriteYqlLambdaSubLinks(
    TPositionHandle pos,
    TExprNode::TPtr list,
    TExprNode::TPtr lambda,
    const TString& inputAlias,
    const TExprNode::TPtr& inputLambda,
    TExprContext& ctx,
    TOptimizeContext& optCtx)
{
    const auto originalBody = lambda->TailPtr();
    const auto sublink = FindNode(originalBody, [](const TExprNode::TPtr& node) {
        return node->IsCallable({"PgSubLink", "YqlSubLink"});
    });
    if (!sublink) {
        return {AdaptYqlLambdaInput(pos, std::move(lambda), inputLambda, ctx), std::move(list)};
    }

    auto row = ctx.NewArgument(pos, "row");
    TExprNode::TPtr body;
    std::tie(body, list) = PrepareYqlLambdaSubLinkInput(pos, std::move(list), lambda, row, inputAlias, inputLambda, ctx);

    auto [sublinks, originalSublinks] = GatherSubLinksWithOriginal(body, originalBody);
    if (!sublinks.empty()) {
        std::tie(body, list) = RewriteSubLinksPartial(pos, list, body, row, sublinks, originalSublinks, {inputAlias}, {list}, ctx, optCtx, /*isYql=*/true);
    }
    return {ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body)), std::move(list)};
}

} // namespace

TExprNode::TPtr BuildYqlAggregationTraits(
    const TExprNode::TPtr& node,
    TExprNode::TPtr type,
    TExprNode::TPtr extractor,
    TExprContext& ctx,
    TOptimizeContext& optCtx)
{
    // clang-format off
    type = ctx.Builder(node->Pos())
        .Callable("ListType")
            .Add(0, std::move(type))
        .Seal()
        .Build();
    // clang-format on

    return NYql::ExpandYqlTraitsFactory(
        node->Child(0), std::move(type), std::move(extractor), ctx, *optCtx.Types);
}

TExprNode::TPtr ExpandYqlFlatten(
    TExprNode::TPtr list,
    const TExprNode& from,
    const TExprNode::TPtr& renameLambda,
    TExprContext& ctx,
    TOptimizeContext& optCtx)
{
    const auto setting = NTypeAnnImpl::GetYqlFlattenSetting(from);
    if (!setting) {
        return list;
    }
    const auto pos = setting->Pos();
    auto flattenLambda = setting->TailPtr();
    if (setting->IsCallable("YqlFlattenBy") && !setting->Head().IsCallable("Void")) {
        const TString inputAlias(from.Child(1)->Content());
        auto [lambda, rewrittenList] = RewriteYqlLambdaSubLinks(pos, list, setting->HeadPtr(), inputAlias, renameLambda, ctx, optCtx);
        list = std::move(rewrittenList);
        list = ctx.NewCallable(pos, "OrderedFlatMap", {list, std::move(lambda)});
    } else {
        flattenLambda = AdaptYqlLambdaInput(pos, std::move(flattenLambda), renameLambda, ctx);
    }
    return ctx.NewCallable(pos, "OrderedFlatMap", {list, std::move(flattenLambda)});
}

} // namespace NYql
