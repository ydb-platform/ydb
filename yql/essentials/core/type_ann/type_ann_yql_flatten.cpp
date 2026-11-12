#include "type_ann_yql_flatten.h"

#include "type_ann_sql.h"
#include "type_ann_yql.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <algorithm>
#include <iterator>

namespace NYql::NTypeAnnImpl {
namespace {

const TTypeAnnotationNode* TryGetYqlFlattenItemType(const TTypeAnnotationNode& type) {
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Optional:
            return type.Cast<TOptionalExprType>()->GetItemType();
        case ETypeAnnotationKind::List:
            return type.Cast<TListExprType>()->GetItemType();
        default:
            return nullptr;
    }
}

struct TYqlLambdaColumns {
    TInputs Inputs;
    THashSet<TString> Aliases;
    TNodeSet Sublinks;
    bool IsUniversal = false;
    bool RebuildSublinks = false;
};

IGraphTransformer::TStatus RebuildYqlLambdaSubLinks(
    const TExprNode::TPtr& lambda, TExprNode::TPtr& output, TYqlLambdaColumns& columns, TExtContext& ctx)
{
    const auto& input = columns.Inputs.front();
    TVector<const TItemExprType*> items;
    for (const auto* item : input.Type->GetItems()) {
        items.push_back(AddAlias(input.Alias, item, ctx.Expr));
    }
    auto type = ctx.Expr.MakeType<TStructExprType>(items);
    TExprNode::TPtr body;
    auto status = RebuildSubLinks(lambda->TailPtr(), body, columns.Sublinks, columns.Inputs,
                                  ExpandType(lambda->Pos(), *type, ctx.Expr), ctx);
    if (status.Level == IGraphTransformer::TStatus::Error) {
        return status;
    }
    output = ctx.Expr.ChangeChild(*lambda, 1, std::move(body));
    return IGraphTransformer::TStatus::Repeat;
}

TExprNode::TPtr LowerYqlLambdaColumnRef(
    const TExprNode::TPtr& node, const TExprNode::TPtr& argument,
    const TYqlLambdaColumns& columns, TExprContext& expr)
{
    if (!node->IsCallable({"YqlColumnRef", "YqlColumnRefOrType"})) {
        return node;
    }
    auto column = BuildSqlColumnRefMember(node, argument, columns.Inputs, columns.Aliases, expr);
    return node->IsCallable("YqlColumnRefOrType")
        ? expr.NewCallable(node->Pos(), "YqlColumnOrType", {std::move(column), node->TailPtr()})
        : column;
}

IGraphTransformer::TStatus LowerYqlLambdaColumns(
    const TExprNode::TPtr& lambda, TExprNode::TPtr& output,
    const TYqlLambdaColumns& columns, TExtContext& ctx)
{
    TOptimizeExprSettings settings(nullptr);
    settings.VisitChecker = [](const TExprNode& node) {
        return !node.IsCallable({"YqlSubLink", "PgSubLink", "YqlSelect", "PgSelect"});
    };
    TExprNode::TPtr body;
    auto status = OptimizeExpr(lambda->TailPtr(), body, [&](const TExprNode::TPtr& node, TExprContext& expr) {
        return LowerYqlLambdaColumnRef(node, lambda->Head().HeadPtr(), columns, expr);
    }, ctx.Expr, settings);
    if (status.Level == IGraphTransformer::TStatus::Error) {
        return status;
    }
    if (body != lambda->TailPtr()) {
        output = ctx.Expr.ChangeChild(*lambda, 1, std::move(body));
        return IGraphTransformer::TStatus::Repeat;
    }
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus ResolveYqlLambdaColumns(
    const TExprNode::TPtr& lambda, TStringBuf alias, const TStructExprType& rowType,
    bool caseSensitive, TExprNode::TPtr& output, bool& isUniversal, TExtContext& ctx)
{
    output = lambda;
    TYqlLambdaColumns columns;
    columns.Inputs = {{.Alias = TString(alias), .Type = &rowType,
                      .Priority = TInput::Current, .CaseSensitive = caseSensitive}};
    if (alias) {
        columns.Aliases.insert(TString(alias));
    }
    if (!ScanSqlLambdaColumns(lambda, columns.Inputs, columns.Aliases, columns.Sublinks,
                              columns.IsUniversal, columns.RebuildSublinks, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }
    isUniversal = columns.IsUniversal;
    if (isUniversal) {
        return IGraphTransformer::TStatus::Ok;
    }
    if (columns.RebuildSublinks) {
        return RebuildYqlLambdaSubLinks(lambda, output, columns, ctx);
    }
    return LowerYqlLambdaColumns(lambda, output, columns, ctx);
}

TExprNode::TPtr ReplaceYqlFromFlatten(
    const TExprNode& input, const TExprNode& fromOption, const TExprNode& source,
    TExprNode::TPtr newFlatten, TExprContext& ctx)
{
    const TString settingName = newFlatten->IsCallable("YqlFlattenBy") ? "flatten_by" : "flatten_columns";
    const auto flattenOption = GetSetting(*source.Child(3), settingName);
    YQL_ENSURE(flattenOption);
    auto newSource = ctx.ChangeChild(source, 3,
        ReplaceSetting(*source.Child(3), ctx.ChangeChild(*flattenOption, 1, std::move(newFlatten)), ctx));
    const auto& data = fromOption.Tail();
    auto newData = data.ChildrenList();
    const auto sourceIndex = std::find(newData.begin(), newData.end(), &source) - newData.begin();
    YQL_ENSURE(sourceIndex < std::ssize(newData));
    newData[sourceIndex] = std::move(newSource);
    auto newOptions = ReplaceSetting(input.Head(),
        ctx.ChangeChild(fromOption, 1, ctx.NewList(data.Pos(), std::move(newData))), ctx);
    return ctx.ChangeChild(input, 0, std::move(newOptions));
}

IGraphTransformer::TStatus UpdateFlattenRowType(
    const TExprNode& lambda, const TStructExprType*& rowType, TExprContext& ctx)
{
    const auto* type = TryGetYqlFlattenItemType(*lambda.GetTypeAnn());
    if (!type) {
        ctx.AddError(TIssue(ctx.GetPosition(lambda.Pos()), "Expected list or optional flatten result"));
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureStructType(lambda.Pos(), *type, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }
    rowType = type->Cast<TStructExprType>();
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus AnnotateFlattenLambda(
    const TExprNode::TPtr& setting, ui32 index, const TStructExprType*& rowType,
    TExprNode::TPtr& newSetting, bool& isUniversal, TExprContext& ctx)
{
    auto lambda = setting->ChildPtr(index);
    if (!UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (lambda != setting->ChildPtr(index)) {
        newSetting = ctx.ChangeChild(*setting, index, std::move(lambda));
        return IGraphTransformer::TStatus::Repeat;
    }
    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }
    isUniversal = lambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal;
    if (isUniversal) {
        return IGraphTransformer::TStatus::Ok;
    }
    return UpdateFlattenRowType(*lambda, rowType, ctx);
}

IGraphTransformer::TStatus AnnotateFlattenLambdas(
    const TExprNode::TPtr& setting, const TStructExprType*& rowType,
    TExprNode::TPtr& newSetting, TExprContext& ctx)
{
    for (ui32 index = 0; index < setting->ChildrenSize(); ++index) {
        if (setting->Child(index)->IsCallable("Void")) {
            continue;
        }
        bool isUniversal = false;
        auto status = AnnotateFlattenLambda(setting, index, rowType, newSetting, isUniversal, ctx);
        if (status.Level != IGraphTransformer::TStatus::Ok || isUniversal) {
            return status;
        }
    }
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus AnnotateYqlFlatten(
    const TExprNode::TPtr& setting, TStringBuf alias, const TStructExprType*& rowType,
    bool caseSensitive, TExprNode::TPtr& newSetting, TExtContext& ctx)
{
    newSetting = setting;
    if (AnyOf(setting->Children(), [](const auto& child) {
            return child->GetTypeAnn() && child->GetTypeAnn()->HasUniversal();
        })) {
        return IGraphTransformer::TStatus::Ok;
    }
    if (setting->IsCallable("YqlFlattenBy") && !setting->Head().IsCallable("Void")) {
        TExprNode::TPtr lambda;
        bool isUniversal = false;
        auto status = ResolveYqlLambdaColumns(setting->HeadPtr(), alias, *rowType, caseSensitive, lambda, isUniversal, ctx);
        if (lambda != setting->HeadPtr()) {
            newSetting = ctx.Expr.ChangeChild(*setting, 0, std::move(lambda));
        }
        if (status.Level != IGraphTransformer::TStatus::Ok || isUniversal) {
            return status;
        }
    }
    return AnnotateFlattenLambdas(setting, rowType, newSetting, ctx.Expr);
}

} // namespace

TExprNode::TPtr GetYqlFlattenSetting(const TExprNode& from) {
    if (from.ChildrenSize() < 4) {
        return nullptr;
    }
    if (auto columns = GetSetting(*from.Child(3), "flatten_columns")) {
        return columns->TailPtr();
    }
    auto by = GetSetting(*from.Child(3), "flatten_by");
    return by ? by->TailPtr() : nullptr;
}

const TStructExprType* GetYqlFlattenRowType(const TExprNode& setting) {
    const TTypeAnnotationNode* type = setting.Tail().GetTypeAnn();
    YQL_ENSURE(type, "Expected a typed flatten lambda");
    const auto* itemType = TryGetYqlFlattenItemType(*type);
    YQL_ENSURE(itemType, "Expected list or optional flatten result");
    return itemType->Cast<TStructExprType>();
}

TMaybe<IGraphTransformer::TStatus> AnnotateYqlFromFlatten(
    const TExprNode::TPtr& input, TExprNode::TPtr& output,
    const TExprNode& fromOption, const TExprNode& source, TInput& sourceType, TExtContext& ctx)
{
    const auto flatten = GetYqlFlattenSetting(source);
    if (!flatten) {
        return Nothing();
    }
    TExprNode::TPtr newFlatten;
    auto status = AnnotateYqlFlatten(flatten, sourceType.Alias, sourceType.Type, sourceType.CaseSensitive, newFlatten, ctx);
    if (newFlatten != flatten) {
        YQL_ENSURE(status.Level == IGraphTransformer::TStatus::Repeat);
        output = ReplaceYqlFromFlatten(*input, fromOption, source, std::move(newFlatten), ctx.Expr);
        return IGraphTransformer::TStatus::Repeat;
    }
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }
    if (AnyOf(flatten->Children(), [](const auto& child) {
            return child->GetTypeAnn() && child->GetTypeAnn()->HasUniversal();
        })) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }
    YQL_ENSURE(flatten->Tail().GetTypeAnn(), "Flatten lambda must be typed after annotation");
    sourceType.Order.Clear();
    return Nothing();
}

} // namespace NYql::NTypeAnnImpl
