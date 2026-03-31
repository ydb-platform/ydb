#include "type_ann_yql.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_module_helpers.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NYql::NTypeAnnImpl {

namespace {

/*
    Consider the following YQL fragment:
    ```yql
    GROUP BY
        a,
        GROUPING SETS (
            (a, b),
            (a),
            ()
        ),
        ROLLUP (a, b)
    ```

    Here is corresponding "group_sets" option YQLs:
    ```yqls
    '(
        '( '('"0")                           )
        '( '('"0" '"1") '('"0") '()          )
        '( '()          '('"0") '('"0" '"1") )
    )
    ```

    Indexes are refered to "group_exprs" entries.
*/

template <class T>
TVector<T> IntersectionOfSorted(const TVector<T>& lhs, const TVector<T>& rhs) {
    Y_DEBUG_ABORT_UNLESS(std::ranges::is_sorted(lhs));
    Y_DEBUG_ABORT_UNLESS(std::ranges::is_sorted(rhs));

    TVector<T> sorted(Reserve(Max(lhs.size(), rhs.size())));
    std::ranges::set_intersection(lhs, rhs, std::back_inserter(sorted));
    return sorted;
}

template <class T>
TVector<T> UnionOfSorted(const TVector<T>& lhs, const TVector<T>& rhs) {
    Y_DEBUG_ABORT_UNLESS(std::ranges::is_sorted(lhs));
    Y_DEBUG_ABORT_UNLESS(std::ranges::is_sorted(rhs));

    TVector<T> sorted(Reserve(lhs.size() + rhs.size()));
    std::ranges::set_union(lhs, rhs, std::back_inserter(sorted));
    return sorted;
}

TVector<ui32> GroupingSortedNotNullIndexes(const TExprNode& grouping) {
    TVector<ui32> indexes(Reserve(grouping.ChildrenSize()));
    for (const auto& atom : grouping.Children()) {
        indexes.emplace_back(FromString<ui32>(atom->Content()));
    }

    Sort(indexes);
    return indexes;
}

TVector<ui32> GroupingSetSortedNotNullIndexes(const TExprNode& groupingSet) {
    if (groupingSet.ChildrenSize() == 0) {
        return {};
    }

    TVector<ui32> indexes = GroupingSortedNotNullIndexes(*groupingSet.Child(0));
    for (size_t i = 1; i < groupingSet.ChildrenSize(); ++i) {
        const auto& child = groupingSet.Child(i);

        TVector<ui32> grouping = GroupingSortedNotNullIndexes(*child);
        indexes = IntersectionOfSorted(indexes, grouping);
    }

    return indexes;
}

TVector<ui32> GroupingSetsSortedNotNullIndexes(const TExprNode& groupingSets) {
    TVector<ui32> indexes;
    for (const auto& child : groupingSets.Children()) {
        TVector<ui32> groupingSet = GroupingSetSortedNotNullIndexes(*child);
        indexes = UnionOfSorted(indexes, groupingSet);
    }
    return indexes;
}

} // namespace

IGraphTransformer::TStatus PromoteYqlAggOptions(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    YQL_ENSURE(0 < input->ChildrenSize());
    TExprNode::TPtr options = input->ChildPtr(input->ChildrenSize() - 1);
    if (GetSetting(*options, "yql_agg_promoted")) {
        return IGraphTransformer::TStatus::Ok;
    }

    TExprNode::TPtr groupBy = GetSetting(*options, "group_by");
    if (!groupBy) {
        groupBy = GetSetting(*options, "group_exprs");
    }

    if (groupBy && 0 < groupBy->Tail().ChildrenSize()) {
        return IGraphTransformer::TStatus::Ok;
    }

    TOptimizeExprSettings settings(&ctx.Types);
    settings.VisitChecker = [&](const TExprNode& node) {
        return !node.IsCallable({"YqlSelect"});
    };

    auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (!node->IsCallable("YqlAgg")) {
            return node;
        }

        TExprNode::TPtr options = node->ChildPtr(1);
        if (GetSetting(*options, "nokey")) {
            return node;
        }

        options = AddSetting(*options, node->Pos(), "nokey", /*value=*/nullptr, ctx);
        return ctx.ChangeChild(*node, 1, std::move(options));
    }, ctx.Expr, settings);

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    options = AddSetting(*options, options->Pos(), "yql_agg_promoted", /*value=*/nullptr, ctx.Expr);
    output = ctx.Expr.ChangeChild(*input, input->ChildrenSize() - 1, std::move(options));
    return IGraphTransformer::TStatus::Repeat;
}

TVector<TExprNode::TPtr> InferYqlGroupRefTypes(
    const TExprNode& groupExprs, const TExprNode& groupSets, TExprContext& ctx)
{
    TVector<TExprNode::TPtr> types(Reserve(groupExprs.ChildrenSize()));

    const TVector<ui32> notNulls = GroupingSetsSortedNotNullIndexes(groupSets);
    const auto isNullable = [&](ui32 index) -> bool {
        return !std::ranges::binary_search(notNulls, index);
    };

    for (ui32 i = 0; i < groupExprs.ChildrenSize(); ++i) {
        const auto& g = *groupExprs.Child(i);
        const auto& lambda = g.Tail();
        const TTypeAnnotationNode& typeAnn = *lambda.GetTypeAnn();

        TExprNode::TPtr type = ExpandType(g.Pos(), typeAnn, ctx);

        if (isNullable(i) && !typeAnn.IsOptionalOrNull()) {
            // clang-format off
            type = ctx.Builder(g.Pos())
                .Callable("OptionalType")
                    .Add(0, std::move(type))
                .Seal()
                .Build();
            // clang-format on
        }

        types.emplace_back(std::move(type));
    }

    return types;
}

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

    bool isUniversal;
    if (!EnsureAtomOrUniversal(*input->Child(0), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!ctx.Types.Modules) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
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

// See also the logic at the AggregateWrapper
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

    if (input->Child(0)->GetTypeAnn() && input->Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(0)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!input->Child(0)->IsCallable("YqlAggFactory")) {
        ctx.Expr.AddError(TIssue(
            input->Child(0)->Pos(ctx.Expr),
            TStringBuilder() << "Expected YqlAggFactory, "
                             << "but got " << input->Child(0)->Type()));
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(0)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_ENSURE(input->Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Unit);

    if (input->Child(1)->GetTypeAnn() && input->Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(1)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!EnsureTuple(*input->Child(1), ctx.Expr)) {
        ctx.Expr.AddError(TIssue(
            input->Child(1)->Pos(ctx.Expr),
            TStringBuilder() << "Expected aggregation settings"));
        return IGraphTransformer::TStatus::Error;
    }

    const TExprNode* settings = input->Child(1);
    for (const auto& setting : settings->Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isUniversal;
        if (!EnsureAtomOrUniversal(setting->Head(), ctx.Expr, isUniversal)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        TStringBuf content = setting->Head().Content();
        if (content == "distinct") {
            if (!EnsureTupleSize(*setting, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else if (content == "nokey") {
            if (!EnsureTupleSize(*setting, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            ctx.Expr.AddError(TIssue(
                input->Pos(ctx.Expr),
                TStringBuilder() << "Unexpected setting " << content));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (input->Child(2)->GetTypeAnn() && input->Child(2)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(2)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
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

    const TString name(input->Child(0)->Child(0)->Content());
    TExprNode::TPtr traitsFactory = ImportDeeplyCopied(
        input->Child(0)->Child(0)->Pos(ctx.Expr),
        "/lib/yql/aggregate.yqls",
        name + "_traits_factory",
        ctx.Expr,
        ctx.Types);
    YQL_ENSURE(traitsFactory);

    TExprNode::TPtr body = input->Child(3);
    if (body->GetTypeAnn() && body->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(body->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

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

    TExprNode::TPtr defVal = traits->ChildPtr(7);
    const bool isDefault = !defVal->IsCallable("Null");

    TExprNode::TPtr finish;
    if (!isDefault) {
        // clang-format off
        finish = ctx.Expr.Builder(input->Pos())
            .Apply(traits->Child(6))
                .With(0, std::move(init))
            .Seal()
            .Build();
        // clang-format on
    } else if (defVal->IsLambda()) {
        ctx.Expr.AddError(TIssue(
            input->Pos(ctx.Expr),
            TStringBuilder()
                << "An aggregation '" << name << "'"
                << "with a lambda DefVal is not yet supported"));
        return IGraphTransformer::TStatus::Error;
    } else {
        finish = std::move(defVal);
    }

    // clang-format off
    TExprNode::TPtr result = ctx.Expr.Builder(input->Pos())
        .Callable("TypeOf")
            .Add(0, std::move(finish))
        .Seal()
        .Build();
    // clang-format on

    if (!isDefault && GetSetting(*settings, "nokey")) {
        // clang-format off
        result = ctx.Expr.Builder(input->Pos())
            .Callable("MatchType")
                .Add(0, result)
                .Atom(1, "Optional")
                .Lambda(2)
                    .Set(result)
                .Seal()
                .Lambda(3)
                    .Callable("OptionalType")
                        .Add(0, result)
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        // clang-format on
    }

    output = ctx.Expr.ChangeChild(*input, 2, std::move(result));
    return IGraphTransformer::TStatus::Repeat;
}

} // namespace NYql::NTypeAnnImpl
