#include "type_ann_yql.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_module_helpers.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_sqlselect.h>

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

TMaybe<IGraphTransformer::TStatus> TryFinishYqlTypeSlot(
    const TExprNode::TPtr& slot,
    const TExprNode::TPtr& input,
    TExtContext& ctx)
{
    const TTypeAnnotationNode* slotT = slot->GetTypeAnn();

    if (!slotT) {
        YQL_ENSURE(slot->Type() == TExprNode::Lambda);
        ctx.Expr.AddError(TIssue(
            slot->Pos(ctx.Expr),
            TStringBuilder() << "Unexpected lambda for type slot"));
        return IGraphTransformer::TStatus::Error;
    }

    if (slotT->GetKind() == ETypeAnnotationKind::Universal ||
        slotT->GetKind() == ETypeAnnotationKind::UniversalStruct)
    {
        input->SetTypeAnn(slot->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    if (slotT->GetKind() == ETypeAnnotationKind::Type) {
        if (!EnsureType(*slot, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(slot->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_ENSURE(!slot->IsCallable("Void"), "Void was not replaced during RebuildLambdaColumns");

    const TTypeAnnotationNode* rowType = slot->GetTypeAnn();
    if (!EnsureStructType(slot->Pos(), *rowType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    return Nothing();
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

    if (auto status = TryFinishYqlTypeSlot(input->ChildPtr(2), input, ctx)) {
        return *status;
    }

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
                .Add(0, input->ChildPtr(2))
            .Seal()
        .Seal()
        .Build();

    TExprNode::TPtr extractor = ctx.Expr.Builder(input->Pos())
        .Lambda()
            .Param("row")
            .Set(body) // extractor body defined in terms of a `row`
        .Seal()
        .Build();
    // clang-format on

    TExprNode::TPtr traits = ExpandYqlTraitsFactory(
        input->Child(0), std::move(listType), std::move(extractor), ctx.Expr, ctx.Types);

    const bool isDefault = !traits->ChildPtr(DefValIndex(traits))->IsCallable("Null");

    TExprNode::TPtr result = ExpandResultType(traits, body, ctx.Expr);
    if (!result) {
        return IGraphTransformer::TStatus::Error;
    }

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

IGraphTransformer::TStatus YqlWinFactoryWrapper(
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
        "/lib/yql/window.yqls",
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

IGraphTransformer::TStatus YqlAggWinWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    Y_UNUSED(output);

    if (!EnsureMinArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*input, 5, ctx.Expr)) {
        ctx.Expr.AddError(TIssue(
            input->Child(0)->Pos(ctx.Expr),
            TStringBuilder() << "2+ arguments are not implemented yet"));
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Child(0)->GetTypeAnn() && input->Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(0)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!input->Child(0)->IsCallable("YqlWinFactory")) {
        ctx.Expr.AddError(TIssue(
            input->Child(0)->Pos(ctx.Expr),
            TStringBuilder() << "Expected YqlWinFactory, "
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

    bool isUniversal;
    if (!EnsureAtomOrUniversal(*input->Child(1), ctx.Expr, isUniversal)) {
        ctx.Expr.AddError(TIssue(
            input->Child(1)->Pos(ctx.Expr),
            TStringBuilder() << "Expected window name"));
        return IGraphTransformer::TStatus::Error;
    }

    if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (input->Child(2)->GetTypeAnn() && input->Child(2)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(2)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!EnsureTuple(*input->Child(2), ctx.Expr)) {
        ctx.Expr.AddError(TIssue(
            input->Child(2)->Pos(ctx.Expr),
            TStringBuilder() << "Expected aggregation settings"));
        return IGraphTransformer::TStatus::Error;
    }

    const TExprNode* settings = input->Child(2);
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

            ctx.Expr.AddError(TIssue(
                ctx.Expr.GetPosition(input->Pos()),
                "distinct over window is not supported"));
            return IGraphTransformer::TStatus::Error;
        } else {
            ctx.Expr.AddError(TIssue(
                input->Pos(ctx.Expr),
                TStringBuilder() << "Unexpected setting " << content));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (auto status = TryFinishYqlTypeSlot(input->ChildPtr(3), input, ctx)) {
        return *status;
    }

    TExprNode::TPtr body = input->Child(4);
    if (body->GetTypeAnn() && body->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(body->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_ENSURE(input->ChildrenSize() <= 5);

    // clang-format off
    TExprNode::TPtr listType = ctx.Expr.Builder(input->Pos())
        .Callable("ListType")
            .Callable(0, "TypeOf")
                .Add(0, input->ChildPtr(3))
            .Seal()
        .Seal()
        .Build();

    TExprNode::TPtr extractor = ctx.Expr.Builder(input->Pos())
        .Lambda()
            .Param("row")
            .Set(body) // extractor body defined in terms of a `row`
        .Seal()
        .Build();
    // clang-format on

    TExprNode::TPtr traits = ExpandYqlTraitsFactory(
        input->Child(0), std::move(listType), std::move(extractor), ctx.Expr, ctx.Types);

    TExprNode::TPtr result = ExpandResultType(traits, body, ctx.Expr);
    if (!result) {
        return IGraphTransformer::TStatus::Error;
    }

    output = ctx.Expr.ChangeChild(*input, 3, std::move(result));
    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus YqlWinWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    if (!EnsureMinArgsCount(*input, 4, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (bool isUniversal; !EnsureAtomOrUniversal(*input->Child(0), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    } else if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (bool isUniversal; !EnsureAtomOrUniversal(*input->Child(1), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    } else if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    if (bool isUniversal; !EnsureTupleOfAtomsOrUniversal(*input->Child(2), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    } else if (isUniversal) {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    } else if (!EnsureTupleSize(*input->Child(2), 0, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (auto status = TryFinishYqlTypeSlot(input->ChildPtr(3), input, ctx)) {
        return *status;
    }

    // clang-format off
    TExprNode::TPtr listType = ctx.Expr.Builder(input->Pos())
        .Callable("ListType")
            .Callable(0, "TypeOf")
                .Add(0, input->ChildPtr(3))
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    auto rewrite = [&](TExprNode::TPtr arg, TExprNode::TPtr row) -> TExprNode::TPtr {
        Y_UNUSED(row);
        return arg;
    };

    // clang-format off
    TExprNode::TPtr keyExtractor = ctx.Expr.Builder(input->Pos())
        .Lambda()
            .Param("row")
            .Callable(0, "Void")
            .Seal()
        .Seal()
        .Build();
    // clang-format on

    if (input->Head().Content().EndsWith("rank") && input->ChildrenSize() == 4) {
        // clang-format off
        keyExtractor = ctx.Expr.Builder(keyExtractor->Pos())
            .Lambda()
                .Param("row")
                .Set(input->Child(3))
            .Seal()
            .Build();
        // clang-format on
    }

    TExprNode::TPtr resultExpr = ExpandSqlWindowCall(
        input, listType, keyExtractor, rewrite, ctx.Expr, ctx.Types);
    if (!resultExpr) {
        return IGraphTransformer::TStatus::Error;
    }

    if (resultExpr->IsCallable("WindowTraits")) {
        TExprNode::TPtr traits = resultExpr;
        TExprNode::TPtr body = input->Child(4);

        TExprNode::TPtr resultType =
            ExpandResultType(std::move(traits), std::move(body), ctx.Expr);

        // clang-format off
        resultExpr = ctx.Expr.Builder(input->Pos())
            .Callable("InstanceOf")
                .Add(0, std::move(resultType))
            .Seal()
            .Build();
        // clang-format on
    }

    // clang-format off
    TExprNode::TPtr resultType = ctx.Expr.Builder(input->Pos())
        .Callable("TypeOf")
            .Add(0, std::move(resultExpr))
        .Seal()
        .Build();
    // clang-format on

    output = ctx.Expr.ChangeChild(*input, 3, std::move(resultType));
    return IGraphTransformer::TStatus::Repeat;
}

} // namespace NYql::NTypeAnnImpl
