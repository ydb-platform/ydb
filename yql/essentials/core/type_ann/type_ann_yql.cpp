#include "type_ann_yql.h"

#include "type_ann_columnorder.h"
#include "type_ann_list.h"

#include <yql/essentials/core/issue/yql_issue.h>
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

    Indexes are referred to "group_exprs" entries.
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

bool IsEmptyGroupingSetPresent(const TExprNode& groupingSets) {
    for (const auto& component : groupingSets.Children()) {
        bool hasEmptySet = AnyOf(component->Children(), [](const auto& set) {
            return set->ChildrenSize() == 0;
        });

        if (!hasEmptySet) {
            return false;
        }
    }

    return true;
}

bool IsOptionalAggregation(const TExprNode::TPtr& options) {
    TExprNode::TPtr groupSets = GetSetting(*options, "group_sets");
    TExprNode::TPtr groupBy = GetSetting(*options, "group_by");
    if (!groupBy) {
        groupBy = GetSetting(*options, "group_exprs");
    }

    const bool hasGroupingKey = groupBy && 0 < groupBy->Tail().ChildrenSize();
    const bool hasEmptyGroupingSet = groupSets && IsEmptyGroupingSetPresent(groupSets->Tail());

    return !(hasGroupingKey && !hasEmptyGroupingSet);
}

bool IsYqlRank(const TExprNode& node) {
    return node.IsCallable("YqlWin") &&
        IsIn({"rank", "denserank", "percentrank"}, node.Head().Content());
}

TStringBuf YqlRankDisplayName(TStringBuf name) {
    if (name == "rank") {
        return "Rank";
    }
    if (name == "denserank") {
        return "DenseRank";
    }

    YQL_ENSURE(name == "percentrank");
    return "PercentRank";
}

const TExprNode* FindYqlWindow(const TExprNode& windows, TStringBuf name) {
    for (const auto& window : windows.Children()) {
        if (window->Head().Content() == name) {
            return window.Get();
        }
    }

    return nullptr;
}

// Mirrors the key shape built by BuildSingletonSortTraits and BuildSortTraits during
// common-opt expansion. This earlier type-annotation stage only has the annotated
// YqlSort nodes, so it reconstructs the resulting key type without expanding them.
const TTypeAnnotationNode* BuildYqlWindowSortKeyType(const TExprNode& sort, TExprContext& ctx) {
    auto getColumnType = [&](const TExprNode& column) -> const TTypeAnnotationNode* {
        const auto type = column.GetTypeAnn();
        if (!type) {
            return nullptr;
        }

        const auto* itemType = &*type;
        if (column.Child(3)->Content() != "last") {
            return itemType;
        }

        return ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TDataExprType>(EDataSlot::Bool), itemType});
    };

    if (sort.ChildrenSize() == 0) {
        return ctx.MakeType<TVoidExprType>();
    }

    if (sort.ChildrenSize() == 1) {
        return getColumnType(sort.Head());
    }

    TTypeAnnotationNode::TListType types;
    types.reserve(sort.ChildrenSize());
    for (const auto& column : sort.Children()) {
        const auto* type = getColumnType(*column);
        if (!type) {
            return nullptr;
        }
        types.push_back(type);
    }
    return ctx.MakeType<TTupleExprType>(std::move(types));
}

TExprNode::TPtr BuildYqlWindowSortKeyTypeWitness(const TExprNode& sort, TExprContext& ctx) {
    const auto* type = BuildYqlWindowSortKeyType(sort, ctx);
    if (!type) {
        ctx.AddError(TIssue(sort.Pos(ctx), "Expected annotated window sort key"));
        return nullptr;
    }

    // clang-format off
    return ctx.Builder(sort.Pos())
        .Callable("InstanceOf")
            .Add(0, ExpandType(sort.Pos(), *type, ctx))
        .Seal()
        .Build();
    // clang-format on
}

bool WarnYqlRankWithoutOrderBy(const TExprNode& rank, TExprContext& ctx) {
    const auto name = YqlRankDisplayName(rank.Head().Content());
    const bool hasArgument = rank.ChildrenSize() > 4;
    TIssue issue(
        rank.Pos(ctx),
        hasArgument
            ? TStringBuilder() << name << "(<expression>) is used with unordered window - the result is likely to be undefined"
            : TStringBuilder() << name << "() is used with unordered window - all rows will be considered equal to each other");
    SetIssueCode(TIssuesIds::YQL_RANK_WITHOUT_ORDER_BY, issue);
    return ctx.AddWarning(issue);
}

TExprNode::TPtr RemoveSettings(
    TExprNode::TPtr input,
    TVector<TStringBuf> settings,
    TExprContext& ctx)
{
    for (const auto setting : settings) {
        if (GetSetting(*input, setting)) {
            input = RemoveSetting(*input, setting, ctx);
        }
    }

    return input;
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

void ForEachConjunct(
    const TExprNode::TPtr& predicate,
    std::invocable<const TExprNode::TPtr&> auto f)
{
    if (!predicate->IsCallable("And")) {
        f(predicate);
        return;
    }

    for (const auto& child : predicate->Children()) {
        ForEachConjunct(child, f);
    }
}

TMaybe<std::pair<TExprNode::TPtr, TExprNode::TPtr>>
TryGetImplicitUsingEquality(const TExprNode::TPtr& conjunct) {
    if (!conjunct->IsCallable("==")) {
        return Nothing();
    }

    const auto& lhs = conjunct->ChildPtr(0);
    const auto& rhs = conjunct->ChildPtr(1);
    if (!lhs->IsCallable("YqlColumnRef") ||
        !rhs->IsCallable("YqlColumnRef"))
    {
        return Nothing();
    }

    if (lhs->ChildrenSize() != 2 || rhs->ChildrenSize() != 2) {
        return Nothing();
    }

    if (lhs->Tail().Content() != rhs->Tail().Content()) {
        return Nothing();
    }

    return std::make_pair(lhs, rhs);
}

TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>>
GetImplicitUsingEqualities(const TExprNode::TPtr& predicate) {
    TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>> equalities;
    ForEachConjunct(predicate, [&](const TExprNode::TPtr& conjunct) {
        if (const auto eq = TryGetImplicitUsingEquality(conjunct)) {
            equalities.emplace_back(*eq);
        }
    });
    return equalities;
}

TMaybe<ui32> FindAliasIndex(
    const TInputs& inputs,
    const TVector<ui32>& indexes,
    TStringBuf alias)
{
    for (ui32 idx : indexes) {
        if (!inputs[idx].Alias.empty() && inputs[idx].Alias == alias) {
            return idx;
        }
    }

    return Nothing();
}

TString GetCorrelationAlias(const TExprNode::TPtr& ref) {
    YQL_ENSURE(ref->IsCallable("YqlColumnRef"));
    YQL_ENSURE(ref->ChildrenSize() == 2);
    return TString(ref->Head().Content());
}

IGraphTransformer::TStatus ResolveInputs(
    const TInputs& inputs,
    const TVector<ui32>& lhsIndexes,
    const TVector<ui32>& rhsIndexes,
    TExprNode::TPtr& lhsRef,
    ui32& lhsIdx,
    TExprNode::TPtr& rhsRef,
    ui32& rhsIdx,
    TExtContext& ctx)
{
    const TString lhsAlias = GetCorrelationAlias(lhsRef);
    const TString rhsAlias = GetCorrelationAlias(rhsRef);

    auto lhsInLhs = FindAliasIndex(inputs, lhsIndexes, lhsAlias);
    auto rhsInRhs = FindAliasIndex(inputs, rhsIndexes, rhsAlias);
    if (lhsInLhs && rhsInRhs) {
        lhsIdx = *lhsInLhs;
        rhsIdx = *rhsInRhs;
        return IGraphTransformer::TStatus::Ok;
    }

    auto lhsInRhs = FindAliasIndex(inputs, rhsIndexes, lhsAlias);
    auto rhsInLhs = FindAliasIndex(inputs, lhsIndexes, rhsAlias);
    if (lhsInRhs && rhsInLhs) {
        std::swap(lhsRef, rhsRef);
        lhsIdx = *rhsInLhs;
        rhsIdx = *lhsInRhs;
        return IGraphTransformer::TStatus::Ok;
    }

    if (!lhsInLhs) {
        ctx.Expr.AddError(TIssue(
            ctx.Expr.GetPosition(lhsRef->Pos()),
            TStringBuilder() << "Unknown correlation: " << lhsAlias));
    }

    if (!rhsInRhs) {
        ctx.Expr.AddError(TIssue(
            ctx.Expr.GetPosition(rhsRef->Pos()),
            TStringBuilder() << "Unknown correlation: " << rhsAlias));
    }

    return IGraphTransformer::TStatus::Error;
}

IGraphTransformer::TStatus TryToFindItemI(
    TPositionHandle position,
    const TInput& input,
    TStringBuf name,
    ui32& index,
    TExtContext& ctx)
{
    auto maybe = input.Type->FindItemI(name, /*isVirtual=*/nullptr);
    if (!maybe) {
        ctx.Expr.AddError(TIssue(
            ctx.Expr.GetPosition(position),
            TStringBuilder()
                << "Unknown column: " << name
                << " in correlation name: " << input.Alias));
        return IGraphTransformer::TStatus::Error;
    }

    index = *maybe;
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus TryToUsingEntry(
    TExprNode::TPtr lhsRef,
    TExprNode::TPtr rhsRef,
    const TInputs& groupInputs,
    const TVector<ui32>& lhsIndexes,
    const TVector<ui32>& rhsIndexes,
    std::pair<TString, TString>& entry,
    TExtContext& ctx)
{
    ui32 lhsIdx = 0;
    ui32 rhsIdx = 0;
    if (auto status = ResolveInputs(
            groupInputs,
            lhsIndexes,
            rhsIndexes,
            lhsRef, lhsIdx,
            rhsRef, rhsIdx,
            ctx);
        status != IGraphTransformer::TStatus::Ok)
    {
        return status;
    }

    const TStringBuf lhsName = lhsRef->Tail().Content();
    const TStringBuf rhsName = rhsRef->Tail().Content();

    const auto& lhsInput = groupInputs[lhsIdx];
    const auto& rhsInput = groupInputs[rhsIdx];

    ui32 rhsPos;
    if (auto status = TryToFindItemI(rhsRef->Pos(), rhsInput, rhsName, rhsPos, ctx);
        status != IGraphTransformer::TStatus::Ok)
    {
        return status;
    }

    ui32 lhsPos;
    if (auto status = TryToFindItemI(lhsRef->Pos(), lhsInput, lhsName, lhsPos, ctx);
        status != IGraphTransformer::TStatus::Ok)
    {
        return status;
    }

    const auto& lhsItems = lhsInput.Type->GetItems();
    const auto& rhsItems = rhsInput.Type->GetItems();

    entry = std::make_pair(
        MakeAliasedColumn(lhsInput.Alias, lhsItems[lhsPos]->GetName()),
        MakeAliasedColumn(rhsInput.Alias, rhsItems[rhsPos]->GetName()));

    return IGraphTransformer::TStatus::Ok;
}

TVector<TYqlResultItemLabel> YqlSetItemLabels(const TExprNode::TPtr& input) {
    YQL_ENSURE(input->IsCallable("YqlSetItem"));

    const auto setting = GetSetting(input->Head(), "result");
    YQL_ENSURE(setting, "Expected result in YqlSetItem");

    const auto result = setting->TailPtr();

    TVector<TYqlResultItemLabel> labels(Reserve(result->ChildrenSize()));
    for (const auto& item : result->Children()) {
        auto label = item->Child(0);

        TString name(label->Content());

        bool isSynthetic = (3 < item->ChildrenSize() &&
                            HasSetting(*item->Child(2), "synthetic"));

        bool isShadowingWarning = (3 < item->ChildrenSize() &&
                                   HasSetting(*item->Child(2), "warnShadow"));

        TYqlResultItemLabel x;
        x.Position = label->Pos();
        x.Content = std::move(name);
        x.IsSynthetic = isSynthetic;
        x.IsShadowingWarning = isShadowingWarning;

        labels.emplace_back(std::move(x));
    }

    return labels;
}

TYqlColumnOrder ToColumnOrder(TVector<TYqlResultItemLabel> labels) {
    TYqlColumnOrder order(Reserve(labels.size()));
    for (auto& label : labels) {
        order.push_back({
            .Content = std::move(label.Content),
            .IsSynthetic = label.IsSynthetic,
        });
    }
    return order;
}

TString YqlWithoutName(TStringBuf source, TStringBuf name) {
    return TStringBuilder() << source << "." << name;
}

TString YqlWithoutItemName(TStringBuf itemName, bool isJoin) {
    TString alias;
    const TStringBuf name = RemoveAlias(itemName, alias);
    return isJoin && !alias.empty() ? YqlWithoutName(alias, name) : TString(name);
}

TString YqlWithoutColumnName(const TExprNode& column, bool isJoin) {
    return isJoin
        ? YqlWithoutName(column.Head().Content(), column.Tail().Content())
        : TString(column.Tail().Content());
}

void ReportMissingYqlWithoutItem(
    TStringBuf name,
    TPositionHandle position,
    const TVector<const TItemExprType*>& items,
    bool isJoin,
    TExtContext& ctx)
{
    TVector<const TItemExprType*> namedItems(Reserve(items.size()));
    for (const auto* item : items) {
        namedItems.push_back(ctx.Expr.MakeType<TItemExprType>(
            YqlWithoutItemName(item->GetName(), isJoin), item->GetItemType()));
    }
    const auto currentType = ctx.Expr.MakeType<TStructExprType>(namedItems);
    FindOrReportMissingMember(name, position, *currentType, ctx.Expr);
}

TExprNode::TPtr GetYqlWithoutForStarResult(const TExprNode& setItem) {
    YQL_ENSURE(setItem.IsCallable("YqlSetItem"));
    const auto& settings = setItem.Head();
    const auto result = GetSetting(settings, "result");
    const bool hasStarResult = result && AnyOf(
        result->Tail().Children(), [](const auto& item) {
            return item->Tail().Tail().IsCallable("YqlStar");
        });
    return hasStarResult ? GetSetting(settings, "without") : nullptr;
}

} // namespace

bool ValidateYqlWithoutSetting(TExprNode& setting, TExprContext& ctx) {
    if (!EnsureTupleMinSize(setting, 2, ctx) ||
        !EnsureTupleMaxSize(setting, 3, ctx) ||
        !EnsureTuple(*setting.Child(1), ctx)) {
        return false;
    }

    for (const auto& column : setting.Child(1)->Children()) {
        if (!EnsureTupleSize(*column, 2, ctx) ||
            !EnsureAtom(column->Head(), ctx) ||
            !EnsureAtom(column->Tail(), ctx)) {
            return false;
        }
    }

    if (setting.ChildrenSize() == 3 &&
        (!EnsureAtom(*setting.Child(2), ctx) || setting.Child(2)->Content() != "if_exists")) {
        ctx.AddError(TIssue(ctx.GetPosition(setting.Child(2)->Pos()), "Expected if_exists"));
        return false;
    }
    return true;
}

bool IsYqlWithoutItem(TStringBuf itemName, const TExprNode& without, bool isJoin) {
    const TString normalizedItemName = YqlWithoutItemName(itemName, isJoin);
    return AnyOf(without.Child(1)->Children(), [&](const auto& column) {
        return normalizedItemName == YqlWithoutColumnName(*column, isJoin);
    });
}

IGraphTransformer::TStatus ApplyYqlWithoutToStar(
    const TExprNode& setItem,
    const TInputs& inputs,
    TVector<const TItemExprType*>& items,
    TExtContext& ctx)
{
    YQL_ENSURE(setItem.IsCallable("YqlSetItem"));
    const auto without = GetSetting(setItem.Head(), "without");
    if (!without) {
        return IGraphTransformer::TStatus::Ok;
    }

    const bool isJoin = CountIf(inputs, [](const auto& input) {
        return input.Priority == TInput::Current;
    }) > 1;
    YQL_ENSURE(
        without->ChildrenSize() == 2 || without->ChildrenSize() == 3,
        "Expected WITHOUT setting with 2 or 3 children, got " << without->ChildrenSize());
    const bool ifExists = without->ChildrenSize() == 3;
    for (const auto& column : without->Child(1)->Children()) {
        const TString name = YqlWithoutColumnName(*column, isJoin);
        const size_t itemCount = items.size();
        EraseIf(items, [&](const auto* item) {
            return YqlWithoutItemName(item->GetName(), isJoin) == name;
        });
        if (items.size() == itemCount && !ifExists) {
            ReportMissingYqlWithoutItem(name, column->Tail().Pos(), items, isJoin, ctx);
            return IGraphTransformer::TStatus::Error;
        }
    }

    return IGraphTransformer::TStatus::Ok;
}

// The executable zero-argument rank key is built later from the window ORDER BY specification.
// During lambda rebuilding, YqlWin needs only the same key type to infer legacy nullable results.
// Keep that InstanceOf witness in the internal window_expr setting so Rank() stays zero-argument for
// downstream consumers such as YDB RBO; Rank(expr) continues to carry and use its explicit key.
TExprNode::TPtr RebuildLambdaYqlWin(
    const TExprNode::TPtr& node,
    const TExprNode::TPtr& row,
    const TExprNode* windows,
    TExprContext& ctx)
{
    auto children = node->ChildrenList();
    children[3] = row;

    if (!IsYqlRank(*node) || !windows) {
        return ctx.ChangeChildren(*node, std::move(children));
    }

    const auto* window = FindYqlWindow(*windows, node->Child(1)->Content());
    if (!window) {
        ctx.AddError(TIssue(
            node->Pos(ctx),
            TStringBuilder() << "Not found window name: " << node->Child(1)->Content()));
        return nullptr;
    }

    const auto& sort = *window->Child(3);
    TExprNode::TPtr settings = children[2];
    if (sort.ChildrenSize() == 0 && !HasSetting(*settings, "warned_unordered_window")) {
        settings = AddSetting(
            *settings, node->Pos(), "warned_unordered_window", /*value=*/nullptr, ctx);
        if (!WarnYqlRankWithoutOrderBy(*node, ctx)) {
            return nullptr;
        }
    }

    if (node->ChildrenSize() == 4) {
        auto windowExpr = BuildYqlWindowSortKeyTypeWitness(sort, ctx);
        if (!windowExpr) {
            return nullptr;
        }
        settings = AddSetting(
            *settings, node->Pos(), "window_expr", windowExpr, ctx);
    }

    children[2] = std::move(settings);
    return ctx.ChangeChildren(*node, std::move(children));
}

TMaybe<TYqlFromSettings> TYqlFromSettings::Parse(const TExprNode::TPtr& settings, TExtContext& ctx) {
    TYqlFromSettings parsed;

    auto validator = [&](TStringBuf name, TExprNode& setting, TExprContext& ctx) -> bool {
        if (name == "cte" || name == "into_values") {
            if (setting.ChildrenSize() != 1) {
                ctx.AddError(TIssue(
                    ctx.GetPosition(setting.Pos()),
                    TStringBuilder() << "No extra parameters are expected by setting "
                                     << "'" << name << "'"));
                return false;
            }

            parsed.IsExplicitlyColumnOrdered = true;
            return true;
        }

        YQL_ENSURE(false, "unknown setting " << name);
    };

    if (!EnsureValidSettings(*settings, {"cte", "into_values"}, validator, ctx.Expr)) {
        return Nothing();
    }

    return parsed;
}

IGraphTransformer::TStatus PromoteYqlAggOptions(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx)
{
    YQL_ENSURE(0 < input->ChildrenSize());
    TExprNode::TPtr options = input->ChildPtr(input->ChildrenSize() - 1);
    if (GetSetting(*options, "yql_agg_promoted")) {
        return IGraphTransformer::TStatus::Ok;
    }

    if (!IsOptionalAggregation(options)) {
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
        if (GetSetting(*options, "as_optional")) {
            return node;
        }

        options = AddSetting(*options, node->Pos(), "as_optional", /*value=*/nullptr, ctx);
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

IGraphTransformer::TStatus InferYqlImplicitUsingJoinColumns(
    const TExprNode::TPtr& predicate,
    const TInputs& groupInputs,
    const TVector<ui32>& lhsIndexes,
    const TVector<ui32>& rhsIndexes,
    const TExprNode& setItem,
    TVector<std::pair<TString, TString>>& implicitUsing,
    TExtContext& ctx)
{
    auto equalities = GetImplicitUsingEqualities(predicate);
    const auto without = GetYqlWithoutForStarResult(setItem);

    implicitUsing.clear();
    implicitUsing.reserve(equalities.size());

    for (auto [lhsRef, rhsRef] : equalities) {
        std::pair<TString, TString> entry;
        if (auto status = TryToUsingEntry(
                std::move(lhsRef), std::move(rhsRef),
                groupInputs, lhsIndexes, rhsIndexes,
                entry, ctx);
            status != IGraphTransformer::TStatus::Ok)
        {
            return status;
        }

        if (without &&
            (IsYqlWithoutItem(entry.first, *without, /*isJoin=*/true) ||
             IsYqlWithoutItem(entry.second, *without, /*isJoin=*/true))) {
            continue;
        }

        implicitUsing.emplace_back(std::move(entry));
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus InferYqlInferUnionType(
    TPositionHandle pos,
    const TExprNode::TListType& children,
    TColumnOrder& resultColumnOrder,
    const TStructExprType*& resultStructType,
    TExtContext& ctx,
    bool& areColumnsOrdered,
    bool& isUniversal)
{
    YQL_ENSURE(resultColumnOrder.Size() == 0);
    areColumnsOrdered = false;

    auto status = InferUnionType(
        pos, children, resultStructType, ctx, /* areHashesChecked = */ false, isUniversal);

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (isUniversal) {
        return IGraphTransformer::TStatus::Ok;
    }

    auto order = InferOrderForUnionAll(resultStructType, children, ctx.Types);
    if (order) {
        resultColumnOrder = *order;
        areColumnsOrdered = true;
    }

    return status;
}

TMaybe<TYqlColumnOrder> InferYqlSimpleColumnOrder(const TExprNode::TPtr& input) {
    if (!input->IsCallable("YqlSelect")) {
        return Nothing();
    }

    const auto items = GetSetting(input->Head(), "set_items")->ChildPtr(1);

    TYqlColumnOrder result = ToColumnOrder(YqlSetItemLabels(items->ChildPtr(0)));

    for (const auto& item : items->Children()) {
        TYqlColumnOrder x = ToColumnOrder(YqlSetItemLabels(item));
        if (result != x) {
            return Nothing();
        }
    }

    return result;
}

IGraphTransformer::TStatus ValidateYqlExplicitColumnOrders(
    const TExprNode::TPtr& input,
    TExprNode::TPtr& output,
    TExtContext& ctx,
    TPositionHandle position,
    const TVector<TPositionHandle>& expectedPositions,
    const TVector<TString>& expectedOrder,
    const TYqlColumnOrder& actualOrder)
{
    constexpr size_t Limit = 4;

    TIssue issue(
        ctx.Expr.GetPosition(position),
        "Column names in SELECT don't match column specification in parenthesis");
    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_SOURCE_SELECT_COLUMN_MISMATCH, issue);

    for (size_t i = 0;
         (i < Min(actualOrder.size(), expectedOrder.size())) &&
         (issue.GetSubIssues().size() < Limit);
         i += 1)
    {
        const auto& label = actualOrder[i];
        if (label.IsSynthetic || label.Content == expectedOrder[i]) {
            continue;
        }

        auto subIssue = MakeIntrusive<TIssue>(
            ctx.Expr.GetPosition(expectedPositions[i]),
            TStringBuilder()
                << "At position " << (i + 1) << ' '
                << "actual " << '"' << label.Content << '"' << ' '
                << "doesn't match "
                << "expected " << '"' << expectedOrder[i] << '"');
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YQL_SOURCE_SELECT_COLUMN_MISMATCH, *subIssue);
        issue.AddSubIssue(std::move(subIssue));
    }

    if (issue.GetSubIssues().empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    if (auto status = AddSqlSelectWarning(input, output, ctx.Expr, "yql_explicit_column_orders");
        status != IGraphTransformer::TStatus::Repeat)
    {
        return status;
    }

    if (!ctx.Expr.AddWarning(issue)) {
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus ValidateYqlWarnShadow(
    const TExprNode::TPtr& input,
    TExprNode::TPtr& output,
    TExtContext& ctx,
    const TInputs& inputs)
{
    YQL_ENSURE(input->IsCallable("YqlSetItem"));

    const auto isResult = HasSetting(input->Head(), "result");
    const auto isValues = HasSetting(input->Head(), "values");
    YQL_ENSURE(isResult xor isValues, "Expected 'result' or 'values' in YqlSetItem");
    if (isValues) {
        return IGraphTransformer::TStatus::Ok;
    }

    TVector<TIssue> issues;

    for (const auto& label : YqlSetItemLabels(input)) {
        const TString& alias = label.Content;

        if (!label.IsShadowingWarning) {
            continue;
        }

        if (!AnyOf(inputs, [&](const TInput& input) {
            return input.Type->FindItemType(alias);
        })) {
            continue;
        }

        TIssue issue(
            ctx.Expr.GetPosition(label.Position),
            TStringBuilder()
                << "Alias `" << alias << "` shadows column with the same name. "
                << "It looks like comma is missed here. "
                << "If not, it is recommended to use ... AS `" << alias << "` to avoid confusion");
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_ALIAS_SHADOWS_COLUMN, issue);
        issues.emplace_back(std::move(issue));
    }

    if (issues.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    if (auto status = AddSqlSelectWarning(input, output, ctx.Expr, "yql_core_alias_shadows_column");
        status != IGraphTransformer::TStatus::Repeat)
    {
        return status;
    }

    bool isError = false;
    for (const auto& issue : issues) {
        if (!ctx.Expr.AddWarning(issue)) {
            isError = true;
        }
    }
    if (isError) {
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Repeat;
}

IGraphTransformer::TStatus YqlColumnOrTypeWrapper(
    const TExprNode::TPtr& input,
    TExprNode::TPtr& output,
    TContext& ctx)
{
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureComputable(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isUniversal;
    if (!EnsureAtomOrUniversal(input->Tail(), ctx.Expr, isUniversal)) {
        return IGraphTransformer::TStatus::Error;
    }

    // Keep the resolved column (or its deferred error) until the parent
    // has had a chance to interpret the original identifier as a type.
    input->SetTypeAnn(isUniversal ? ctx.Expr.MakeType<TUniversalExprType>() : input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus FinalizeYqlColumnRefs(
    const TExprNode::TPtr& input,
    TExprNode::TPtr& output,
    TExtContext& ctx)
{
    YQL_ENSURE(input->IsCallable("YqlSelect"));

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    settings.VisitChecker = [&](const TExprNode& node) {
        // Nested SELECTs finalize their own column references.
        return &node == input.Get() || !node.IsCallable({"YqlSelect", "PgSelect"});
    };

    return OptimizeExpr(
        input,
        output,
        [](const TExprNode::TPtr& node, TExprContext&) -> TExprNode::TPtr {
            // Type arguments have already been consumed by EnsureTypeRewrite.
            return node->IsCallable("YqlColumnOrType") ? node->HeadPtr() : node;
        },
        ctx.Expr,
        settings);
}

IGraphTransformer::TStatus ValidateYqlSubLinkSettings(
    const TExprNode::TPtr& input,
    TContext& ctx,
    bool& isUniversal)
{
    YQL_ENSURE(input->IsCallable("YqlSubLink"));
    isUniversal = false;
    if (input->ChildrenSize() != 6) {
        return IGraphTransformer::TStatus::Ok;
    }

    const auto settings = input->Child(5);
    if (settings->GetTypeAnn() && settings->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(settings->GetTypeAnn());
        isUniversal = true;
        return IGraphTransformer::TStatus::Ok;
    }

    if (!settings->GetTypeAnn() && settings->IsLambda()) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(settings->Pos()), "Expected settings, but got lambda"));
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Head().Content() != "any") {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(settings->Pos()),
            "Settings are allowed only for link type 'any'"));
        return IGraphTransformer::TStatus::Error;
    }

    const auto validator = [](TStringBuf name, TExprNode& setting, TExprContext& ctx) -> bool {
        if (setting.ChildrenSize() != 1) {
            ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()),
                TStringBuilder() << "No extra parameters are expected by setting '" << name << "'"));
            return false;
        }
        return true;
    };

    if (!EnsureValidSettings(*settings, {"ansiIn", "warnNoAnsiIn"}, validator, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (HasSetting(*settings, "ansiIn") && HasSetting(*settings, "warnNoAnsiIn")) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(settings->Pos()),
            "Settings 'ansiIn' and 'warnNoAnsiIn' are mutually exclusive"));
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus ValidateYqlSublinkInCollectionItemsNullable(
    const TExprNode::TPtr& input,
    TExprNode::TPtr& output,
    TContext& ctx,
    const TTypeAnnotationNode* lookupType,
    const TTypeAnnotationNode* collectionItemType)
{
    YQL_ENSURE(input->IsCallable("YqlSubLink"));
    if (input->ChildrenSize() != 6) {
        return IGraphTransformer::TStatus::Ok;
    }

    const auto settings = input->Child(5);
    if (!HasSetting(*settings, "warnNoAnsiIn")) {
        return IGraphTransformer::TStatus::Ok;
    }

    if (!lookupType->HasOptionalOrNull() &&
        !IsSqlInCollectionItemsNullable(lookupType, collectionItemType))
    {
        return IGraphTransformer::TStatus::Ok;
    }

    auto issue = TIssue(ctx.Expr.GetPosition(input->Pos()),
        "IN may produce unexpected result when used with nullable arguments. "
        "Consider adding 'PRAGMA AnsiInForEmptyOrNullableItemsCollections;'");
    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_CORE_LEGACY_IN_FOR_EMPTY_OR_NULLABLE, issue);
    if (!ctx.Expr.AddWarning(issue)) {
        return IGraphTransformer::TStatus::Error;
    }

    output = ctx.Expr.ChangeChild(*input, 5, RemoveSetting(*settings, "warnNoAnsiIn", ctx.Expr));
    return IGraphTransformer::TStatus::Repeat;
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
        } else if (content == "as_optional") {
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

    if (!isDefault && GetSetting(*settings, "as_optional")) {
        // clang-format off
        result = ctx.Expr.Builder(input->Pos())
            .Callable("AsOptionalType")
                .Add(0, result)
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

    if (input->Child(2)->GetTypeAnn() &&
        input->Child(2)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal)
    {
        input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
        return IGraphTransformer::TStatus::Ok;
    }

    const TStringBuf name = input->Head().Content();
    const bool isRank = IsIn({"rank", "denserank", "percentrank"}, name);
    THashSet<TStringBuf> supportedSettings;
    if (isRank) {
        supportedSettings.insert("ansi");
        supportedSettings.insert("warnNoAnsi");
        supportedSettings.insert("window_expr");
        supportedSettings.insert("warned_unordered_window");
    }

    auto settingsValidator = [](TStringBuf name, TExprNode& setting, TExprContext& ctx) {
        const ui32 expectedSize = name == "window_expr" ? 2 : 1;
        if (!EnsureTupleSize(setting, expectedSize, ctx)) {
            return false;
        }

        if (name == "window_expr" && !setting.Tail().IsCallable("InstanceOf")) {
            ctx.AddError(TIssue(
                setting.Tail().Pos(ctx),
                TStringBuilder() << "Expected InstanceOf for setting '" << name << "'"));
            return false;
        }

        return true;
    };
    if (!EnsureValidSettings(*input->Child(2), supportedSettings, settingsValidator, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TTypeAnnotationNode* typeSlot = input->Child(3)->GetTypeAnn();
    if (typeSlot && typeSlot->GetKind() == ETypeAnnotationKind::Type) {
        TExprNode::TPtr settings = RemoveSettings(
            input->ChildPtr(2),
            {"window_expr", "warned_unordered_window"},
            ctx.Expr);

        if (settings != input->ChildPtr(2)) {
            output = ctx.Expr.ChangeChild(*input, 2, std::move(settings));
            return IGraphTransformer::TStatus::Repeat;
        }
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

    if (isRank) {
        const auto windowExpr = GetSetting(*input->Child(2), "window_expr");
        const TExprNode::TPtr& key = input->ChildrenSize() > 4
            ? input->ChildPtr(4)
            : windowExpr
                ? windowExpr->TailPtr()
                : input->ChildPtr(3);

        // clang-format off
        keyExtractor = ctx.Expr.Builder(keyExtractor->Pos())
            .Lambda()
                .Param("row")
                .Set(key)
            .Seal()
            .Build();
        // clang-format on
    }

    TExprNode::TPtr call = ctx.Expr.ChangeChild(
        *input,
        2,
        FilterSettings(*input->Child(2), {"ansi", "warnNoAnsi"}, ctx.Expr));

    TExprNode::TPtr resultExpr = ExpandSqlWindowCall(
        call, listType, keyExtractor, rewrite, ctx.Expr, ctx.Types);
    if (!resultExpr) {
        return IGraphTransformer::TStatus::Error;
    }

    if (resultExpr->IsCallable("WindowTraits")) {
        TExprNode::TPtr traits = resultExpr;
        TExprNode::TPtr body = input->Child(4);

        TExprNode::TPtr resultType =
            ExpandResultType(traits, body, ctx.Expr);

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
