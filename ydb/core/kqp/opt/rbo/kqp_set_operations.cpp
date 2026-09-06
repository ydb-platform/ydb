#include "kqp_plan_conversion_utils.h"
#include "kqp_rbo_utils.h"
#include "map_renames.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <algorithm>

namespace NKikimr::NKqp {
namespace {

bool OrdinaryEqualityIsSetEquality(const TTypeAnnotationNode& type) {
    if (type.GetKind() != ETypeAnnotationKind::Data) {
        return false; // In particular, NULL-equal keys need the count plan.
    }
    const auto slot = type.Cast<TDataExprType>()->GetSlot();
    return (NUdf::GetDataTypeInfo(slot).Features & NUdf::IntegralType) ||
        slot == NUdf::EDataSlot::Bool || slot == NUdf::EDataSlot::String ||
        slot == NUdf::EDataSlot::Utf8 || slot == NUdf::EDataSlot::Date;
}

// The frontend has already inferred one by-name common schema. Materialize
// missing fields as NULL and widen present fields before comparing any rows.
TIntrusivePtr<IOperator> AlignSetInput(TIntrusivePtr<IOperator> input,
    const TStructExprType& source, const TStructExprType& target,
    TPositionHandle pos, TExprContext& ctx, TPlanProps& props)
{
    TVector<TMapElement> elements;
    for (const auto* field : target.GetItems()) {
        const auto* sourceType = source.FindItemType(field->GetName());
        const auto* targetType = field->GetItemType();
        if (sourceType && IsSameAnnotation(*sourceType, *targetType)) {
            continue;
        }
        const TInfoUnit column(TString(field->GetName()));
        const auto row = ctx.NewArgument(pos, "row");
        TExprNode::TPtr value;
        if (sourceType) {
            value = ctx.NewCallable(pos, "SafeCast", {
                ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, field->GetName())}),
                ExpandType(pos, *targetType, ctx)});
            // A Map computes all expressions from its input. Hide the old IU
            // while defining the widened value under the public column name.
            elements.emplace_back(MakeGeneratedIgnoreIU(props), column, pos, &ctx, &props);
        } else {
            value = targetType->GetKind() == ETypeAnnotationKind::Null
                ? ctx.NewCallable(pos, "Null", {})
                : ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *targetType, ctx)});
        }
        elements.emplace_back(column, TExpression(ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), {value}), &ctx));
    }
    return elements.empty() ? input : MakeIntrusive<TOpMap>(input, pos, elements);
}

} // namespace

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpSetOp(TExprNode::TPtr node) {
    const auto op = NYql::NNodes::TKqpOpSetOp(node);
    const auto kind = op.SetOp().StringValue();
    Y_ENSURE(kind == "union_all" || kind == "union" || kind == "intersect" || kind == "except",
        "Unsupported set operation: " << kind);
    const auto pos = node->Pos();
    const auto* rowType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto align = [&](TExprNode::TPtr branch) {
        const auto* source = branch->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        return AlignSetInput(ExprNodeToOperator(branch), *source, *rowType, pos, Ctx, PlanProps);
    };
    auto left = align(op.LeftInput().Ptr());
    auto right = align(op.RightInput().Ptr());
    TVector<TInfoUnit> columns;
    for (const auto* field : rowType->GetItems()) {
        columns.emplace_back(TString(field->GetName()));
    }
    const auto distinct = [&](TIntrusivePtr<IOperator> input) {
        TVector<TOpAggregationTraits> traits;
        for (const auto& column : columns) {
            traits.emplace_back(column, "distinct", column);
        }
        return MakeIntrusive<TOpAggregate>(input, traits, columns, EOpPhase::Undefined, true, pos);
    };

    if (kind == "union_all" || kind == "union") {
        TIntrusivePtr<IOperator> result = MakeIntrusive<TOpUnionAll>(left, right, pos, columns);
        if (kind == "union") {
            result = distinct(result);
        }
        return result;
    }

    // The existing semi/anti-join plan is smaller when ordinary equality is
    // exactly set equality. Optional and NaN-capable keys must not take it.
    if (!columns.empty() && std::all_of(rowType->GetItems().begin(), rowType->GetItems().end(),
        [](const auto* field) { return OrdinaryEqualityIsSetEquality(*field->GetItemType()); }))
    {
        TVector<std::pair<TInfoUnit, TInfoUnit>> keys;
        for (const auto& column : columns) {
            keys.emplace_back(column, column);
        }
        return distinct(MakeIntrusive<TOpJoin>(left, right, pos,
            kind == "intersect" ? TString("LeftSemi") : TString("LeftOnly"), keys));
    }

    // Set equality treats NULLs as equal. As in YQL's CombineSetItems, count
    // occurrences from each side in a NULL-equal GROUP BY, not an ordinary join.
    auto used = MakeInfoUnitSet(left->GetOutputIUs());
    AddInfoUnits(used, right->GetOutputIUs());
    const auto leftCount = NMapRenames::MakeUniqueInternalIU(PlanProps.InternalVarIdx, used);
    const auto rightCount = NMapRenames::MakeUniqueInternalIU(PlanProps.InternalVarIdx, used);
    const auto* optionalBool = Ctx.MakeType<TOptionalExprType>(Ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool));
    const auto present = Ctx.NewCallable(pos, "Just", {Ctx.NewCallable(pos, "Bool", {Ctx.NewAtom(pos, "true")})});
    const auto absent = Ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *optionalBool, Ctx)});
    const auto mark = [&](TIntrusivePtr<IOperator> input, bool isLeft) {
        return MakeIntrusive<TOpMap>(input, pos, TVector<TMapElement>{
            {leftCount, TExpression(isLeft ? present : absent, &Ctx)},
            {rightCount, TExpression(isLeft ? absent : present, &Ctx)}});
    };
    auto markedColumns = columns;
    markedColumns.push_back(leftCount);
    markedColumns.push_back(rightCount);
    const auto both = MakeIntrusive<TOpUnionAll>(mark(left, true), mark(right, false), pos, markedColumns);
    const auto counts = MakeIntrusive<TOpAggregate>(both, TVector<TOpAggregationTraits>{
        {leftCount, "count", leftCount}, {rightCount, "count", rightCount}},
        columns, EOpPhase::Undefined, false, pos);
    const auto row = Ctx.NewArgument(pos, "row");
    const auto zero = Ctx.NewCallable(pos, "Uint64", {Ctx.NewAtom(pos, "0")});
    const auto compareCount = [&](const TInfoUnit& column, TStringBuf comparison) {
        return Ctx.NewCallable(pos, comparison, {
            Ctx.NewCallable(pos, "Member", {row, Ctx.NewAtom(pos, column.GetFullName())}), zero});
    };
    const auto predicate = Ctx.NewCallable(pos, "And", {
        compareCount(leftCount, ">"), compareCount(rightCount, kind == "intersect" ? ">" : "==")});
    const auto filtered = MakeIntrusive<TOpFilter>(counts, pos,
        TExpression(Ctx.NewLambda(pos, Ctx.NewArguments(pos, {row}), {predicate}), &Ctx));
    return MakeIntrusive<TOpMap>(filtered, pos, TVector<TMapElement>{
        {MakeGeneratedIgnoreIU(PlanProps), leftCount, pos, &Ctx, &PlanProps},
        {MakeGeneratedIgnoreIU(PlanProps), rightCount, pos, &Ctx, &PlanProps}});
}

} // namespace NKikimr::NKqp
