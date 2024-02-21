#include "yql_opt_range.h"
#include "yql_opt_utils.h"
#include "yql_expr_type_annotation.h"

#include <ydb/library/yql/public/udf/tz/udf_tz.h>

namespace NYql {
namespace {

struct TRangeBoundary {
    TExprNode::TPtr Value; // top level null means infinity
    bool Included = false;
};

TExprNode::TPtr BuildBoundaryNode(TPositionHandle pos, const TRangeBoundary& boundary, TExprContext& ctx) {
    YQL_ENSURE(boundary.Value);
    return ctx.Builder(pos)
        .List()
            .Add(0, boundary.Value)
            .Callable(1, "Int32")
                .Atom(0, boundary.Included ? "1" : "0", TNodeFlags::Default)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildRange(TPositionHandle pos, const TRangeBoundary& left, const TRangeBoundary& right,
    TExprContext& ctx)
{
    return ctx.NewList(pos, { BuildBoundaryNode(pos, left, ctx), BuildBoundaryNode(pos, right, ctx) });
}

TExprNode::TPtr BuildRangeSingle(TPositionHandle pos, const TRangeBoundary& left, const TRangeBoundary& right,
                                 TExprContext& ctx)
{
    return ctx.NewCallable(pos, "AsRange", { BuildRange(pos, left, right, ctx) });
}

TMaybe<EDataSlot> GetBaseDataSlot(const TTypeAnnotationNode* keyType) {
    auto baseType = RemoveAllOptionals(keyType);
    TMaybe<EDataSlot> result;
    if (baseType->GetKind() == ETypeAnnotationKind::Data) {
        result = baseType->Cast<TDataExprType>()->GetSlot();
    }
    return result;
}

bool HasUncomparableNaNs(const TTypeAnnotationNode* keyType) {
    // PostgreSQL comparison operators treat NaNs as biggest flating values
    // this behavior matches ordering in ORDER BY, so we don't need special NaN handling for Pg types
    auto slot = GetBaseDataSlot(keyType);
    return slot && (NUdf::GetDataTypeInfo(*slot).Features & (NUdf::EDataTypeFeatures::FloatType | NUdf::EDataTypeFeatures::DecimalType));
}

TExprNode::TPtr MakeNaNBoundary(TPositionHandle pos, const TTypeAnnotationNode* keyType, TExprContext& ctx) {
    YQL_ENSURE(HasUncomparableNaNs(keyType));
    auto baseType = RemoveAllOptionals(keyType);
    auto keySlot = baseType->Cast<TDataExprType>()->GetSlot();

    return ctx.Builder(pos)
        .Callable("SafeCast")
            .Callable(0, NUdf::GetDataTypeInfo(keySlot).Name)
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                parent.Atom(0, "nan", TNodeFlags::Default);
                if (keySlot == EDataSlot::Decimal) {
                    auto decimalType = baseType->Cast<TDataExprParamsType>();
                    parent.Atom(1, decimalType->GetParamOne());
                    parent.Atom(2, decimalType->GetParamTwo());
                }
                return parent;
            })
            .Seal()
            .Add(1, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(keyType), ctx))
        .Seal()
        .Build();
}

TExprNode::TPtr TzRound(const TExprNode::TPtr& key, const TTypeAnnotationNode* keyType, bool down, TExprContext& ctx) {
    TPositionHandle pos = key->Pos();
    const auto& timeZones = NUdf::GetTimezones();
    const size_t tzSize = timeZones.size();
    YQL_ENSURE(tzSize > 0);
    size_t targetTzId = 0;
    if (!down) {
        for (targetTzId = tzSize - 1; targetTzId > 0; --targetTzId) {
            if (!timeZones[targetTzId].empty()) {
                break;
            }
        }
    }
    YQL_ENSURE(!timeZones[targetTzId].empty());
    return ctx.Builder(pos)
        .Callable("Coalesce")
            .Callable(0, "AddTimezone")
                .Callable(0, "RemoveTimezone")
                    .Callable(0, "SafeCast")
                        .Add(0, key)
                        .Add(1, ExpandType(pos, *RemoveAllOptionals(keyType), ctx))
                    .Seal()
                .Seal()
                .Callable(1, "Uint16")
                    .Atom(0, ToString(targetTzId), TNodeFlags::Default)
                .Seal()
            .Seal()
            .Add(1, key)
        .Seal()
        .Build();
}

TRangeBoundary BuildPlusInf(TPositionHandle pos, const TTypeAnnotationNode* keyType, TExprContext& ctx, bool excludeNaN = true) {
    TRangeBoundary result;

    if (excludeNaN && HasUncomparableNaNs(keyType)) {
        // exclude NaN value (NaN is ordered as largest floating point value)
        result.Value = MakeNaNBoundary(pos, keyType, ctx);
    } else {
        auto optKeyTypeNode = ExpandType(pos, *ctx.MakeType<TOptionalExprType>(keyType), ctx);
        result.Value = ctx.NewCallable(pos, "Nothing", { optKeyTypeNode });
    }

    result.Included = false;
    return result;
}

TRangeBoundary BuildMinusInf(TPositionHandle pos, const TTypeAnnotationNode* keyType, TExprContext& ctx) {
    // start from first non-null value
    auto optKeyTypeNode = ExpandType(pos, *ctx.MakeType<TOptionalExprType>(keyType), ctx);
    auto optBaseKeyTypeNode = ExpandType(pos, *ctx.MakeType<TOptionalExprType>(RemoveAllOptionals(keyType)), ctx);
    TExprNode::TPtr largestNull;
    if (keyType->GetKind() == ETypeAnnotationKind::Pg) {
        largestNull = ctx.Builder(pos)
            .Callable("Just")
                .Callable(0, "Nothing")
                    .Add(0, ExpandType(pos, *keyType, ctx))
                .Seal()
            .Seal()
            .Build();
    } else {
        largestNull = ctx.Builder(pos)
            .Callable("SafeCast")
                .Callable(0, "Nothing")
                    .Add(0, optBaseKeyTypeNode)
                .Seal()
                .Add(1, optKeyTypeNode)
            .Seal()
            .Build();
    }

    TRangeBoundary result;
    result.Value = largestNull;
    result.Included = false;
    return result;
}

TExprNode::TPtr MakeWidePointRangeLambda(TPositionHandle pos, const TTypeAnnotationNode* keyType, TExprContext& ctx) {
    TStringBuf name;
    TString maxValueStr;

    const TTypeAnnotationNode* baseKeyType = RemoveAllOptionals(keyType);
    const auto keySlot = baseKeyType->Cast<TDataExprType>()->GetSlot();
    switch (keySlot) {
        case EDataSlot::Int8:   name = "Int8"; maxValueStr = ToString(Max<i8>()); break;
        case EDataSlot::Uint8:  name = "Uint8"; maxValueStr = ToString(Max<ui8>()); break;
        case EDataSlot::Int16:  name = "Int16"; maxValueStr = ToString(Max<i16>()); break;
        case EDataSlot::Uint16: name = "Uint16"; maxValueStr = ToString(Max<ui16>()); break;
        case EDataSlot::Int32:  name = "Int32"; maxValueStr = ToString(Max<i32>()); break;
        case EDataSlot::Uint32: name = "Uint32"; maxValueStr = ToString(Max<ui32>()); break;
        case EDataSlot::Int64:  name = "Int64"; maxValueStr = ToString(Max<i64>()); break;
        case EDataSlot::Uint64: name = "Uint64"; maxValueStr = ToString(Max<ui64>()); break;

        case EDataSlot::Date:        name = "Date"; maxValueStr = ToString(NUdf::MAX_DATE - 1); break;
        case EDataSlot::Datetime:    name = "Datetime"; maxValueStr = ToString(NUdf::MAX_DATETIME - 1); break;
        case EDataSlot::Timestamp:   name = "Timestamp"; maxValueStr = ToString(NUdf::MAX_TIMESTAMP - 1); break;
        case EDataSlot::Date32:      name = "Date32"; maxValueStr = ToString(NUdf::MAX_DATE32); break;
        case EDataSlot::Datetime64:  name = "Datetime64"; maxValueStr = ToString(NUdf::MAX_DATETIME64); break;
        case EDataSlot::Timestamp64: name = "Timestamp64"; maxValueStr = ToString(NUdf::MAX_TIMESTAMP64); break;
        default:
            ythrow yexception() << "Unexpected type: " << baseKeyType->Cast<TDataExprType>()->GetName();
    }

    TExprNode::TPtr maxValue = ctx.NewCallable(pos, name, { ctx.NewAtom(pos, maxValueStr, TNodeFlags::Default) });
    TExprNode::TPtr addValue;
    if (keySlot == EDataSlot::Date) {
        addValue = ctx.NewCallable(pos, "Interval", { ctx.NewAtom(pos, "86400000000", TNodeFlags::Default) });
    } else if (keySlot == EDataSlot::Datetime) {
        addValue = ctx.NewCallable(pos, "Interval", { ctx.NewAtom(pos, "1000000", TNodeFlags::Default) });
    } else if (keySlot == EDataSlot::Timestamp) {
        addValue = ctx.NewCallable(pos, "Interval", { ctx.NewAtom(pos, "1", TNodeFlags::Default) });
    } else if (keySlot == EDataSlot::Date32) {
        addValue = ctx.NewCallable(pos, "Interval64", { ctx.NewAtom(pos, "86400000000", TNodeFlags::Default) });
    } else if (keySlot == EDataSlot::Datetime64) {
        addValue = ctx.NewCallable(pos, "Interval64", { ctx.NewAtom(pos, "1000000", TNodeFlags::Default) });
    } else if (keySlot == EDataSlot::Timestamp64) {
        addValue = ctx.NewCallable(pos, "Interval64", { ctx.NewAtom(pos, "1", TNodeFlags::Default) });
    } else {
        addValue = ctx.NewCallable(pos, name, { ctx.NewAtom(pos, "1", TNodeFlags::Default) });
    }

    TExprNode::TPtr key = ctx.NewArgument(pos, "optKey");
    TExprNode::TPtr castedKey = ctx.Builder(pos)
        .Callable("SafeCast")
            .Add(0, key)
            .Add(1, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(baseKeyType), ctx))
        .Seal()
        .Build();

    TExprNode::TPtr leftBoundary = ctx.Builder(pos)
        .List()
            .Add(0, key)
            .Callable(1, "Int32")
                .Atom(0, "1", TNodeFlags::Default)
            .Seal()
        .Seal()
        .Build();

    auto body = ctx.Builder(pos)
        .Callable("AsRange")
            .List(0)
                .Add(0, leftBoundary)
                .Callable(1, "If")
                    .Callable(0, "Coalesce")
                        .Callable(0, "==")
                            .Add(0, key)
                            .Add(1, maxValue)
                        .Seal()
                        .Callable(1, "Bool")
                            .Atom(0, "true", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Add(1, leftBoundary)
                    .List(2)
                        .Callable(0, "SafeCast")
                            .Callable(0, "+")
                                .Add(0, castedKey)
                                .Add(1, addValue)
                            .Seal()
                            .Add(1, ExpandType(pos, *keyType, ctx))
                        .Seal()
                        .Callable(1, "Int32")
                            .Atom(0, "0", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
    return ctx.NewLambda(pos, ctx.NewArguments(pos, { key }), std::move(body));
}

TExprNode::TPtr BuildNormalRangeLambdaRaw(TPositionHandle pos, const TTypeAnnotationNode* keyType,
    TStringBuf op, TExprContext& ctx)
{
    // key is argument of Optional<keyType> type
    const auto key = ctx.NewArgument(pos, "key");
    const auto keySlot = GetBaseDataSlot(keyType);
    const bool isTzKey = keySlot && (NUdf::GetDataTypeInfo(*keySlot).Features & NUdf::EDataTypeFeatures::TzDateType);
    const bool isIntegralOrDateKey = keySlot && (NUdf::GetDataTypeInfo(*keySlot).Features &
        (NUdf::EDataTypeFeatures::IntegralType | NUdf::EDataTypeFeatures::DateType));
    const auto downKey = isTzKey ? TzRound(key, keyType, true, ctx) : key;
    const auto upKey = isTzKey ? TzRound(key, keyType, false, ctx) : key;
    if (op == "!=") {
        TRangeBoundary left, right;
        left = BuildMinusInf(pos, keyType, ctx);
        right.Value = downKey;
        right.Included = false;
        auto leftRange = BuildRange(pos, left, right, ctx);

        left.Value = upKey;
        left.Included = false;
        bool excludeNaN = false;
        right = BuildPlusInf(pos, keyType, ctx, excludeNaN);
        auto rightRange = BuildRange(pos, left, right, ctx);

        auto body = ctx.NewCallable(pos, "AsRange", { leftRange, rightRange });
        if (HasUncomparableNaNs(keyType)) {
            // NaN is not equal to any value (including NaN itself), so we must include it
            left.Included = true;
            left.Value = MakeNaNBoundary(pos, keyType, ctx);
            right = left;
            auto nan = BuildRangeSingle(pos, left, right, ctx);
            body = ctx.NewCallable(pos, "RangeUnion", { body, nan });
        }
        return ctx.NewLambda(pos, ctx.NewArguments(pos, { key }), std::move(body));
    }

    if (op == "===") {
        if (isIntegralOrDateKey) {
            return MakeWidePointRangeLambda(pos, keyType, ctx);
        }
        op = "==";
    }

    TRangeBoundary left, right;
    if (op == "==") {
        left.Value = downKey;
        right.Value = upKey;
        left.Included = right.Included = true;
    } else if (op == "<" || op == "<=") {
        left = BuildMinusInf(pos, keyType, ctx);
        right.Value = (op == "<=") ? upKey : downKey;
        right.Included = (op == "<=");
    } else if (op == ">" || op == ">=") {
        right = BuildPlusInf(pos, keyType, ctx);
        left.Value = (op == ">=") ? downKey : upKey;
        left.Included = (op == ">=");
    } else if (op == "Exists" || op == "NotExists" ) {
        YQL_ENSURE(keyType->GetKind() == ETypeAnnotationKind::Optional || keyType->GetKind() == ETypeAnnotationKind::Pg);
        auto nullKey = ctx.Builder(pos)
            .Callable("Just")
                .Callable(0, "Nothing")
                    .Add(0, ExpandType(pos, *keyType, ctx))
                .Seal()
            .Seal()
            .Build();

        if (op == "NotExists") {
            left.Value = right.Value = nullKey;
            left.Included = right.Included = true;
        } else {
            left.Value = nullKey;
            left.Included = false;
            // Exists should include NaN
            bool excludeNaN = false;
            right = BuildPlusInf(pos, keyType, ctx, excludeNaN);
        }
    } else {
        YQL_ENSURE(false, "Unknown operation: " << op);
    }

    TExprNode::TPtr body = BuildRangeSingle(pos, left, right, ctx);
    if (op == "==" && HasUncomparableNaNs(keyType)) {
        auto fullRangeWithoutNaNs = BuildRangeSingle(pos,
            BuildMinusInf(pos, keyType, ctx),
            BuildPlusInf(pos, keyType, ctx), ctx);
        body = ctx.NewCallable(pos, "RangeIntersect", { body, fullRangeWithoutNaNs });
    }

    return ctx.NewLambda(pos, ctx.NewArguments(pos, { key }), std::move(body));
}

} //namespace

TExprNode::TPtr ExpandRangeEmpty(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable("RangeEmpty"));
    if (node->ChildrenSize() == 0) {
        return ctx.RenameNode(*node, "EmptyList");
    }

    return ctx.NewCallable(node->Pos(), "List", { ExpandType(node->Pos(), *node->GetTypeAnn(), ctx) });
}

TExprNode::TPtr ExpandAsRange(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable("AsRange"));
    YQL_ENSURE(node->ChildrenSize());

    return ctx.NewCallable(node->Pos(), "RangeCreate", { ctx.RenameNode(*node, "AsList") });
}

TExprNode::TPtr ExpandRangeFor(const TExprNode::TPtr& node, TExprContext& ctx) {
    TPositionHandle pos = node->Pos();

    TStringBuf op = node->Head().Content();
    auto value = node->ChildPtr(1);
    const TTypeAnnotationNode* valueType = value->GetTypeAnn();
    const TTypeAnnotationNode* keyType = node->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();

    const TTypeAnnotationNode* optKeyType = ctx.MakeType<TOptionalExprType>(keyType);
    const TTypeAnnotationNode* keyBaseType = RemoveAllOptionals(keyType);
    const TTypeAnnotationNode* valueBaseType = RemoveAllOptionals(valueType);

    const TExprNode::TPtr castedToKey = ctx.NewCallable(pos, "StrictCast",
        { value, ExpandType(pos, *optKeyType, ctx) });

    const TExprNode::TPtr emptyRange = ctx.NewCallable(pos, "RangeEmpty",
        { ExpandType(pos, *keyType, ctx) });

    TExprNode::TPtr rangeForNullCast;
    if (op == "==" || op == "===" || op == "StartsWith") {
        rangeForNullCast = emptyRange;
    } else if (op == "!=" || op == "NotStartsWith") {
        bool excludeNaN = false;
        rangeForNullCast = BuildRangeSingle(pos,
            BuildMinusInf(pos, keyType, ctx),
           BuildPlusInf(pos, keyType, ctx, excludeNaN), ctx);
    }

    TExprNode::TPtr result;
    if (op == "Exists" || op == "NotExists") {
        result = ctx.Builder(pos)
            .Apply(BuildNormalRangeLambdaRaw(pos, keyType, op, ctx))
                .With(0, value) // value is not actually used for Exists/NotExists
            .Seal()
            .Build();
    } else if (op == "==" || op == "!=" || op == "===") {
        YQL_ENSURE(rangeForNullCast);
        result = ctx.Builder(pos)
            .Callable("If")
                .Callable(0, "HasNull")
                    .Add(0, castedToKey)
                .Seal()
                .Add(1, rangeForNullCast)
                .Apply(2, BuildNormalRangeLambdaRaw(pos, keyType, op, ctx))
                    .With(0, castedToKey)
                .Seal()
            .Seal()
            .Build();
    } else if (op == "StartsWith" || op == "NotStartsWith") {
        YQL_ENSURE(rangeForNullCast);

        const auto boundary = ctx.NewArgument(pos, "boundary");
        const auto next = ctx.NewCallable(pos, "NextValue", { boundary });

        TExprNode::TPtr rangeForIfNext;
        TExprNode::TPtr rangeForIfNotNext;

        if (op == "StartsWith") {
            const auto geBoundary = ctx.Builder(pos)
                .Callable("RangeFor")
                    .Atom(0, ">=", TNodeFlags::Default)
                    .Add(1, boundary)
                    .Add(2, node->TailPtr())
                .Seal()
                .Build();

            rangeForIfNext = ctx.Builder(pos)
                // [boundary, next)
                .Callable("RangeIntersect")
                    .Add(0, geBoundary)
                    .Callable(1, "RangeFor")
                        .Atom(0, "<", TNodeFlags::Default)
                        .Add(1, next)
                        .Add(2, node->TailPtr())
                    .Seal()
                .Seal()
                .Build();

            // [boundary, +inf)
            rangeForIfNotNext = geBoundary;
        } else {
            const auto lessBoundary = ctx.Builder(pos)
                .Callable("RangeFor")
                    .Atom(0, "<", TNodeFlags::Default)
                    .Add(1, boundary)
                    .Add(2, node->TailPtr())
                .Seal()
                .Build();

            rangeForIfNext = ctx.Builder(pos)
                // (-inf, boundary) U [next, +inf)
                .Callable("RangeUnion")
                    .Add(0, lessBoundary)
                    .Callable(1, "RangeFor")
                        .Atom(0, ">=", TNodeFlags::Default)
                        .Add(1, next)
                        .Add(2, node->TailPtr())
                    .Seal()
                .Seal()
                .Build();

            // (-inf, boundary)
            rangeForIfNotNext = lessBoundary;
        }

        auto body = ctx.Builder(pos)
            .Callable("If")
                .Callable(0, "HasNull")
                    .Add(0, next)
                .Seal()
                .Add(1, rangeForIfNotNext)
                .Add(2, rangeForIfNext)
            .Seal()
            .Build();

        YQL_ENSURE(rangeForNullCast);
        result = ctx.Builder(pos)
            .Callable("IfPresent")
                .Callable(0, "StrictCast")
                    .Add(0, value)
                    .Add(1, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(keyBaseType), ctx))
                .Seal()
                .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, { boundary}), std::move(body)))
                .Add(2, rangeForNullCast)
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(op == "<" || op == ">" || op == "<=" || op == ">=");
        result = ctx.Builder(pos)
            .Callable("If")
                .Callable(0, "HasNull")
                    .Add(0, castedToKey)
                .Seal()
                .Callable(1, "IfPresent")
                    .Callable(0, "SafeCast")
                        .Add(0, value)
                        .Add(1, ExpandType(pos, *ctx.MakeType<TOptionalExprType>(valueBaseType), ctx))
                    .Seal()
                    .Lambda(1)
                        .Param("unwrappedValue")
                        .Callable("IfPresent")
                            .Callable(0, op.StartsWith("<") ? "RoundDown" : "RoundUp")
                                .Arg(0, "unwrappedValue")
                                .Add(1, ExpandType(pos, *keyBaseType, ctx))
                            .Seal()
                            .Lambda(1)
                                .Param("roundedKey")
                                .Apply(BuildNormalRangeLambdaRaw(pos, keyType, op.StartsWith("<") ? "<=" : ">=", ctx))
                                    .With(0)
                                        .Callable("SafeCast")
                                            .Arg(0, "roundedKey")
                                            .Add(1, ExpandType(pos, *optKeyType, ctx))
                                        .Seal()
                                    .Done()
                                .Seal()
                            .Seal()
                            .Add(2, emptyRange)
                        .Seal()
                    .Seal()
                    .Add(2, emptyRange)
                .Seal()
                .Apply(2, BuildNormalRangeLambdaRaw(pos, keyType, op, ctx))
                    .With(0, castedToKey)
                .Seal()
            .Seal()
            .Build();
    }

    return result;
}

TExprNode::TPtr ExpandRangeToPg(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable("RangeToPg"));
    const size_t numComponents = node->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->
        Cast<TTupleExprType>()->GetItems().front()->Cast<TTupleExprType>()->GetSize();
    return ctx.Builder(node->Pos())
        .Callable("OrderedMap")
            .Add(0, node->HeadPtr())
            .Lambda(1)
                .Param("range")
                .Callable("StaticMap")
                    .Arg(0, "range")
                    .Lambda(1)
                        .Param("boundary")
                        .List()
                            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                for (size_t i = 0; i < numComponents; ++i) {
                                    if (i % 2 == 0) {
                                        parent
                                            .Callable(i, "Nth")
                                                .Arg(0, "boundary")
                                                .Atom(1, i)
                                            .Seal();
                                    } else {
                                        parent
                                            .Callable(i, "Map")
                                                .Callable(0, "Nth")
                                                    .Arg(0, "boundary")
                                                    .Atom(1, i)
                                                .Seal()
                                                .Lambda(1)
                                                    .Param("unwrapped")
                                                    .Callable("ToPg")
                                                        .Arg(0, "unwrapped")
                                                    .Seal()
                                                .Seal()
                                            .Seal();
                                    }
                                }
                                return parent;
                            })
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}
}
