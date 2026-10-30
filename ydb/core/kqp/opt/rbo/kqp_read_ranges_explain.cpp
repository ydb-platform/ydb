#include "kqp_read_ranges_explain.h"

#include "kqp_info_unit.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_type_ops.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/types/uuid/uuid.h>
#include <yql/essentials/utils/utf8.h>

#include <util/charset/utf8.h>
#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/array_ref.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

#include <cmath>
#include <limits>
#include <optional>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using NUdf::EDataSlot;
using NDecimal::TInt128;

namespace {

// The structure lists at most this many values of a set and counts the rest.
constexpr size_t MaxDescribedPoints = 10;

// A finite key value. NULL is ordered before every other value, as an empty Optional is in MiniKQL.
struct TKeyValue {
    enum class EKind {
        Null,
        Data,
        Pg,
        Parameter, // a value only described by name, never computed
    };

    EKind Kind = EKind::Null;
    NYql::NUdf::EDataSlot Slot = NYql::NUdf::EDataSlot::Bool;
    ui8 Precision = 0;
    ui8 Scale = 0;
    TString PgType;
    TString Bytes;                      // String, Utf8 and Uuid data, Pg text, a parameter name
    NYql::NUdf::TUnboxedValuePod Value; // every other data type
};

// A literal key value, possibly NULL. Does not evaluate casts or other expressions.
std::optional<TKeyValue> ParseKeyValue(const NYql::TExprNode::TPtr& node);

TString FormatKeyValue(const TKeyValue& value);

// A range boundary in the MiniKQL internal form: for each column an infinity marker
// (-1 = -inf, +1 = +inf, 0 = Value), then the inclusion (-1 = '[', +1 = ']', 0 = excluded).
struct TComponent {
    i32 Inf = 0;
    TKeyValue Value;
};

struct TBoundary {
    TVector<TComponent> Components;
    i32 Included = 0;

    bool HasPrefix() const {
        return !Components.empty() && Components.front().Inf == 0;
    }
};

struct TRange {
    TBoundary Left;
    TBoundary Right;
};

struct TRangeList {
    TVector<TRange> Ranges;
    size_t Width = 0;
};

// The infinity a boundary continues with past its columns.
i32 GetInfSign(bool hasPrefix, bool isIncluded, bool isLeft);

// A boundary element: an empty outer Optional is an infinity, anything else is a key value.
struct TBoundaryElement {
    bool Infinite = false;
    TKeyValue Value;
};

std::optional<TBoundary> MakeBoundary(const TVector<TBoundaryElement>& elements, bool included, bool isLeft);

// The integer an integral, date or interval value holds.
std::optional<TInt128> GetInteger(const TKeyValue& value) {
    const auto& pod = value.Value;
    switch (value.Slot) {
        case EDataSlot::Int8: return pod.Get<i8>();
        case EDataSlot::Uint8: return pod.Get<ui8>();
        case EDataSlot::Int16: return pod.Get<i16>();
        case EDataSlot::Uint16: return pod.Get<ui16>();
        case EDataSlot::Int32: return pod.Get<i32>();
        case EDataSlot::Uint32: return pod.Get<ui32>();
        case EDataSlot::Int64: return pod.Get<i64>();
        case EDataSlot::Uint64: return pod.Get<ui64>();
        case EDataSlot::Date: return pod.Get<ui16>();
        case EDataSlot::Datetime: return pod.Get<ui32>();
        case EDataSlot::Timestamp: return pod.Get<ui64>();
        case EDataSlot::Interval: return pod.Get<i64>();
        case EDataSlot::Date32: return pod.Get<i32>();
        case EDataSlot::Datetime64: return pod.Get<i64>();
        case EDataSlot::Timestamp64: return pod.Get<i64>();
        case EDataSlot::Interval64: return pod.Get<i64>();
        default: return std::nullopt;
    }
}

template <typename T>
std::optional<NUdf::TUnboxedValuePod> ParseNumber(TStringBuf text) {
    T value;
    if (!TryFromString(text, value)) {
        return std::nullopt;
    }
    return NUdf::TUnboxedValuePod(value);
}

template <typename T>
std::optional<NUdf::TUnboxedValuePod> ParseFloat(TStringBuf text) {
    if (text == "nan" || text == "-nan") {
        return NUdf::TUnboxedValuePod(std::numeric_limits<T>::quiet_NaN());
    }
    if (text == "inf" || text == "+inf" || text == "-inf") {
        const T inf = std::numeric_limits<T>::infinity();
        return NUdf::TUnboxedValuePod(text == "-inf" ? -inf : inf);
    }
    return ParseNumber<T>(text);
}

// A data literal in the form YQL keeps it: the text of a number or a date, or the bytes of a string or a Uuid.
std::optional<TKeyValue> ParseData(const TExprNode& node) {
    const auto slot = NUdf::FindDataSlot(node.Content());
    if (!slot || node.ChildrenSize() == 0 || !node.Head().IsAtom()) {
        return std::nullopt;
    }
    const TStringBuf text = node.Head().Content();
    TKeyValue result{.Kind = TKeyValue::EKind::Data, .Slot = *slot};
    std::optional<NUdf::TUnboxedValuePod> value;
    switch (*slot) {
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Uuid:
            result.Bytes = text;
            return result;
        case EDataSlot::Bool: value = ParseNumber<bool>(text); break;
        case EDataSlot::Int8: value = ParseNumber<i8>(text); break;
        case EDataSlot::Uint8: value = ParseNumber<ui8>(text); break;
        case EDataSlot::Int16: value = ParseNumber<i16>(text); break;
        case EDataSlot::Uint16: value = ParseNumber<ui16>(text); break;
        case EDataSlot::Int32: value = ParseNumber<i32>(text); break;
        case EDataSlot::Uint32: value = ParseNumber<ui32>(text); break;
        case EDataSlot::Int64: value = ParseNumber<i64>(text); break;
        case EDataSlot::Uint64: value = ParseNumber<ui64>(text); break;
        case EDataSlot::Float: value = ParseFloat<float>(text); break;
        case EDataSlot::Double: value = ParseFloat<double>(text); break;
        case EDataSlot::Date: value = ParseNumber<ui16>(text); break;
        case EDataSlot::Datetime: value = ParseNumber<ui32>(text); break;
        case EDataSlot::Timestamp: value = ParseNumber<ui64>(text); break;
        case EDataSlot::Interval: value = ParseNumber<i64>(text); break;
        case EDataSlot::Date32: value = ParseNumber<i32>(text); break;
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
            value = ParseNumber<i64>(text);
            break;
        case EDataSlot::Decimal: {
            if (node.ChildrenSize() != 3 || !TryFromString(node.Child(1)->Content(), result.Precision)
                || !TryFromString(node.Child(2)->Content(), result.Scale)) {
                return std::nullopt;
            }
            const auto decimal = NDecimal::FromString(text, result.Precision, result.Scale);
            if (NDecimal::IsError(decimal)) {
                return std::nullopt;
            }
            value = NUdf::TUnboxedValuePod(decimal);
            break;
        }
        default:
            // Timezone types depend on the timezone database, the rest are not key types.
            return std::nullopt;
    }
    if (!value) {
        return std::nullopt;
    }
    result.Value = *value;
    return result;
}

bool IsPgNumeric(TStringBuf pgType) {
    return pgType == "int2" || pgType == "int4" || pgType == "int8" || pgType == "float4" || pgType == "float8"
        || pgType == "numeric";
}

TString QuoteText(TStringBuf text) {
    if (!IsUtf(text)) {
        return TStringBuilder() << '"' << EscapeC(text) << '"';
    }
    // Keep valid UTF-8 readable, escaping only what breaks the quoted form.
    TStringBuilder result;
    result << '"';
    for (const char c : text) {
        if (c == '"' || c == '\\') {
            result << '\\' << c;
        } else if (static_cast<unsigned char>(c) < 0x20 || c == 0x7f) {
            result << EscapeC(TStringBuf(&c, 1));
        } else {
            result << c;
        }
    }
    result << '"';
    return result;
}

TString FormatInteger(TInt128 value) {
    return value < 0 ? ToString(static_cast<i64>(value)) : ToString(static_cast<ui64>(value));
}

template <typename T>
TString FormatFloat(T value) {
    if (std::isnan(value)) {
        return "nan";
    }
    if (std::isinf(value)) {
        return value < 0 ? "-inf" : "inf";
    }
    return FloatToString(value);
}

std::optional<TBoundary> ParseBoundary(const TExprNode& node, bool isLeft) {
    if (!node.IsList() || node.ChildrenSize() < 2 || !node.Tail().IsCallable("Int32")) {
        return std::nullopt;
    }
    i32 included;
    if (!TryFromString(node.Tail().Head().Content(), included)) {
        return std::nullopt;
    }
    TVector<TBoundaryElement> elements;
    for (size_t i = 0; i + 1 < node.ChildrenSize(); ++i) {
        // An empty outer Optional is an infinity. Only Just of an empty Optional, or an empty Optional cast to a nested
        // Optional, is a finite NULL.
        const auto& element = node.ChildPtr(i);
        if (element->IsCallable("Nothing") && element->ChildrenSize() == 1) {
            elements.push_back({.Infinite = !element->Head().IsCallable("PgType")});
            continue;
        }
        if (element->IsCallable({"SafeCast", "StrictCast"}) && element->ChildrenSize() == 2 && element->Head().IsCallable("Nothing")) {
            const TExprNode* type = &element->Tail();
            size_t depth = 0;
            while (type->IsCallable("OptionalType") && type->ChildrenSize() == 1) {
                ++depth;
                type = &type->Head();
            }
            if (!depth) {
                return std::nullopt;
            }
            elements.push_back({.Infinite = depth == 1});
            continue;
        }
        auto value = ParseKeyValue(element);
        if (!value) {
            return std::nullopt;
        }
        elements.push_back({.Value = std::move(*value)});
    }
    return MakeBoundary(elements, included != 0, isLeft);
}

// Columns past a boundary are unconstrained, with the same sign as an infinity in the boundary.
TString FormatBoundary(const TBoundary& boundary, bool isLeft, TConstArrayRef<TString> keyColumns) {
    const i32 tailSign = GetInfSign(boundary.HasPrefix(), boundary.Included != 0, isLeft);
    TStringBuilder result;
    result << '{';
    for (size_t i = 0; i < keyColumns.size(); ++i) {
        if (i) {
            result << ", ";
        }
        result << SplitAliasedMemberName(keyColumns[i]).second << ": ";
        const i32 inf = i < boundary.Components.size() ? boundary.Components[i].Inf : tailSign;
        if (inf == 0) {
            result << FormatKeyValue(boundary.Components[i].Value);
        } else {
            result << (inf < 0 ? "-∞" : "+∞");
        }
    }
    result << '}';
    return result;
}

// Infinities are never read, so a boundary given or padded with them is shown as open.
bool IsBoundaryRead(const TBoundary& boundary, size_t width) {
    return boundary.Included != 0 && boundary.Components.size() == width
        && AllOf(boundary.Components, [](const TComponent& component) { return component.Inf == 0; });
}

std::optional<TKeyValue> ParseKeyValue(const TExprNode::TPtr& node) {
    if (node->IsCallable("Nothing")) {
        return TKeyValue{};
    }
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return ParseKeyValue(node->HeadPtr());
    }
    if (node->IsCallable("ToString") && node->ChildrenSize() == 1) {
        auto value = ParseKeyValue(node->HeadPtr());
        if (value && value->Kind == TKeyValue::EKind::Data && value->Slot == EDataSlot::Utf8) {
            value->Slot = EDataSlot::String;
            return value;
        }
        return std::nullopt;
    }
    if (TCoDataCtor::Match(node.Get())) {
        return ParseData(*node);
    }
    if (const auto pg = TMaybeNode<TCoPgConst>(node)) {
        const auto type = pg.Cast().Type().Ptr();
        if (!type->IsCallable("PgType") || type->ChildrenSize() != 1) {
            return std::nullopt;
        }
        return TKeyValue{.Kind = TKeyValue::EKind::Pg, .PgType = TString(type->Head().Content()),
                         .Bytes = TString(pg.Cast().Value().Value())};
    }
    return std::nullopt;
}

TString FormatKeyValue(const TKeyValue& value) {
    switch (value.Kind) {
        case TKeyValue::EKind::Null:
            return "NULL";
        case TKeyValue::EKind::Parameter:
            return value.Bytes;
        case TKeyValue::EKind::Pg:
            return IsPgNumeric(value.PgType) ? value.Bytes : QuoteText(value.Bytes);
        case TKeyValue::EKind::Data:
            break;
    }
    switch (value.Slot) {
        case EDataSlot::String:
        case EDataSlot::Utf8:
            return QuoteText(value.Bytes);
        case EDataSlot::Uuid:
            return NUuid::UuidBytesToString(value.Bytes);
        case EDataSlot::Bool:
            return value.Value.Get<bool>() ? "true" : "false";
        case EDataSlot::Float:
            return FormatFloat(value.Value.Get<float>());
        case EDataSlot::Double:
            return FormatFloat(value.Value.Get<double>());
        case EDataSlot::Decimal:
            return NDecimal::ToString(value.Value.GetInt128(), value.Precision, value.Scale);
        case EDataSlot::Date:
            return TInstant::Days(value.Value.Get<ui16>()).FormatGmTime("%Y-%m-%d");
        case EDataSlot::Datetime:
            return TInstant::Seconds(value.Value.Get<ui32>()).ToStringUpToSeconds();
        case EDataSlot::Timestamp:
            return TInstant::MicroSeconds(value.Value.Get<ui64>()).ToString();
        default:
            return FormatInteger(*GetInteger(value));
    }
}

i32 GetInfSign(bool hasPrefix, bool isIncluded, bool isLeft) {
    if (!hasPrefix || isIncluded) {
        return isLeft ? -1 : 1;
    }
    return isLeft ? 1 : -1;
}

std::optional<TBoundary> MakeBoundary(const TVector<TBoundaryElement>& elements, bool included, bool isLeft) {
    TBoundary result;
    const bool hasPrefix = !elements.empty() && !elements.front().Infinite;
    bool tail = false;
    for (const auto& element : elements) {
        tail = tail || element.Infinite;
        if (!element.Infinite && tail) {
            return std::nullopt;
        }
        result.Components.push_back({
            .Inf = element.Infinite ? GetInfSign(hasPrefix, included, isLeft) : 0,
            .Value = element.Value,
        });
    }
    result.Included = included ? (isLeft ? -1 : 1) : 0;
    return result;
}

std::optional<TRangeList> ParseAsRange(const TExprNode& node, size_t* expandedBytes = nullptr) {
    if (node.ChildrenSize() > MaxEvaluatedRangeInputs) {
        return std::nullopt;
    }
    size_t literalBytes = 0;
    TRangeList result;
    for (const auto& range : node.Children()) {
        if (!range->IsList() || range->ChildrenSize() != 2) {
            return std::nullopt;
        }
        // Count expanded literal payload, including repeated references to shared values. The AST
        // budget alone cannot bound the copies made while parsing a compact, shared expression.
        for (const auto& bound : range->Children()) {
            if (!bound->IsList() || bound->ChildrenSize() < 2 || bound->ChildrenSize() > 65) {
                return std::nullopt;
            }
            for (size_t i = 0; i + 1 < bound->ChildrenSize(); ++i) {
                const auto* value = bound->Child(i);
                size_t depth = 0;
                while (value->IsCallable({"Just", "ToString"}) && value->ChildrenSize() == 1) {
                    if (++depth > 64) {
                        return std::nullopt;
                    }
                    value = &value->Head();
                }
                if (value->ChildrenSize() && value->Head().IsAtom()) {
                    literalBytes += value->Head().Content().size();
                    if (literalBytes > MaxRangeEvaluationBytes) {
                        return std::nullopt;
                    }
                }
            }
        }
        auto left = ParseBoundary(range->Head(), /*isLeft=*/true);
        auto right = ParseBoundary(range->Tail(), /*isLeft=*/false);
        if (!left || !right || left->Components.size() != right->Components.size()) {
            return std::nullopt;
        }
        if (!result.Ranges.empty() && left->Components.size() != result.Width) {
            return std::nullopt;
        }
        result.Width = left->Components.size();
        result.Ranges.push_back({std::move(*left), std::move(*right)});
    }
    if (expandedBytes) {
        *expandedBytes = literalBytes;
    }
    return result;
}

TRangeList ParseRangeEmpty(const TExprNode& node) {
    if (node.ChildrenSize() == 0) {
        return TRangeList{};
    }
    if (node.Head().IsCallable("TupleType")) {
        return TRangeList{.Width = node.Head().ChildrenSize()};
    }
    return TRangeList{.Width = 1};
}

TString FormatRange(const TRange& range, TConstArrayRef<TString> keyColumns) {
    return TStringBuilder()
        << (IsBoundaryRead(range.Left, keyColumns.size()) ? '[' : '(') << FormatBoundary(range.Left, true, keyColumns) << ", "
        << FormatBoundary(range.Right, false, keyColumns) << (IsBoundaryRead(range.Right, keyColumns.size()) ? ']' : ')');
}

// The ranges of IN as the predicate extractor builds them:
//   IfPresent(list, collection -> RangeUnion of the ranges of every item of the collection, or the full range
//             when there are more items than the limit, RangeEmpty(type)).
// The ranges of an item are RangeMultiply(limit, RangeFor(op, value, type), ...), one RangeFor per key column. For IN
// over single values the value is the item. For IN over tuples it is Nth(item, i), after a Map puts the elements of
// every tuple in key order.
struct TSqlInColumn {
    TStringBuf Op;
    NYql::TExprNode::TPtr Type;
    std::optional<ui32> Element; // the element of a tuple in key order, or the whole item
};

struct TSqlInRanges {
    NYql::TExprNode::TPtr List;
    std::optional<NYql::TExprNode::TListType> Items; // when the list is a literal
    TVector<TSqlInColumn> Columns;
    ui64 ItemLimit = 0;
    TVector<ui32> KeyOrder; // the element of a tuple for every element in key order, when the tuples are reordered
    std::optional<size_t> TupleSize; // the original tuple width, before selecting/reordering key columns

    // The value of a column in an item, null when the item is not a literal tuple.
    NYql::TExprNode::TPtr GetValue(const NYql::TExprNode::TPtr& item, const TSqlInColumn& column) const {
        if (!column.Element) {
            return item;
        }
        ui32 element = *column.Element;
        if (!KeyOrder.empty()) {
            if (element >= KeyOrder.size()) {
                return nullptr;
            }
            element = KeyOrder[element];
        }
        if (!item->IsList() || element >= item->ChildrenSize()) {
            return nullptr;
        }
        return item->ChildPtr(element);
    }
};

bool IsArgumentElement(const TExprNode& node) {
    return node.IsCallable("Nth") && node.ChildrenSize() == 2 && node.Head().Type() == TExprNode::Argument;
}

// The items of a literal list: AsList, a tuple, or a tuple made a list by Map(tuple, t -> AsList(Nth(t, i), ...)).
std::optional<TExprNode::TListType> ParseLiteralList(const TExprNode::TPtr& node) {
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return ParseLiteralList(node->HeadPtr());
    }
    if (node->IsCallable("AsList") || node->IsList()) {
        return node->ChildrenList();
    }
    if (node->IsCallable("Map") && node->ChildrenSize() == 2 && node->Tail().IsLambda()) {
        const auto tuple = ParseLiteralList(node->HeadPtr());
        const auto& lambda = node->Tail();
        const auto& body = lambda.Tail();
        if (!tuple || lambda.Head().ChildrenSize() != 1 || !body.IsCallable("AsList")) {
            return std::nullopt;
        }
        TExprNode::TListType result;
        for (const auto& item : body.Children()) {
            ui32 index;
            if (!item->IsCallable("Nth") || item->ChildrenSize() != 2 || item->Head().Type() != TExprNode::Argument
                || item->HeadPtr() != lambda.Head().HeadPtr()
                || !TryFromString(item->Tail().Content(), index) || index >= tuple->size()) {
                return std::nullopt;
            }
            result.push_back((*tuple)[index]);
        }
        return result;
    }
    return std::nullopt;
}

std::optional<TSqlInRanges> MatchSqlInRanges(const TExprNode::TPtr& node) {
    if (!node->IsCallable("IfPresent") || node->ChildrenSize() != 3 || !node->Child(1)->IsLambda()
        || !node->Child(2)->IsCallable("RangeEmpty")) {
        return std::nullopt;
    }
    const auto& body = node->Child(1)->TailPtr();

    auto isItemRangeFor = [](const TExprNode& child) {
        return child.IsCallable("RangeFor") && child.ChildrenSize() == 3 && child.Head().IsAtom({"==", "==="})
            && (child.Child(1)->Type() == TExprNode::Argument || IsArgumentElement(*child.Child(1)));
    };
    const auto itemRanges = FindNode(body, [&](const TExprNode::TPtr& child) {
        if (!child->IsCallable("RangeMultiply") || child->ChildrenSize() < 2) {
            return false;
        }
        for (size_t i = 1; i < child->ChildrenSize(); ++i) {
            if (!isItemRangeFor(*child->Child(i))) {
                return false;
            }
        }
        return true;
    });
    TSqlInRanges result{.List = node->HeadPtr(), .Items = ParseLiteralList(node->HeadPtr())};
    if (!itemRanges || !itemRanges->Head().IsCallable("Uint64")
        || !TryFromString(itemRanges->Head().Head().Content(), result.ItemLimit)) {
        return std::nullopt;
    }
    for (size_t i = 1; i < itemRanges->ChildrenSize(); ++i) {
        const auto& rangeFor = *itemRanges->Child(i);
        TSqlInColumn column{.Op = rangeFor.Head().Content(), .Type = rangeFor.TailPtr()};
        if (IsArgumentElement(*rangeFor.Child(1))) {
            ui32 element;
            if (!TryFromString(rangeFor.Child(1)->Tail().Content(), element)) {
                return std::nullopt;
            }
            column.Element = element;
        }
        result.Columns.push_back(std::move(column));
    }

    // Map(tuples, t -> '(Nth(t, i), ...)) puts the elements of every tuple in key order.
    const auto keyOrder = FindNode(body, [](const TExprNode::TPtr& child) {
        if (!child->IsCallable("Map") || child->ChildrenSize() != 2 || !child->Tail().IsLambda() || !child->Tail().Tail().IsList()) {
            return false;
        }
        return AllOf(child->Tail().Tail().Children(), [](const TExprNode::TPtr& element) { return IsArgumentElement(*element); });
    });
    if (keyOrder) {
        const auto& input = keyOrder->Head();
        if (input.IsCallable("FlatMap") && input.ChildrenSize() == 2 && input.Tail().IsLambda()) {
            const auto& cast = input.Tail().Tail();
            if (cast.IsCallable("StrictCast") && cast.ChildrenSize() == 2
                && cast.Tail().IsCallable("OptionalType") && cast.Tail().ChildrenSize() == 1
                && cast.Tail().Head().IsCallable("TupleType")) {
                result.TupleSize = cast.Tail().Head().ChildrenSize();
            }
        }
        for (const auto& element : keyOrder->Tail().Tail().Children()) {
            ui32 index;
            if (!TryFromString(element->Tail().Content(), index)) {
                return std::nullopt;
            }
            result.KeyOrder.push_back(index);
        }
    }

    return result;
}

TString DescribeExprValue(const TExprNode::TPtr& node) {
    if (const auto param = TMaybeNode<TCoParameter>(node)) {
        return TString(param.Cast().Name().Value());
    }
    if (const auto value = ParseKeyValue(node)) {
        return FormatKeyValue(*value);
    }
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return DescribeExprValue(node->HeadPtr());
    }
    if (node->IsCallable("Map") && node->ChildrenSize() == 2 && node->Tail().IsLambda()
        && node->Tail().Head().ChildrenSize() == 1
        && node->Tail().TailPtr() == node->Tail().Head().HeadPtr()) {
        // The extractor wraps an optional list in an identity Map.
        return DescribeExprValue(node->HeadPtr());
    }
    if (node->IsCallable({"SafeCast", "StrictCast", "Convert", "ToString"}) && node->ChildrenSize() > 0) {
        return TStringBuilder() << node->Content() << '(' << DescribeExprValue(node->HeadPtr()) << ')';
    }
    return TStringBuilder() << (node->IsCallable() ? node->Content() : "expr") << "(…)";
}

// The values one key column takes: a set of points, an interval, or another set described as text.
struct TColumnSet {
    enum class EKind {
        Points,
        Interval,
        Other,
    };

    EKind Kind = EKind::Other;
    TVector<TString> Points;
    std::optional<TString> Lower; // -∞ when not set
    bool LowerIncluded = false;
    std::optional<TString> Upper; // +∞ when not set
    bool UpperIncluded = false;
    TString Text;
    bool Compound = false; // Text is a union or an intersection of sets

    TString Format() const {
        TStringBuilder result;
        if (Kind == EKind::Points) {
            result << '{';
            for (size_t i = 0; i < Min(Points.size(), MaxDescribedPoints); ++i) {
                result << (i ? ", " : "") << Points[i];
            }
            if (Points.size() > MaxDescribedPoints) {
                result << ", … " << Points.size() - MaxDescribedPoints << " more";
            }
            result << '}';
        } else if (Kind == EKind::Interval) {
            if (Lower) {
                result << (LowerIncluded ? '[' : '(') << *Lower;
            } else {
                result << "(-∞";
            }
            result << ", ";
            if (Upper) {
                result << *Upper << (UpperIncluded ? ']' : ')');
            } else {
                result << "+∞)";
            }
        } else {
            result << Text;
        }
        return result;
    }

    TString FormatOperand() const {
        return Compound ? TString(TStringBuilder() << '(' << Format() << ')') : Format();
    }
};

std::optional<TColumnSet> DescribeRangeFor(const TExprNode& node) {
    using EKind = TColumnSet::EKind;
    const auto op = node.Head().Content();
    const auto value = DescribeExprValue(node.ChildPtr(1));
    if (op == "==" || op == "===") {
        return TColumnSet{.Kind = EKind::Points, .Points = {value}};
    }
    if (op == "<" || op == "<=") {
        return TColumnSet{.Kind = EKind::Interval, .Upper = value, .UpperIncluded = op == "<="};
    }
    if (op == ">" || op == ">=") {
        return TColumnSet{.Kind = EKind::Interval, .Lower = value, .LowerIncluded = op == ">="};
    }
    if (op == "!=") {
        return TColumnSet{.Text = TStringBuilder() << "(-∞, " << value << ") ∪ (" << value << ", +∞)", .Compound = true};
    }
    if (op == "Exists") {
        // NULL sorts before every other value.
        return TColumnSet{.Kind = EKind::Interval, .Lower = "NULL"};
    }
    if (op == "NotExists") {
        return TColumnSet{.Kind = EKind::Points, .Points = {"NULL"}};
    }
    if (op == "StartsWith" || op == "NotStartsWith") {
        return TColumnSet{.Text = TStringBuilder() << (op == "NotStartsWith" ? "NOT " : "") << "StartsWith(" << value << ')'};
    }
    return std::nullopt;
}

// The values of IN: single values, or tuples in key order.
TColumnSet DescribeSqlIn(const TSqlInRanges& in) {
    if (!in.Items) {
        // A list known only at runtime, usually a parameter.
        return TColumnSet{.Text = DescribeExprValue(in.List)};
    }
    // Describe the input set. Its length alone cannot establish a range-limit overflow: NULLs and values
    // that do not fit the key type may produce no ranges. Only MiniKQL determines the actual scan bounds.
    TColumnSet result{.Kind = TColumnSet::EKind::Points};
    THashSet<TString> seen;
    for (const auto& item : *in.Items) {
        TStringBuilder point;
        for (size_t i = 0; i < in.Columns.size(); ++i) {
            const auto value = in.GetValue(item, in.Columns[i]);
            point << (i ? ", " : "") << (value ? DescribeExprValue(value) : TString("…"));
        }
        const TString text = in.Columns.size() > 1 ? TString(TStringBuilder() << '(' << point << ')') : TString(point);
        if (seen.insert(text).second) {
            result.Points.push_back(text);
        }
    }
    return result;
}

TColumnSet JoinSets(const TVector<TColumnSet>& sets, TStringBuf op) {
    TColumnSet result{.Compound = true};
    TStringBuilder text;
    for (size_t i = 0; i < sets.size(); ++i) {
        text << (i ? op : "") << sets[i].FormatOperand();
    }
    result.Text = text;
    return result;
}

TColumnSet UniteSets(const TVector<TColumnSet>& sets) {
    if (AllOf(sets, [](const TColumnSet& set) { return set.Kind == TColumnSet::EKind::Points; })) {
        TColumnSet result{.Kind = TColumnSet::EKind::Points};
        THashSet<TString> seen;
        for (const auto& set : sets) {
            for (const auto& point : set.Points) {
                if (seen.insert(point).second) {
                    result.Points.push_back(point);
                }
            }
        }
        return result;
    }
    return JoinSets(sets, " ∪ ");
}

TColumnSet IntersectSets(const TVector<TColumnSet>& sets) {
    // Half-bounded intervals with one lower and one upper bound are one interval.
    TColumnSet interval{.Kind = TColumnSet::EKind::Interval};
    for (const auto& set : sets) {
        if (set.Kind != TColumnSet::EKind::Interval || (set.Lower && interval.Lower) || (set.Upper && interval.Upper)) {
            return JoinSets(sets, " ∩ ");
        }
        if (set.Lower) {
            interval.Lower = set.Lower;
            interval.LowerIncluded = set.LowerIncluded;
        }
        if (set.Upper) {
            interval.Upper = set.Upper;
            interval.UpperIncluded = set.UpperIncluded;
        }
    }
    return interval;
}

// Describes a ranges expression without computing it. Every node covers some key columns starting from a given one:
// RangeFor covers one, RangeMultiply puts its children one after another, RangeUnion and RangeIntersect combine
// children over the same columns. The RangeMultiply limit is not applied, so past it the read is coarser than shown.
class TRangesDescriber {
public:
    explicit TRangesDescriber(TConstArrayRef<TString> keyColumns)
        : KeyColumns_(keyColumns)
    {}

    std::optional<TString> Describe(const TExprNode::TPtr& node) {
        const auto description = DescribeNode(node, 0);
        if (!description) {
            return std::nullopt;
        }
        return Render(*description, 0, EOp::None);
    }

private:
    enum class EOp {
        None,
        Union,
        Intersect,
        Product,
        Condition,
    };

    struct TDescription {
        size_t Width = 0;
        std::optional<TColumnSet> Column; // the description of a single column
        TString Text;
        EOp Op = EOp::None;
        bool Unconstrained = false; // the full range, which a product leaves out
    };

    TString ColumnName(size_t column) const {
        return SplitAliasedMemberName(KeyColumns_[column]).second;
    }

    TString Render(const TDescription& description, size_t column, EOp parent) const {
        if (description.Column) {
            const TString text = TStringBuilder() << ColumnName(column) << " ∈ " << description.Column->Format();
            return description.Column->Compound && parent != EOp::None ? TString(TStringBuilder() << '(' << text << ')') : text;
        }
        if (description.Op != EOp::None && parent != EOp::None && description.Op != parent) {
            return TStringBuilder() << '(' << description.Text << ')';
        }
        return description.Text;
    }

    std::optional<TDescription> DescribeNode(const TExprNode::TPtr& node, size_t column, size_t depth = 0) {
        if (++Steps_ > MaxRangeEvaluationNodes || depth > 64) return std::nullopt;
        if (const auto in = MatchSqlInRanges(node)) {
            const size_t width = in->Columns.size();
            if (column + width > KeyColumns_.size()) {
                return std::nullopt;
            }
            auto set = DescribeSqlIn(*in);
            if (!in->Items && in->Columns.front().Element) {
                // The collection still contains tuples in their original order. Put the column names in that
                // order too; a projection or duplicate component cannot be expressed as tuple membership.
                if (!in->TupleSize || *in->TupleSize != width) {
                    return std::nullopt;
                }
                TVector<TString> names(width);
                for (size_t i = 0; i < width; ++i) {
                    const auto element = in->Columns[i].Element;
                    if (!element || *element >= in->KeyOrder.size()) {
                        return std::nullopt;
                    }
                    const auto source = in->KeyOrder[*element];
                    if (source >= width || !names[source].empty()) {
                        return std::nullopt;
                    }
                    names[source] = ColumnName(column + i);
                }
                TStringBuilder text;
                text << '(';
                for (size_t i = 0; i < width; ++i) {
                    text << (i ? ", " : "") << names[i];
                }
                text << ") ∈ " << set.Format();
                return TDescription{.Width = width, .Text = text};
            }
            if (width == 1) {
                return TDescription{.Width = 1, .Column = std::move(set)};
            }
            TStringBuilder text;
            text << '(';
            for (size_t i = 0; i < width; ++i) {
                text << (i ? ", " : "") << ColumnName(column + i);
            }
            text << ") ∈ " << set.Format();
            return TDescription{.Width = width, .Text = text};
        }
        if (node->IsCallable({"RangeFinalize", "RangeToPg"}) && node->ChildrenSize() == 1) {
            return DescribeNode(node->HeadPtr(), column, depth + 1);
        }
        if (node->IsCallable("If") && node->ChildrenSize() == 3) {
            // A condition that does not depend on the row: the ranges if it holds, nothing otherwise.
            const auto condition = ParseKeyValue(node->HeadPtr());
            if (condition && condition->Kind == TKeyValue::EKind::Data && condition->Slot == NUdf::EDataSlot::Bool) {
                return DescribeNode(condition->Value.Get<bool>() ? node->ChildPtr(1) : node->ChildPtr(2), column, depth + 1);
            }
            if (!node->Child(2)->IsCallable("RangeEmpty")) {
                return std::nullopt;
            }
            auto ranges = DescribeNode(node->ChildPtr(1), column, depth + 1);
            if (!ranges) {
                return std::nullopt;
            }
            return TDescription{
                .Width = ranges->Width,
                .Text = TStringBuilder() << Render(*ranges, column, EOp::Condition) << " if " << DescribeExprValue(node->HeadPtr()),
                .Op = EOp::Condition,
            };
        }
        if (node->IsCallable("RangeFor") && node->ChildrenSize() == 3) {
            if (column >= KeyColumns_.size()) {
                return std::nullopt;
            }
            auto set = DescribeRangeFor(*node);
            if (!set) {
                return std::nullopt;
            }
            return TDescription{.Width = 1, .Column = std::move(*set)};
        }
        if (node->IsCallable("RangeEmpty")) {
            return TDescription{.Width = ParseRangeEmpty(*node).Width, .Text = "∅"};
        }
        if (node->IsCallable("AsRange")) {
            const auto ranges = ParseAsRange(*node);
            if (!ranges || column + ranges->Width > KeyColumns_.size()) {
                return std::nullopt;
            }
            const auto columns = KeyColumns_.subspan(column, ranges->Width);
            TStringBuilder text;
            for (size_t i = 0; i < ranges->Ranges.size(); ++i) {
                text << (i ? " ∪ " : "") << FormatRange(ranges->Ranges[i], columns);
            }
            const bool unconstrained = ranges->Ranges.size() == 1 && AllOf(ranges->Ranges.front().Left.Components, [](const TComponent& component) {
                return component.Inf != 0;
            }) && AllOf(ranges->Ranges.front().Right.Components, [](const TComponent& component) { return component.Inf != 0; });
            if (unconstrained && ranges->Width == 1) {
                return TDescription{.Width = 1, .Column = TColumnSet{.Kind = TColumnSet::EKind::Interval}, .Unconstrained = true};
            }
            return TDescription{
                .Width = ranges->Width,
                .Text = ranges->Ranges.empty() ? TString("∅") : TString(text),
                .Op = ranges->Ranges.size() > 1 ? EOp::Union : EOp::None,
                .Unconstrained = unconstrained,
            };
        }
        if (node->IsCallable({"RangeUnion", "RangeIntersect"}) && node->ChildrenSize() > 0) {
            const auto op = node->IsCallable("RangeUnion") ? EOp::Union : EOp::Intersect;
            TVector<TDescription> children;
            for (const auto& child : node->Children()) {
                auto description = DescribeNode(child, column, depth + 1);
                if (!description || (!children.empty() && description->Width != children.front().Width)) {
                    return std::nullopt;
                }
                children.push_back(std::move(*description));
            }
            if (children.size() == 1) {
                return std::move(children.front());
            }
            if (AllOf(children, [](const TDescription& child) { return child.Column.has_value(); })) {
                TVector<TColumnSet> sets;
                for (auto& child : children) {
                    sets.push_back(std::move(*child.Column));
                }
                return TDescription{.Width = children.front().Width, .Column = op == EOp::Union ? UniteSets(sets) : IntersectSets(sets)};
            }
            return Join(children, column, op, op == EOp::Union ? " ∪ " : " ∩ ");
        }
        if (node->IsCallable("RangeMultiply") && node->ChildrenSize() > 1) {
            TVector<TDescription> children;
            TVector<size_t> columns;
            size_t next = column;
            for (size_t i = 1; i < node->ChildrenSize(); ++i) {
                auto description = DescribeNode(node->ChildPtr(i), next, depth + 1);
                if (!description) {
                    return std::nullopt;
                }
                const size_t width = description->Width;
                // Columns past a constrained prefix are padded with the full range, which says nothing.
                if (!description->Unconstrained || children.empty()) {
                    columns.push_back(next);
                    children.push_back(std::move(*description));
                }
                next += width;
            }
            if (children.size() == 1) {
                children.front().Width = next - column;
                return std::move(children.front());
            }
            TDescription result{.Width = next - column, .Op = EOp::Product};
            TStringBuilder text;
            for (size_t i = 0; i < children.size(); ++i) {
                text << (i ? " × " : "") << Render(children[i], columns[i], EOp::Product);
            }
            result.Text = text;
            return result;
        }
        return std::nullopt;
    }

    TDescription Join(const TVector<TDescription>& children, size_t column, EOp op, TStringBuf separator) const {
        TStringBuilder text;
        for (size_t i = 0; i < children.size(); ++i) {
            text << (i ? separator : "") << Render(children[i], column, op);
        }
        return TDescription{.Width = children.front().Width, .Text = text, .Op = op};
    }

    const TConstArrayRef<TString> KeyColumns_;
    size_t Steps_ = 0;
};

std::optional<TString> DescribeRangesStructure(const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns) {
    if (!ranges || keyColumns.empty()) {
        return std::nullopt;
    }
    return TRangesDescriber(keyColumns).Describe(ranges);
}

// Interpretation is deliberately limited to literal bounds and the extractor's finite range algebra.
// It shares the formatter's values/bounds, never builds an executable graph, and rejects unsupported
// operations instead of guessing their result. In particular, floating point, Decimal and PG ordering
// and general expression/list evaluation are left to the structural description.
struct TUnsupportedRange {};

void RequireRange(bool condition) {
    if (!condition) {
        throw TUnsupportedRange{};
    }
}

bool IsIntegerSlot(EDataSlot slot) {
    return slot >= EDataSlot::Int8 && slot <= EDataSlot::Uint64;
}

std::optional<std::pair<TInt128, TInt128>> IntegerLimits(EDataSlot slot) {
    switch (slot) {
        case EDataSlot::Int8: return {{Min<i8>(), Max<i8>()}};
        case EDataSlot::Uint8: return {{0, Max<ui8>()}};
        case EDataSlot::Int16: return {{Min<i16>(), Max<i16>()}};
        case EDataSlot::Uint16: return {{0, Max<ui16>()}};
        case EDataSlot::Int32: return {{Min<i32>(), Max<i32>()}};
        case EDataSlot::Uint32: return {{0, Max<ui32>()}};
        case EDataSlot::Int64: return {{Min<i64>(), Max<i64>()}};
        case EDataSlot::Uint64: return {{0, Max<ui64>()}};
        case EDataSlot::Date: return {{0, NUdf::MAX_DATE - 1}};
        case EDataSlot::Datetime: return {{0, NUdf::MAX_DATETIME - 1}};
        case EDataSlot::Timestamp: return {{0, NUdf::MAX_TIMESTAMP - 1}};
        case EDataSlot::Date32: return {{NUdf::MIN_DATE32, NUdf::MAX_DATE32}};
        case EDataSlot::Datetime64: return {{NUdf::MIN_DATETIME64, NUdf::MAX_DATETIME64}};
        case EDataSlot::Timestamp64: return {{NUdf::MIN_TIMESTAMP64, NUdf::MAX_TIMESTAMP64}};
        default: return std::nullopt;
    }
}

TKeyValue IntegerValue(EDataSlot slot, TInt128 value) {
    TKeyValue result{.Kind = TKeyValue::EKind::Data, .Slot = slot};
    switch (slot) {
        case EDataSlot::Int8: result.Value = NUdf::TUnboxedValuePod(i8(value)); break;
        case EDataSlot::Uint8: result.Value = NUdf::TUnboxedValuePod(ui8(value)); break;
        case EDataSlot::Int16: result.Value = NUdf::TUnboxedValuePod(i16(value)); break;
        case EDataSlot::Uint16: result.Value = NUdf::TUnboxedValuePod(ui16(value)); break;
        case EDataSlot::Int32:
        case EDataSlot::Date32: result.Value = NUdf::TUnboxedValuePod(i32(value)); break;
        case EDataSlot::Uint32:
        case EDataSlot::Datetime: result.Value = NUdf::TUnboxedValuePod(ui32(value)); break;
        case EDataSlot::Date: result.Value = NUdf::TUnboxedValuePod(ui16(value)); break;
        case EDataSlot::Int64:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64: result.Value = NUdf::TUnboxedValuePod(i64(value)); break;
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp: result.Value = NUdf::TUnboxedValuePod(ui64(value)); break;
        default: throw TUnsupportedRange{};
    }
    return result;
}

bool IsInterpretedSlot(EDataSlot slot) {
    return IntegerLimits(slot) || slot == EDataSlot::Interval || slot == EDataSlot::Interval64
        || slot == EDataSlot::Bool || slot == EDataSlot::String || slot == EDataSlot::Utf8 || slot == EDataSlot::Uuid;
}

bool IsInterpretedValue(const TKeyValue& value) {
    return value.Kind == TKeyValue::EKind::Null || (value.Kind == TKeyValue::EKind::Data
        && IsInterpretedSlot(value.Slot) && (value.Slot != EDataSlot::Uuid || value.Bytes.size() == 16));
}

int CompareKeys(const TKeyValue& a, const TKeyValue& b) {
    RequireRange(IsInterpretedValue(a) && IsInterpretedValue(b));
    if (a.Kind == TKeyValue::EKind::Null || b.Kind == TKeyValue::EKind::Null) {
        return int(b.Kind == TKeyValue::EKind::Null) - int(a.Kind == TKeyValue::EKind::Null);
    }
    RequireRange(a.Slot == b.Slot);
    if (const auto av = GetInteger(a)) {
        const auto bv = *GetInteger(b);
        return (*av > bv) - (*av < bv);
    }
    if (a.Slot == EDataSlot::Bool) {
        return int(a.Value.Get<bool>()) - int(b.Value.Get<bool>());
    }
    return a.Bytes.compare(b.Bytes);
}

int CompareBounds(const TBoundary& a, const TBoundary& b, bool inclusion = true) {
    RequireRange(a.Components.size() == b.Components.size());
    for (size_t i = 0; i < a.Components.size(); ++i) {
        const auto& x = a.Components[i];
        const auto& y = b.Components[i];
        if (x.Inf != y.Inf) return x.Inf < y.Inf ? -1 : 1;
        if (x.Inf) break;
        if (const int cmp = CompareKeys(x.Value, y.Value)) return cmp;
    }
    return inclusion ? (a.Included > b.Included) - (a.Included < b.Included) : 0;
}

bool EmptyRange(const TRange& range) {
    return CompareBounds(range.Left, range.Right) >= 0 ||
        (CompareBounds(range.Left, range.Right, false) == 0 && (!range.Left.Included || !range.Right.Included));
}

void NormalizeRanges(TRangeList& list) {
    RequireRange(list.Width <= 64 && list.Ranges.size() <= MaxEvaluatedRangeInputs);
    for (const auto& range : list.Ranges) {
        RequireRange(range.Left.Components.size() == list.Width && range.Right.Components.size() == list.Width);
        for (const auto* bound : {&range.Left, &range.Right}) {
            for (const auto& component : bound->Components) {
                RequireRange(component.Inf || IsInterpretedValue(component.Value));
            }
        }
    }
    Sort(list.Ranges, [](const TRange& a, const TRange& b) { return CompareBounds(a.Left, b.Left) < 0; });
    TVector<TRange> merged;
    for (auto& range : list.Ranges) {
        if (EmptyRange(range)) continue;
        if (!merged.empty() && CompareBounds(range.Left, merged.back().Right) < 0) {
            if (CompareBounds(range.Right, merged.back().Right) > 0) merged.back().Right = std::move(range.Right);
        } else {
            merged.push_back(std::move(range));
        }
    }
    list.Ranges = std::move(merged);
}

void FinalizeRanges(TRangeList& list) {
    for (auto& range : list.Ranges) {
        if (bool(range.Left.Included) == bool(range.Right.Included)) continue;
        size_t finite = 0;
        while (finite < list.Width && !range.Left.Components[finite].Inf && !range.Right.Components[finite].Inf) ++finite;
        if (!finite || (finite < list.Width && (!range.Left.Components[finite].Inf || !range.Right.Components[finite].Inf))) continue;
        bool equalPrefix = true;
        for (size_t i = 0; i + 1 < finite; ++i) {
            equalPrefix &= CompareKeys(range.Left.Components[i].Value, range.Right.Components[i].Value) == 0;
        }
        const auto& left = range.Left.Components[finite - 1].Value;
        const auto& right = range.Right.Components[finite - 1].Value;
        if (!equalPrefix || left.Kind != TKeyValue::EKind::Data || right.Kind != TKeyValue::EKind::Data
            || left.Slot != right.Slot || !IntegerLimits(left.Slot) || *GetInteger(right) - *GetInteger(left) != 1) continue;
        const auto point = range.Left.Included ? range.Left : range.Right;
        range.Left = range.Right = point;
        range.Left.Included = -1;
        range.Right.Included = 1;
        for (size_t i = finite; i < list.Width; ++i) {
            range.Left.Components[i].Inf = -1;
            range.Right.Components[i].Inf = 1;
        }
    }
}

struct TRangeKeyType {
    EDataSlot Slot;
    bool Nullable = false;
};

TRangeKeyType RangeKeyType(const TExprNode& node) {
    const TExprNode* type = &node;
    const bool nullable = type->IsCallable("OptionalType") && type->ChildrenSize() == 1;
    if (nullable) type = &type->Head();
    RequireRange(type->IsCallable("DataType") && type->ChildrenSize() == 1);
    const auto slot = NUdf::FindDataSlot(type->Head().Content());
    RequireRange(bool(slot));
    RequireRange(IsInterpretedSlot(*slot));
    return {*slot, nullable};
}

TKeyValue LiteralValue(const TExprNode::TPtr& node) {
    const auto value = ParseKeyValue(node);
    RequireRange(value && IsInterpretedValue(*value));
    return *value;
}

TRangeList InterpretRangeFor(TStringBuf op, TKeyValue value, const TExprNode& typeNode) {
    const auto type = RangeKeyType(typeNode);
    const TBoundary low{{type.Nullable ? TComponent{.Value = {}} : TComponent{.Inf = -1}}, 0};
    const TBoundary high{{TComponent{.Inf = 1}}, 0};
    if (op == "Exists" || op == "NotExists") {
        RequireRange(type.Nullable);
        return op == "Exists" ? TRangeList{{{low, high}}, 1}
            : TRangeList{{{TBoundary{{TComponent{}}, -1}, TBoundary{{TComponent{}}, 1}}}, 1};
    }
    RequireRange(op == "==" || op == "===" || op == "!=" || op == "<" || op == "<="
        || op == ">" || op == ">=" || op == "StartsWith" || op == "NotStartsWith");
    if (value.Kind == TKeyValue::EKind::Null) {
        RequireRange(op == "==" || op == "===");
        return {{}, 1};
    }
    if (IsIntegerSlot(type.Slot) && IsIntegerSlot(value.Slot)) {
        const auto integer = *GetInteger(value);
        const auto [min, max] = *IntegerLimits(type.Slot);
        if (integer < min || integer > max) {
            if (op == "!=") return {{{low, high}}, 1};
            if ((integer > max && (op == "<" || op == "<=")) || (integer < min && (op == ">" || op == ">="))) {
                value = IntegerValue(type.Slot, integer > max ? max : min);
                op = integer > max ? "<=" : ">=";
            } else {
                RequireRange(op == "==" || op == "===" || op == "<" || op == "<=" || op == ">" || op == ">=");
                return {{}, 1};
            }
        } else {
            value = IntegerValue(type.Slot, integer);
        }
    } else if ((type.Slot == EDataSlot::String || type.Slot == EDataSlot::Utf8)
        && (value.Slot == EDataSlot::String || value.Slot == EDataSlot::Utf8)) {
        RequireRange(type.Slot != EDataSlot::Utf8 || IsUtf(value.Bytes));
        value.Slot = type.Slot;
    } else {
        RequireRange(value.Slot == type.Slot);
    }
    if (const auto limits = IntegerLimits(value.Slot)) {
        RequireRange(*GetInteger(value) >= limits->first && *GetInteger(value) <= limits->second);
    }
    const TBoundary open{{TComponent{.Value = value}}, 0};
    const TBoundary left{{TComponent{.Value = value}}, -1};
    const TBoundary right{{TComponent{.Value = value}}, 1};
    if (op == "StartsWith" || op == "NotStartsWith") {
        RequireRange(type.Slot == EDataSlot::String || type.Slot == EDataSlot::Utf8);
        const std::string_view text(value.Bytes.data(), value.Bytes.size());
        const auto next = type.Slot == EDataSlot::Utf8 ? NYql::NextValidUtf8(text) : NYql::NextLexicographicString(text);
        TBoundary upper = high;
        if (next) {
            value.Bytes = TString(*next);
            upper = TBoundary{{TComponent{.Value = value}}, 0};
        }
        if (op == "StartsWith") return {{{left, upper}}, 1};
        TRangeList result{{{low, open}}, 1};
        if (next) {
            upper.Included = -1;
            result.Ranges.push_back({upper, high});
        }
        return result;
    }
    if (op == "===") {
        if (const auto limits = IntegerLimits(type.Slot); limits && *GetInteger(value) < limits->second) {
            return {{{left, TBoundary{{TComponent{.Value = IntegerValue(type.Slot, *GetInteger(value) + 1)}}, 0}}}, 1};
        }
        op = "==";
    }
    if (op == "==") return {{{left, right}}, 1};
    if (op == "!=") return {{{low, open}, {open, high}}, 1};
    if (op == "<") return {{{low, open}}, 1};
    if (op == "<=") return {{{low, right}}, 1};
    if (op == ">") return {{{open, high}}, 1};
    return {{{left, high}}, 1};
}

// Operations on range lists use the whole composite boundary, never independent per-column intervals.
TRangeList MultiplyRanges(TRangeList first, const TRangeList& second, ui64 limit) {
    RequireRange(first.Width + second.Width <= 64);
    TVector<TRange> result;
    for (const auto& a : first.Ranges) {
        if (second.Ranges.empty()) break;
        if (a.Left.Included && a.Right.Included && CompareBounds(a.Left, a.Right, false) == 0) {
            RequireRange(second.Ranges.size() <= Min<ui64>(limit, MaxEvaluatedRangeInputs) - result.size());
            for (const auto& b : second.Ranges) {
                auto append = [](TBoundary prefix, const TBoundary& suffix) {
                    if (!suffix.Components.front().Inf) prefix.Included = suffix.Included;
                    prefix.Components.insert(prefix.Components.end(), suffix.Components.begin(), suffix.Components.end());
                    return prefix;
                };
                result.push_back({append(a.Left, b.Left), append(a.Right, b.Right)});
            }
        } else {
            RequireRange(result.size() < Min<ui64>(limit, MaxEvaluatedRangeInputs));
            auto padded = a;
            for (size_t i = 0; i < second.Width; ++i) {
                padded.Left.Components.push_back({.Inf = GetInfSign(a.Left.HasPrefix(), a.Left.Included != 0, true)});
                padded.Right.Components.push_back({.Inf = GetInfSign(a.Right.HasPrefix(), a.Right.Included != 0, false)});
            }
            result.push_back(std::move(padded));
        }
    }
    return {std::move(result), first.Width + second.Width};
}

ui64 RangeCountLiteral(const TExprNode& node) {
    ui64 value = 0;
    RequireRange(node.IsCallable("Uint64") && node.ChildrenSize() == 1 && TryFromString(node.Head().Content(), value));
    return value;
}

class TRangeInterpreter {
public:
    TRangeList Evaluate(const TExprNode::TPtr& node) {
        RequireRange(++Steps_ <= MaxRangeEvaluationNodes * 2);
        TRangeList result;
        if (node->IsCallable("RangeFinalize") && node->ChildrenSize() == 1) {
            result = Evaluate(node->HeadPtr());
            FinalizeRanges(result);
        } else if (node->IsCallable("AsRange")) {
            size_t literalBytes = 0;
            auto list = ParseAsRange(*node, &literalBytes);
            RequireRange(bool(list));
            Spend(literalBytes);
            result = std::move(*list);
        } else if (node->IsCallable("RangeEmpty") && node->ChildrenSize() == 1) {
            result = ParseRangeEmpty(*node);
        } else if (node->IsCallable("RangeFor") && node->ChildrenSize() == 3) {
            result = InterpretRangeFor(node->Head().Content(), node->Head().IsAtom({"Exists", "NotExists"})
                ? TKeyValue{} : ReadLiteral(node->ChildPtr(1)), node->Tail());
        } else if (node->IsCallable({"RangeUnion", "RangeIntersect"}) && node->ChildrenSize()) {
            result = Evaluate(node->HeadPtr());
            for (size_t i = 1; i < node->ChildrenSize(); ++i) {
                auto next = Evaluate(node->ChildPtr(i));
                RequireRange(result.Width == next.Width);
                if (node->IsCallable("RangeUnion")) {
                    RequireRange(result.Ranges.size() + next.Ranges.size() <= MaxEvaluatedRangeInputs);
                    result.Ranges.insert(result.Ranges.end(), next.Ranges.begin(), next.Ranges.end());
                } else {
                    TVector<TRange> intersection;
                    size_t a = 0, b = 0;
                    while (a < result.Ranges.size() && b < next.Ranges.size()) {
                        const auto& x = result.Ranges[a];
                        const auto& y = next.Ranges[b];
                        TRange range{CompareBounds(x.Left, y.Left) > 0 ? x.Left : y.Left,
                                     CompareBounds(x.Right, y.Right) < 0 ? x.Right : y.Right};
                        if (!EmptyRange(range)) intersection.push_back(std::move(range));
                        if (CompareBounds(x.Right, y.Right) < 0) ++a; else ++b;
                    }
                    result.Ranges = std::move(intersection);
                }
                if (node->IsCallable("RangeIntersect")) Normalize(result);
            }
        } else if (node->IsCallable("RangeMultiply") && node->ChildrenSize() > 1) {
            const auto limit = RangeCountLiteral(node->Head());
            result = Evaluate(node->ChildPtr(1));
            RequireRange(node->ChildrenSize() > 2 || result.Ranges.size() <= limit);
            for (size_t i = 2; i < node->ChildrenSize(); ++i) result = Multiply(std::move(result), Evaluate(node->ChildPtr(i)), limit);
        } else if (node->IsCallable({"If", "IfStrict"}) && node->ChildrenSize() == 3) {
            const auto condition = ReadLiteral(node->HeadPtr());
            RequireRange(condition.Kind == TKeyValue::EKind::Data && condition.Slot == EDataSlot::Bool);
            result = Evaluate(node->ChildPtr(condition.Value.Get<bool>() ? 1 : 2));
        } else if (node->IsCallable("IfPresent")) {
            result = SqlIn(node);
        } else {
            throw TUnsupportedRange{};
        }
        Normalize(result);
        return result;
    }

private:
    void Spend(size_t work) {
        RequireRange(work <= RemainingWork_);
        RemainingWork_ -= work;
    }

    TKeyValue ReadLiteral(const TExprNode::TPtr& node) {
        const TExprNode* value = node.Get();
        while (value->IsCallable({"Just", "ToString"}) && value->ChildrenSize() == 1) value = &value->Head();
        if (TCoDataCtor::Match(value) && value->ChildrenSize()) Spend(value->Head().Content().size());
        return LiteralValue(node);
    }

    void Normalize(TRangeList& list) {
        Spend((list.Width + 1) * (list.Ranges.size() + 1));
        NormalizeRanges(list);
    }

    TRangeList Multiply(TRangeList first, const TRangeList& second, ui64 limit) {
        Spend((first.Width + second.Width + 1) * (first.Ranges.size() + second.Ranges.size() + 1));
        return MultiplyRanges(std::move(first), second, limit);
    }

    // Only accept the extractor's checked IN template. General Map/FlatMap/IfPresent programs are not interpreted.
    TRangeList SqlIn(const TExprNode::TPtr& node) {
        const auto in = MatchSqlInRanges(node);
        RequireRange(in && in->Items && in->Items->size() <= MaxEvaluatedRangeInputs);
        const auto& lambda = *node->Child(1);
        RequireRange(lambda.Head().ChildrenSize() == 1);
        const auto argument = lambda.Head().HeadPtr();
        const auto& body = lambda.Tail();
        RequireRange(body.IsCallable("IfStrict") && body.ChildrenSize() == 3);
        const auto& condition = body.Head();
        RequireRange(condition.IsCallable(">") && condition.ChildrenSize() == 2
            && condition.Head().IsCallable("Length") && condition.Head().ChildrenSize() == 1);
        const ui64 limit = RangeCountLiteral(condition.Tail());
        RequireRange(limit < Max<ui64>() && in->Items->size() <= limit);
        const auto points = condition.Head().HeadPtr();
        RequireRange(body.Tail().IsCallable("RangeUnion") && body.Tail().ChildrenSize() == 1 && body.Tail().HeadPtr() == points);
        RequireRange(points->IsCallable("Collect") && points->ChildrenSize() == 1);
        const auto& take = points->Head();
        RequireRange(take.IsCallable("Take") && take.ChildrenSize() == 2 && RangeCountLiteral(take.Tail()) == limit + 1);
        const auto& flatMap = take.Head();
        RequireRange(flatMap.IsCallable("FlatMap") && flatMap.ChildrenSize() == 2 && flatMap.Tail().IsLambda());
        const auto& itemLambda = flatMap.Tail();
        RequireRange(itemLambda.Head().ChildrenSize() == 1);
        const auto itemArg = itemLambda.Head().HeadPtr();
        const auto& multiply = itemLambda.Tail();
        RequireRange(multiply.IsCallable("RangeMultiply") && multiply.ChildrenSize() == in->Columns.size() + 1
            && RangeCountLiteral(multiply.Head()) == limit);
        const auto full = body.Child(1)->IsCallable("AsRange") ? ParseAsRange(*body.Child(1)) : std::nullopt;
        RequireRange(full && full->Width == in->Columns.size() && full->Ranges.size() == 1
            && AllOf(full->Ranges[0].Left.Components, [](const TComponent& c) { return c.Inf == -1; })
            && AllOf(full->Ranges[0].Right.Components, [](const TComponent& c) { return c.Inf == 1; }));
        RequireRange(ParseRangeEmpty(*node->Child(2)).Width == in->Columns.size());
        const TExprNode* tupleTypes = nullptr;
        if (flatMap.HeadPtr() != argument) {
            const auto& order = flatMap.Head();
            RequireRange(order.IsCallable("Map") && order.ChildrenSize() == 2 && order.Tail().IsLambda()
                && order.Tail().Head().ChildrenSize() == 1 && order.Tail().Tail().IsList()
                && order.Tail().Tail().ChildrenSize() == in->KeyOrder.size());
            const auto& casts = order.Head();
            RequireRange(casts.IsCallable("FlatMap") && casts.ChildrenSize() == 2 && casts.HeadPtr() == argument
                && casts.Tail().IsLambda() && casts.Tail().Head().ChildrenSize() == 1);
            const auto& cast = casts.Tail().Tail();
            RequireRange(cast.IsCallable("StrictCast") && cast.ChildrenSize() == 2 && cast.HeadPtr() == casts.Tail().Head().HeadPtr()
                && cast.Tail().IsCallable("OptionalType") && cast.Tail().ChildrenSize() == 1 && cast.Tail().Head().IsCallable("TupleType"));
            tupleTypes = &cast.Tail().Head();
            for (const auto& element : order.Tail().Tail().Children()) {
                RequireRange(IsArgumentElement(*element) && element->HeadPtr() == order.Tail().Head().HeadPtr());
            }
        } else {
            RequireRange(in->KeyOrder.empty());
        }
        for (size_t i = 0; i < in->Columns.size(); ++i) {
            const auto& range = *multiply.Child(i + 1);
            RequireRange(range.IsCallable("RangeFor") && range.ChildrenSize() == 3 && range.Head().IsAtom({"==", "==="}));
            const auto& value = *range.Child(1);
            RequireRange((range.ChildPtr(1) == itemArg && !in->Columns[i].Element)
                || (IsArgumentElement(value) && value.HeadPtr() == itemArg && in->Columns[i].Element
                    && FromString<ui32>(value.Tail().Content()) == *in->Columns[i].Element));
        }
        TRangeList result{.Width = in->Columns.size()};
        for (const auto& item : *in->Items) {
            RequireRange(++Steps_ <= MaxRangeEvaluationNodes * 2);
            if (tupleTypes) {
                RequireRange(item->IsList() && item->ChildrenSize() == tupleTypes->ChildrenSize());
                // A tuple cast may discard the entire item, including components not used in the key.
                // Only identity casts of literal tuples are supported here.
                for (size_t i = 0; i < item->ChildrenSize(); ++i) {
                    const auto value = ReadLiteral(item->ChildPtr(i));
                    const auto type = RangeKeyType(*tupleTypes->Child(i));
                    RequireRange(value.Kind == TKeyValue::EKind::Data && value.Slot == type.Slot);
                }
            }
            TRangeList row;
            for (size_t i = 0; i < in->Columns.size(); ++i) {
                const auto value = in->GetValue(item, in->Columns[i]);
                RequireRange(bool(value));
                const auto& range = *multiply.Child(i + 1);
                auto column = InterpretRangeFor(range.Head().Content(), ReadLiteral(value), range.Tail());
                Normalize(column);
                row = i ? Multiply(std::move(row), column, limit) : std::move(column);
            }
            RequireRange(result.Ranges.size() + row.Ranges.size() <= Min<ui64>(limit, MaxEvaluatedRangeInputs));
            result.Ranges.insert(result.Ranges.end(), row.Ranges.begin(), row.Ranges.end());
        }
        return result;
    }

    size_t Steps_ = 0;
    size_t RemainingWork_ = 64 * 1024;
};

std::optional<TRangeList> InterpretRanges(const TExprNode::TPtr& ranges) {
    try {
        return TRangeInterpreter().Evaluate(ranges);
    } catch (const TUnsupportedRange&) {
        return std::nullopt;
    }
}

// Only literal constructors and the finite range/list operations used by the extractor. Reject external and
// nondeterministic calls, including parameters, clocks, random functions, UDFs, reads and list generators.
bool IsSupportedCallable(const TExprNode& node) {
    if (node.IsCallable("DataType")) {
        const auto slot = node.ChildrenSize() ? NUdf::FindDataSlot(node.Head().Content()) : TMaybe<EDataSlot>{};
        return slot && IsInterpretedSlot(*slot);
    }
    if (const auto slot = NUdf::FindDataSlot(node.Content())) {
        return IsInterpretedSlot(*slot);
    }
    return node.IsCallable({
        "OptionalType", "TupleType", "ListType", "VoidType",
        "Nothing", "Just", "Void", "AsList", "List", "SafeCast", "StrictCast", "ToString",
        "RangeFor", "RangeEmpty", "AsRange", "RangeUnion", "RangeIntersect", "RangeMultiply", "RangeFinalize",
        "If", "IfStrict", "IfPresent", "Map", "FlatMap", "Nth", "Take", "Collect", "Length",
        "<", "<=", ">", ">=", "==", "!=", "And", "Or", "Not", "Exists", "Coalesce", "Ensure"
    });
}

bool FitsInputBudget(const TExprNode& root) {
    TVector<std::pair<const TExprNode*, size_t>> pending{{&root, 0}};
    THashMap<const TExprNode*, size_t> depths;
    size_t bytes = 0;
    size_t edges = 0;
    while (!pending.empty()) {
        const auto [node, depth] = pending.back();
        pending.pop_back();
        if (depth > 64) {
            return false;
        }
        const auto [it, inserted] = depths.emplace(node, depth);
        if (!inserted) {
            if (it->second >= depth) {
                continue;
            }
            // A shared subtree reached through a longer path must also respect the depth limit.
            it->second = depth;
        } else {
            bytes += node->Content().size();
            edges += node->ChildrenSize();
        }
        if (depths.size() > MaxRangeEvaluationNodes || edges > MaxRangeEvaluationNodes || bytes > MaxRangeEvaluationBytes
            || node->Type() == TExprNode::World || (node->IsCallable() && !IsSupportedCallable(*node))) {
            return false;
        }
        for (const auto& child : node->Children()) {
            pending.emplace_back(child.Get(), depth + 1);
        }
    }
    return true;
}

// A bound of a literal range, whose values BuildLiteralRange makes literals, NULLs or parameters.
TBoundary ParseLiteralBound(const TExprNode& bound, bool isLeft) {
    TBoundary result;
    for (const auto& node : bound.Children()) {
        TKeyValue value{.Kind = TKeyValue::EKind::Parameter, .Bytes = "n/a"};
        if (const auto param = TMaybeNode<TCoParameter>(node)) {
            value.Bytes = param.Cast().Name().Value();
        } else if (auto literal = ParseKeyValue(node)) {
            value = std::move(*literal);
        }
        result.Components.push_back({.Value = std::move(value)});
    }
    result.Included = bound.IsCallable("KqlKeyInc") ? (isLeft ? -1 : 1) : 0;
    return result;
}

TString DescribeLiteralKeyRange(const TExprNode::TPtr& keyRange, const TVector<TString>& keyColumns) {
    // A KqlKeyRange is a tuple of the two bounds.
    Y_ENSURE(keyRange->IsList() && keyRange->ChildrenSize() == 2);
    return FormatRange({ParseLiteralBound(keyRange->Head(), /*isLeft=*/true), ParseLiteralBound(keyRange->Tail(), /*isLeft=*/false)},
                       keyColumns);
}

} // namespace

TReadRangesDescription DescribeReadRanges(const TExprNode::TPtr& literalRange,
    const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, TMaybe<size_t> expectedMaxRanges) {
    if (literalRange) {
        return {.Ranges = TVector<TString>{DescribeLiteralKeyRange(literalRange, keyColumns)}};
    }
    if (ranges && expectedMaxRanges && *expectedMaxRanges <= MaxEvaluatedRangeInputs && FitsInputBudget(*ranges)) {
        if (const auto list = InterpretRanges(ranges); list && list->Width <= keyColumns.size()
            && list->Ranges.size() <= MaxExplainedReadRanges) {
            TVector<TString> result;
            for (const auto& range : list->Ranges) {
                result.push_back(FormatRange(range, keyColumns));
            }
            return {.Ranges = std::move(result)};
        }
    }
    return {.Expression = DescribeRangesStructure(ranges, keyColumns)};
}

} // namespace NKikimr::NKqp::NOpt
