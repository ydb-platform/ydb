#include "kqp_read_ranges_explain.h"

#include "../kqp_info_unit.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
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
#include <util/generic/set.h>
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

bool FitsInputBudget(const TExprNode& root, bool interpreted = true);

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

// The extractor maps IN items to point ranges, reorders tuple keys and caps expansion.
struct TSqlInColumn {
    std::optional<ui32> Element; // the element of a tuple in key order, or the whole item
};

struct TSqlInRanges {
    NYql::TExprNode::TPtr List;
    std::optional<NYql::TExprNode::TListType> Items; // when the list is a literal
    TVector<TSqlInColumn> Columns;
    TVector<ui32> KeyOrder; // the element of a tuple for every element in key order, when the tuples are reordered
    std::optional<size_t> TupleSize; // original tuple width
    NYql::TExprNode::TPtr ItemRanges;
    const NYql::TExprNode* TupleTypes = nullptr;
    ui64 Limit = 0;

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
        || !node->Child(2)->IsCallable("RangeEmpty")) return std::nullopt;
    const auto& lambda = *node->Child(1);
    if (lambda.Head().ChildrenSize() != 1) return std::nullopt;
    const auto argument = lambda.Head().HeadPtr();
    const auto& body = lambda.Tail();
    if (!body.IsCallable("IfStrict") || body.ChildrenSize() != 3) return std::nullopt;
    const auto& condition = body.Head();
    if (!condition.IsCallable(">") || condition.ChildrenSize() != 2
        || !condition.Head().IsCallable("Length") || condition.Head().ChildrenSize() != 1) return std::nullopt;
    const auto count = [](const TExprNode& value) -> std::optional<ui64> {
        ui64 result;
        if (value.IsCallable("Uint64") && value.ChildrenSize() == 1 && TryFromString(value.Head().Content(), result)) return result;
        return std::nullopt;
    };
    const auto limit = count(condition.Tail());
    if (!limit || *limit == Max<ui64>()) return std::nullopt;
    const auto points = condition.Head().HeadPtr();
    if (!body.Tail().IsCallable("RangeUnion") || body.Tail().ChildrenSize() != 1 || body.Tail().HeadPtr() != points
        || !points->IsCallable("Collect") || points->ChildrenSize() != 1) return std::nullopt;
    const auto& take = points->Head();
    if (!take.IsCallable("Take") || take.ChildrenSize() != 2 || count(take.Tail()) != *limit + 1) return std::nullopt;
    const auto& flatMap = take.Head();
    if (!flatMap.IsCallable("FlatMap") || flatMap.ChildrenSize() != 2 || !flatMap.Tail().IsLambda()) return std::nullopt;
    const auto& itemLambda = flatMap.Tail();
    if (itemLambda.Head().ChildrenSize() != 1) return std::nullopt;
    const auto itemArg = itemLambda.Head().HeadPtr();
    const auto& multiply = itemLambda.Tail();
    if (!multiply.IsCallable("RangeMultiply") || multiply.ChildrenSize() < 2 || count(multiply.Head()) != limit) return std::nullopt;

    TSqlInRanges result{.List = node->HeadPtr(), .Items = ParseLiteralList(node->HeadPtr()), .ItemRanges = itemLambda.TailPtr(), .Limit = *limit};
    for (size_t i = 1; i < multiply.ChildrenSize(); ++i) {
        const auto& range = *multiply.Child(i);
        if (!range.IsCallable("RangeFor") || range.ChildrenSize() != 3 || !range.Head().IsAtom({"==", "==="})) return std::nullopt;
        TSqlInColumn column;
        if (range.ChildPtr(1) != itemArg) {
            const auto& value = *range.Child(1);
            ui32 index;
            if (!IsArgumentElement(value) || value.HeadPtr() != itemArg || !TryFromString(value.Tail().Content(), index)) return std::nullopt;
            column.Element = index;
        }
        result.Columns.push_back(column);
    }
    const auto full = body.Child(1)->IsCallable("AsRange") ? ParseAsRange(*body.Child(1)) : std::nullopt;
    if (!full || full->Width != result.Columns.size() || full->Ranges.size() != 1
        || !AllOf(full->Ranges[0].Left.Components, [](const TComponent& c) { return c.Inf == -1; })
        || !AllOf(full->Ranges[0].Right.Components, [](const TComponent& c) { return c.Inf == 1; })
        || ParseRangeEmpty(*node->Child(2)).Width != result.Columns.size()) return std::nullopt;

    if (flatMap.HeadPtr() != argument) {
        const auto& order = flatMap.Head();
        if (!order.IsCallable("Map") || order.ChildrenSize() != 2 || !order.Tail().IsLambda()
            || order.Tail().Head().ChildrenSize() != 1 || !order.Tail().Tail().IsList()) return std::nullopt;
        const auto& casts = order.Head();
        if (!casts.IsCallable("FlatMap") || casts.ChildrenSize() != 2 || casts.HeadPtr() != argument
            || !casts.Tail().IsLambda() || casts.Tail().Head().ChildrenSize() != 1) return std::nullopt;
        const auto& cast = casts.Tail().Tail();
        if (!cast.IsCallable("StrictCast") || cast.ChildrenSize() != 2 || cast.HeadPtr() != casts.Tail().Head().HeadPtr()
            || !cast.Tail().IsCallable("OptionalType") || cast.Tail().ChildrenSize() != 1
            || !cast.Tail().Head().IsCallable("TupleType")) return std::nullopt;
        result.TupleTypes = &cast.Tail().Head();
        result.TupleSize = result.TupleTypes->ChildrenSize();
        for (const auto& element : order.Tail().Tail().Children()) {
            ui32 index;
            if (!IsArgumentElement(*element) || element->HeadPtr() != order.Tail().Head().HeadPtr()
                || !TryFromString(element->Tail().Content(), index) || index >= *result.TupleSize) return std::nullopt;
            result.KeyOrder.push_back(index);
        }
    }
    for (const auto& column : result.Columns) {
        if (column.Element && *column.Element >= result.KeyOrder.size()) return std::nullopt;
    }
    return result;
}

bool SameTypeSyntax(const TExprNode& left, const TExprNode& right, size_t& steps) {
    if (++steps > MaxRangeEvaluationNodes) return false;
    if (&left == &right) return true;
    if (left.Type() != right.Type() || left.Content() != right.Content() || left.ChildrenSize() != right.ChildrenSize()) return false;
    for (size_t i = 0; i < left.ChildrenSize(); ++i) {
        if (!SameTypeSyntax(*left.Child(i), *right.Child(i), steps)) return false;
    }
    return true;
}

bool TupleCastPreservesItems(const TSqlInRanges& in) {
    if (!in.TupleTypes) return true;
    size_t steps = 0;
    if (in.Items) {
        for (const auto& item : *in.Items) {
            if (!item->IsList() || item->ChildrenSize() != in.TupleTypes->ChildrenSize()) return false;
            for (size_t i = 0; i < item->ChildrenSize(); ++i) {
                if (++steps > MaxRangeEvaluationNodes) return false;
                const auto value = ParseKeyValue(item->ChildPtr(i));
                const auto& type = *in.TupleTypes->Child(i);
                if (!value || value->Kind != TKeyValue::EKind::Data || !type.IsCallable("DataType") || type.ChildrenSize() != 1) return false;
                const auto slot = NUdf::FindDataSlot(type.Head().Content());
                if (!slot || *slot != value->Slot) return false;
            }
        }
        return true;
    }
    auto list = in.List;
    while (true) {
        if (list->IsCallable("Just") && list->ChildrenSize() == 1) {
            list = list->HeadPtr();
        } else if (list->IsCallable("Map") && list->ChildrenSize() == 2 && list->Tail().IsLambda()
            && list->Tail().Head().ChildrenSize() == 1 && list->Tail().TailPtr() == list->Tail().Head().HeadPtr()) {
            list = list->HeadPtr();
        } else {
            break;
        }
    }
    if (!list->IsCallable("Parameter") || list->ChildrenSize() != 2) return false;
    const auto* type = list->Child(1);
    if (type->IsCallable("OptionalType") && type->ChildrenSize() == 1) type = &type->Head();
    return type->IsCallable("ListType") && type->ChildrenSize() == 1 && SameTypeSyntax(type->Head(), *in.TupleTypes, steps);
}

TString DescribeExprValue(const TExprNode::TPtr& node, TExplainContext& explain) {
    if (const auto param = TMaybeNode<TCoParameter>(node)) {
        return TString(param.Cast().Name().Value());
    }
    if (const auto value = ParseKeyValue(node)) {
        return FormatKeyValue(*value);
    }
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return DescribeExprValue(node->HeadPtr(), explain);
    }
    if (node->IsCallable("Map") && node->ChildrenSize() == 2 && node->Tail().IsLambda()
        && node->Tail().Head().ChildrenSize() == 1
        && node->Tail().TailPtr() == node->Tail().Head().HeadPtr()) {
        // The extractor wraps an optional list in an identity Map.
        return DescribeExprValue(node->HeadPtr(), explain);
    }
    if (node->IsCallable({"SafeCast", "StrictCast", "Convert", "ToString"}) && node->ChildrenSize() > 0) {
        return TStringBuilder() << node->Content() << '(' << DescribeExprValue(node->HeadPtr(), explain) << ')';
    }
    return explain.DescribeUnknownExpression(node);
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

std::optional<TColumnSet> DescribeRangeFor(const TExprNode& node, TExplainContext& explain) {
    using EKind = TColumnSet::EKind;
    const auto op = node.Head().Content();
    if (op == "Exists") {
        return TColumnSet{.Kind = EKind::Interval, .Lower = "NULL"};
    }
    if (op == "NotExists") {
        return TColumnSet{.Kind = EKind::Points, .Points = {"NULL"}};
    }
    const auto value = DescribeExprValue(node.ChildPtr(1), explain);
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
    if (op == "StartsWith" || op == "NotStartsWith") {
        return TColumnSet{.Text = TStringBuilder() << (op == "NotStartsWith" ? "NOT " : "") << "StartsWith(" << value << ')'};
    }
    return std::nullopt;
}

// The values of IN: single values, or tuples in key order.
std::optional<TColumnSet> DescribeSqlIn(const TSqlInRanges& in, TExplainContext& explain) {
    if (!in.Items) {
        // A list known only at runtime, usually a parameter.
        return TColumnSet{.Text = DescribeExprValue(in.List, explain)};
    }
    // NULLs and out-of-type values may produce no ranges.
    TColumnSet result{.Kind = TColumnSet::EKind::Points};
    THashSet<TString> seen;
    size_t bytes = 0;
    for (const auto& item : *in.Items) {
        TStringBuilder point;
        for (size_t i = 0; i < in.Columns.size(); ++i) {
            const auto value = in.GetValue(item, in.Columns[i]);
            const TString text = value ? DescribeExprValue(value, explain) : TString("…");
            if (text.size() + bytes > MaxRangeEvaluationBytes) return std::nullopt;
            bytes += text.size();
            point << (i ? ", " : "") << text;
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

// Describe range algebra without evaluating runtime bounds or applying expansion limits.
class TRangesDescriber {
public:
    TRangesDescriber(TConstArrayRef<TString> keyColumns, TExplainContext& explain)
        : KeyColumns_(keyColumns)
        , Explain_(explain)
    {}

    std::optional<TString> Describe(const TExprNode::TPtr& node) {
        const auto description = DescribeNode(node, 0);
        if (!description || Exhausted_) return Explain_.DescribeUnknownExpression(node);
        TStringBuilder text;
        text << Render(*description, 0, EOp::None);
        for (const auto limit : BroadeningLimits_) text << " [may broaden above " << limit << (limit == 1 ? " range]" : " ranges]");
        return text.size() <= MaxRangeEvaluationBytes ? TString(text) : Explain_.DescribeUnknownExpression(node);
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
        bool KnownWidth = true;
        std::optional<TColumnSet> Column; // the description of a single column
        TString Text;
        EOp Op = EOp::None;
        bool Unconstrained = false; // full range omitted from a product
        std::optional<ui64> MaxRanges;
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

    TDescription Opaque(const TExprNode::TPtr& node, std::optional<size_t> width = {}) {
        return {.Width = width.value_or(0), .KnownWidth = width.has_value(), .Text = Explain_.DescribeUnknownExpression(node)};
    }

    std::optional<TDescription> DescribeNode(const TExprNode::TPtr& node, size_t column, size_t depth = 0) {
        if (++Steps_ > MaxRangeEvaluationNodes || depth > 64 || Exhausted_) {
            Exhausted_ = true;
            return std::nullopt;
        }
        auto result = TryDescribeNode(node, column, depth);
        if (Exhausted_) return std::nullopt;
        if (!result) result = Opaque(node, node->IsCallable("RangeFor") ? std::optional<size_t>(1) : std::nullopt);
        const auto text = Render(*result, column, EOp::None);
        if (text.size() > RemainingBytes_) {
            Exhausted_ = true;
            return std::nullopt;
        }
        RemainingBytes_ -= text.size();
        return result;
    }

    std::optional<TDescription> TryDescribeNode(const TExprNode::TPtr& node, size_t column, size_t depth) {
        if (const auto in = MatchSqlInRanges(node)) {
            const size_t width = in->Columns.size();
            if (width > 64 || column + width > KeyColumns_.size()) return std::nullopt;
            if (!TupleCastPreservesItems(*in)) return Opaque(node, width);
            if (!in->Items || in->Items->size() > in->Limit) BroadeningLimits_.insert(in->Limit);
            const ui64 maxRanges = Max<ui64>(1, Min<ui64>(in->Limit, in->Items ? in->Items->size() : Max<ui64>()));
            auto set = DescribeSqlIn(*in, Explain_);
            if (!set) return Opaque(node, width);
            if (!in->Items && in->Columns.front().Element) {
                if (!in->TupleSize) return Opaque(node, width);
                TVector<size_t> sources;
                TVector<TString> names(*in->TupleSize);
                bool permutation = *in->TupleSize == width;
                for (size_t i = 0; i < width; ++i) {
                    const auto element = in->Columns[i].Element;
                    if (!element || *element >= in->KeyOrder.size()) return Opaque(node, width);
                    const auto source = in->KeyOrder[*element];
                    if (source >= *in->TupleSize) return Opaque(node, width);
                    sources.push_back(source);
                    permutation &= names[source].empty();
                    names[source] = ColumnName(column + i);
                }
                TStringBuilder text;
                if (width > 1) text << '(';
                for (size_t i = 0; i < width; ++i) {
                    text << (i ? ", " : "") << (permutation ? names[i] : ColumnName(column + i));
                }
                if (width > 1) text << ')';
                text << " ∈ ";
                if (permutation) {
                    text << set->Format();
                } else {
                    text << "project(" << set->Format();
                    for (const auto source : sources) text << ", " << source;
                    text << ')';
                }
                return TDescription{.Width = width, .Text = text, .MaxRanges = maxRanges};
            }
            if (width == 1) return TDescription{.Width = 1, .Column = std::move(*set), .MaxRanges = maxRanges};
            TStringBuilder text;
            text << '(';
            for (size_t i = 0; i < width; ++i) text << (i ? ", " : "") << ColumnName(column + i);
            text << ") ∈ " << set->Format();
            return TDescription{.Width = width, .Text = text, .MaxRanges = maxRanges};
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
                .KnownWidth = ranges->KnownWidth,
                .Text = TStringBuilder() << Render(*ranges, column, EOp::Condition) << " if " << DescribeExprValue(node->HeadPtr(), Explain_),
                .Op = EOp::Condition,
                .MaxRanges = ranges->MaxRanges,
            };
        }
        if (node->IsCallable("RangeFor") && node->ChildrenSize() == 3) {
            if (column >= KeyColumns_.size()) {
                return std::nullopt;
            }
            auto set = DescribeRangeFor(*node, Explain_);
            if (!set) {
                return std::nullopt;
            }
            return TDescription{.Width = 1, .Column = std::move(*set), .MaxRanges = (node->Head().IsAtom("!=") || node->Head().IsAtom("NotStartsWith")) ? 2u : 1u};
        }
        if (node->IsCallable("RangeEmpty")) {
            return TDescription{.Width = ParseRangeEmpty(*node).Width, .Text = "∅", .MaxRanges = 0};
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
                return TDescription{.Width = 1, .Column = TColumnSet{.Kind = TColumnSet::EKind::Interval}, .Unconstrained = true, .MaxRanges = 1};
            }
            return TDescription{
                .Width = ranges->Width,
                .Text = ranges->Ranges.empty() ? TString("∅") : TString(text),
                .Op = ranges->Ranges.size() > 1 ? EOp::Union : EOp::None,
                .Unconstrained = unconstrained,
                .MaxRanges = ranges->Ranges.size(),
            };
        }
        if (node->IsCallable({"RangeUnion", "RangeIntersect"}) && node->ChildrenSize() > 0) {
            const auto op = node->IsCallable("RangeUnion") ? EOp::Union : EOp::Intersect;
            TVector<TDescription> children;
            for (const auto& child : node->Children()) {
                auto description = DescribeNode(child, column, depth + 1);
                if (!description) {
                    return std::nullopt;
                }
                children.push_back(std::move(*description));
            }
            std::optional<ui64> maxRanges = 0;
            std::optional<size_t> width;
            for (const auto& child : children) {
                if (maxRanges && child.MaxRanges && *child.MaxRanges <= Max<ui64>() - *maxRanges) {
                    *maxRanges += *child.MaxRanges;
                } else {
                    maxRanges.reset();
                }
                if (child.KnownWidth) {
                    if (width && *width != child.Width) return std::nullopt;
                    width = child.Width;
                }
            }
            if (op == EOp::Intersect) {
                if (AnyOf(children, [](const TDescription& child) { return child.MaxRanges == 0; })) {
                    maxRanges = 0;
                } else if (maxRanges) {
                    *maxRanges -= children.size() - 1;
                }
            }
            for (auto& child : children) {
                child.Width = width.value_or(0);
                child.KnownWidth = width.has_value();
            }
            if (children.size() == 1) return std::move(children.front());
            if (AllOf(children, [](const TDescription& child) { return child.Column.has_value(); })) {
                TVector<TColumnSet> sets;
                for (auto& child : children) {
                    sets.push_back(std::move(*child.Column));
                }
                return TDescription{.Width = children.front().Width, .Column = op == EOp::Union ? UniteSets(sets) : IntersectSets(sets), .MaxRanges = maxRanges};
            }
            auto result = Join(children, column, op, op == EOp::Union ? " ∪ " : " ∩ ");
            result.MaxRanges = maxRanges;
            return result;
        }
        if (node->IsCallable("RangeMultiply") && node->ChildrenSize() > 1) {
            ui64 limit;
            if (!node->Head().IsCallable("Uint64") || node->Head().ChildrenSize() != 1
                || !TryFromString(node->Head().Head().Content(), limit)) return std::nullopt;
            std::optional<ui64> maxRanges = 1;
            bool mayBroaden = false;
            TVector<TDescription> children;
            TVector<size_t> columns;
            size_t next = column;
            for (size_t i = 1; i < node->ChildrenSize(); ++i) {
                auto description = DescribeNode(node->ChildPtr(i), next, depth + 1);
                if (!description) {
                    return std::nullopt;
                }
                if (!description->KnownWidth) return std::nullopt;
                if (maxRanges && description->MaxRanges && (!*description->MaxRanges || *maxRanges <= Max<ui64>() / *description->MaxRanges)) {
                    *maxRanges *= *description->MaxRanges;
                } else {
                    maxRanges.reset();
                }
                if ((i > 1 || node->ChildrenSize() == 2) && (!maxRanges || *maxRanges > limit)) {
                    mayBroaden = true;
                }
                const size_t width = description->Width;
                // Columns past a constrained prefix are padded with the full range, which says nothing.
                if (!description->Unconstrained || children.empty()) {
                    columns.push_back(next);
                    children.push_back(std::move(*description));
                }
                next += width;
            }
            if (mayBroaden) {
                BroadeningLimits_.insert(limit);
                // Multiplication can keep a prefix exceeding the limit.
                maxRanges.reset();
            }
            if (children.size() == 1) {
                children.front().Width = next - column;
                children.front().MaxRanges = maxRanges;
                return std::move(children.front());
            }
            TDescription result{.Width = next - column, .Op = EOp::Product, .MaxRanges = maxRanges};
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
        return TDescription{.Width = children.front().Width, .KnownWidth = children.front().KnownWidth, .Text = text, .Op = op};
    }

    const TConstArrayRef<TString> KeyColumns_;
    TExplainContext& Explain_;
    size_t Steps_ = 0;
    size_t RemainingBytes_ = MaxRangeEvaluationBytes;
    bool Exhausted_ = false;
    TSet<ui64> BroadeningLimits_;
};

std::optional<TString> DescribeRangesStructure(const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, TExplainContext& explain) {
    if (!ranges) return std::nullopt;
    if (keyColumns.empty() || !FitsInputBudget(*ranges, false)) return explain.DescribeUnknownExpression(ranges);
    return TRangesDescriber(keyColumns, explain).Describe(ranges);
}

// Interpret finite range algebra; leave unsupported types and general expressions to the formatter.
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
        const ui64 limit = in->Limit;
        RequireRange(in->Items->size() <= limit);
        const auto& multiply = *in->ItemRanges;
        const auto* tupleTypes = in->TupleTypes;
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

// Only literal constructors and finite operations emitted by the extractor.
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

bool FitsInputBudget(const TExprNode& root, bool interpreted) {
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
            || (interpreted && (node->Type() == TExprNode::World || (node->IsCallable() && !IsSupportedCallable(*node))))) {
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
    const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, TMaybe<size_t> expectedMaxRanges, TExplainContext* explain) {
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
    TExplainContext local;
    return {.Expression = DescribeRangesStructure(ranges, keyColumns, explain ? *explain : local)};
}

} // namespace NKikimr::NKqp::NOpt
