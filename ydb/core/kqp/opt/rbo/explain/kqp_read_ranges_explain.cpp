#include "kqp_read_ranges_explain.h"

#include "../kqp_info_unit.h"
#include "../eval/kqp_eval.h"

#include <yql/essentials/ast/yql_expr.h>
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
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

#include <cmath>
#include <limits>
#include <optional>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using NUdf::EDataSlot;
using NDecimal::TInt128;

namespace {

// The structure lists at most this many values of a set and counts the rest.
constexpr size_t MaxDescribedPoints = 10;
constexpr size_t MaxRangeDepth = 64;
constexpr size_t MaxRangeKeyColumns = 64;

bool FitsInputBudget(const TExprNode& root);
bool IsIntegerSlot(EDataSlot slot);
std::optional<std::pair<TInt128, TInt128>> IntegerLimits(EDataSlot slot);

struct TUnsupportedRange {};

void RequireRange(bool condition) {
    if (!condition) {
        throw TUnsupportedRange{};
    }
}

bool IsUnaryLambda(const TExprNode& node) {
    return node.IsLambda() && node.ChildrenSize() == 2 && node.Head().Type() == TExprNode::Arguments
        && node.Head().ChildrenSize() == 1 && node.Head().Head().Type() == TExprNode::Argument;
}

std::optional<TStringBuf> ParameterName(const TExprNode& node) {
    if (node.IsCallable("Parameter") && node.ChildrenSize() == 2 && node.Head().IsAtom()) return node.Head().Content();
    return std::nullopt;
}


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
    if (!node.IsCallable()) return std::nullopt;
    const auto slot = NUdf::FindDataSlot(node.Content());
    if (!slot || node.ChildrenSize() != (*slot == EDataSlot::Decimal ? 3u : 1u) || !node.Head().IsAtom()) {
        return std::nullopt;
    }
    // A refused binary numeric literal must not be reinterpreted as text by
    // explain's structural fallback. String-like payloads already contain bytes.
    if ((node.Head().Flags() & TNodeFlags::BinaryContent) &&
        *slot != EDataSlot::String && *slot != EDataSlot::Utf8 && *slot != EDataSlot::Uuid) {
        return std::nullopt;
    }

    const TStringBuf text = node.Head().Content();
    TKeyValue result{.Kind = TKeyValue::EKind::Data, .Slot = *slot};
    std::optional<NUdf::TUnboxedValuePod> value;
    switch (*slot) {
        case EDataSlot::String:
        case EDataSlot::Utf8:
            result.Bytes = text;
            return result;
        case EDataSlot::Uuid:
            if (text.size() != 16) return std::nullopt;
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
            if (!node.Child(1)->IsAtom() || !node.Child(2)->IsAtom()
                || !TryFromString(node.Child(1)->Content(), result.Precision)
                || !TryFromString(node.Child(2)->Content(), result.Scale)
                || !result.Precision || result.Precision > NDecimal::MaxPrecision || result.Scale > result.Precision) {
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
    if ((*slot == EDataSlot::Interval && !NUdf::IsValidLayoutValue<NUdf::TInterval>(value->Get<i64>())) ||
        (*slot == EDataSlot::Interval64 && !NUdf::IsValidLayoutValue<NUdf::TInterval64>(value->Get<i64>()))) {
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
    size_t begin = 0;
    for (size_t i = 0; i < text.size(); ++i) {
        const char c = text[i];
        if (c != '"' && c != '\\' && static_cast<unsigned char>(c) >= 0x20 && c != 0x7f) {
            continue;
        }

        result << text.SubStr(begin, i - begin);
        if (c == '"' || c == '\\') {
            result << '\\' << c;
        } else {
            result << EscapeC(TStringBuf(&c, 1));
        }

        RequireRange(result.size() <= MaxRangeEvaluationBytes);
        begin = i + 1;
    }
    result << text.SubStr(begin);
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
    if (!node.IsList() || node.ChildrenSize() < 2 || !node.Tail().IsCallable("Int32")
        || node.Tail().ChildrenSize() != 1 || !node.Tail().Head().IsAtom()
        || (node.Tail().Head().Flags() & TNodeFlags::BinaryContent)) {
        return std::nullopt;
    }
    i32 included;
    if (!TryFromString(node.Tail().Head().Content(), included) || (included != 0 && included != 1)) {
        return std::nullopt;
    }
    TVector<TBoundaryElement> elements;
    for (size_t i = 0; i + 1 < node.ChildrenSize(); ++i) {
        // An empty outer Optional is an infinity; Just of an empty Optional is
        // a finite NULL. Casts are left to the evaluator, which checks both types.
        const auto& element = node.ChildPtr(i);
        if (element->IsCallable("Nothing") && element->ChildrenSize() == 1) {
            elements.push_back({.Infinite = !element->Head().IsCallable("PgType")});
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
    if (!node) return std::nullopt;
    if (node->IsCallable("Nothing") && node->ChildrenSize() == 1) {
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
    if (node->IsCallable("PgConst") && node->ChildrenSize() == 2 && node->Head().IsAtom()
        && node->Tail().IsCallable("PgType") && node->Tail().ChildrenSize() == 1 && node->Tail().Head().IsAtom()) {
        return TKeyValue{.Kind = TKeyValue::EKind::Pg, .PgType = TString(node->Tail().Head().Content()),
                         .Bytes = TString(node->Head().Content())};
    }
    return ParseData(*node);
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
        case EDataSlot::Decimal: {
            const auto* text = NDecimal::ToString(value.Value.GetInt128(), value.Precision, value.Scale);
            RequireRange(text != nullptr);
            return text;
        }
        case EDataSlot::Date:
            return TInstant::Days(value.Value.Get<ui16>()).FormatGmTime("%Y-%m-%d");
        case EDataSlot::Datetime:
            return TInstant::Seconds(value.Value.Get<ui32>()).ToStringUpToSeconds();
        case EDataSlot::Timestamp:
            return TInstant::MicroSeconds(value.Value.Get<ui64>()).ToString();
        default: {
            const auto integer = GetInteger(value);
            RequireRange(integer.has_value());
            return FormatInteger(*integer);
        }
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
            if (!bound->IsList() || bound->ChildrenSize() < 2 || bound->ChildrenSize() > MaxRangeKeyColumns + 1) {
                return std::nullopt;
            }
            for (size_t i = 0; i + 1 < bound->ChildrenSize(); ++i) {
                const auto* value = bound->Child(i);
                size_t depth = 0;
                while (value->IsCallable({"Just", "ToString"}) && value->ChildrenSize() == 1) {
                    if (++depth > MaxRangeDepth) {
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

std::optional<TRangeList> ParseRangeEmpty(const TExprNode& node) {
    if (node.ChildrenSize() != 1) return std::nullopt;
    const size_t width = node.Head().IsCallable("TupleType") ? node.Head().ChildrenSize() : 1;
    if (!width || width > MaxRangeKeyColumns) return std::nullopt;
    return TRangeList{.Width = width};
}

TString FormatRange(const TRange& range, TConstArrayRef<TString> keyColumns) {
    size_t remaining = MaxRangeEvaluationBytes;
    for (const auto* bound : {&range.Left, &range.Right}) {
        for (const auto& c : bound->Components) {
            RequireRange(c.Value.Bytes.size() <= remaining);
            remaining -= c.Value.Bytes.size();
        }
    }
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

bool CanDescribeSqlInAsMembership(const TSqlInRanges& in) {
    if (!in.Items || in.Items->size() > in.Limit) return false;
    size_t steps = 0;
    size_t bytes = 0;
    for (const auto& item : *in.Items) {
        for (size_t i = 0; i < in.Columns.size(); ++i) {
            if (++steps > MaxRangeEvaluationNodes) return false;
            const auto value = ParseKeyValue(in.GetValue(item, in.Columns[i]));
            if (!value || value->Kind != TKeyValue::EKind::Data) return false;
            if (value->Bytes.size() > MaxRangeEvaluationBytes - bytes) return false;
            bytes += value->Bytes.size();
            const auto* type = &in.ItemRanges->Child(i + 1)->Tail();
            if (type->IsCallable("OptionalType") && type->ChildrenSize() == 1) type = &type->Head();
            if (!type->IsCallable("DataType") || type->ChildrenSize() != 1) return false;
            const auto slot = NUdf::FindDataSlot(type->Head().Content());
            if (!slot) return false;
            if (IsIntegerSlot(*slot) && IsIntegerSlot(value->Slot)) {
                const auto integer = GetInteger(*value);
                const auto limits = IntegerLimits(*slot);
                if (!integer || !limits || *integer < limits->first || *integer > limits->second) return false;
            } else if (*slot != value->Slot || *slot == EDataSlot::Float || *slot == EDataSlot::Double) {
                return false;
            }
        }
    }
    return true;
}

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
    if (node->IsCallable("Map") && node->ChildrenSize() == 2 && IsUnaryLambda(node->Tail())) {
        const auto tuple = ParseLiteralList(node->HeadPtr());
        const auto& lambda = node->Tail();
        const auto& body = lambda.Tail();
        if (!tuple || !body.IsCallable("AsList")) {
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
    if (!node->IsCallable("IfPresent") || node->ChildrenSize() != 3 || !IsUnaryLambda(*node->Child(1))
        || !node->Child(2)->IsCallable("RangeEmpty")) return std::nullopt;
    const auto& lambda = *node->Child(1);
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
    if (!flatMap.IsCallable("FlatMap") || flatMap.ChildrenSize() != 2 || !IsUnaryLambda(flatMap.Tail())) return std::nullopt;
    const auto& itemLambda = flatMap.Tail();
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
    const auto empty = ParseRangeEmpty(*node->Child(2));
    if (!full || full->Width != result.Columns.size() || full->Ranges.size() != 1
        || !AllOf(full->Ranges[0].Left.Components, [](const TComponent& c) { return c.Inf == -1; })
        || !AllOf(full->Ranges[0].Right.Components, [](const TComponent& c) { return c.Inf == 1; })
        || !empty || empty->Width != result.Columns.size()) return std::nullopt;

    if (flatMap.HeadPtr() != argument) {
        const auto& order = flatMap.Head();
        if (!order.IsCallable("Map") || order.ChildrenSize() != 2 || !IsUnaryLambda(order.Tail())
            || !order.Tail().Tail().IsList()) return std::nullopt;
        const auto& casts = order.Head();
        if (!casts.IsCallable("FlatMap") || casts.ChildrenSize() != 2 || casts.HeadPtr() != argument
            || !IsUnaryLambda(casts.Tail())) return std::nullopt;
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
        } else if (list->IsCallable("Map") && list->ChildrenSize() == 2 && IsUnaryLambda(list->Tail())
            && list->Tail().TailPtr() == list->Tail().Head().HeadPtr()) {
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
    if (const auto param = ParameterName(*node)) return TString(*param);
    if (const auto value = ParseKeyValue(node)) {
        return FormatKeyValue(*value);
    }
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return DescribeExprValue(node->HeadPtr(), explain);
    }
    if (node->IsCallable("Map") && node->ChildrenSize() == 2 && IsUnaryLambda(node->Tail())
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
    bool Sequence = false; // Preserve duplicates when they count toward an IN expansion limit.

    TString Format() const {
        TStringBuilder result;
        if (Kind == EKind::Points) {
            result << (Sequence ? '[' : '{');
            for (size_t i = 0; i < Min(Points.size(), MaxDescribedPoints); ++i) {
                result << (i ? ", " : "") << Points[i];
            }
            if (Points.size() > MaxDescribedPoints) {
                result << ", … " << Points.size() - MaxDescribedPoints << " more";
            }
            result << (Sequence ? ']' : '}');
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
std::optional<TColumnSet> DescribeSqlIn(const TSqlInRanges& in, TExplainContext& explain, bool sequence = false) {
    if (!in.Items) {
        // A list known only at runtime, usually a parameter.
        return TColumnSet{.Text = DescribeExprValue(in.List, explain)};
    }
    // NULLs and out-of-type values may produce no ranges.
    TColumnSet result{.Kind = TColumnSet::EKind::Points, .Sequence = sequence};
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
        if (sequence || seen.insert(text).second) {
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
        const auto text = Render(*description, 0, EOp::None);
        return text.size() <= MaxRangeEvaluationBytes ? text : Explain_.DescribeUnknownExpression(node);
    }

private:
    enum class EOp {
        None,
        Union,
        Intersect,
        Product,
    };

    struct TDescription {
        size_t Width = 0;
        bool KnownWidth = true;
        std::optional<TColumnSet> Column; // the description of a single column
        TString Text;
        EOp Op = EOp::None;
        bool Unconstrained = false; // full range omitted from a product
        std::optional<ui64> MaxRanges;
        bool PointRanges = false; // Every intermediate range is a singleton, before finalization.
    };

    TString ColumnName(size_t column) const {
        RequireRange(column < KeyColumns_.size());
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

    std::optional<TDescription> DescribeNode(const TExprNode::TPtr& node, size_t column, size_t depth = 0, bool preserveSuffixes = false) {
        if (++Steps_ > MaxRangeEvaluationNodes || depth > MaxRangeDepth || Exhausted_) {
            Exhausted_ = true;
            return std::nullopt;
        }
        auto result = TryDescribeNode(node, column, depth, preserveSuffixes);
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

    std::optional<TDescription> TryDescribeNode(const TExprNode::TPtr& node, size_t column, size_t depth, bool preserveSuffixes) {
        if (const auto in = MatchSqlInRanges(node)) {
            return DescribeInRanges(node, *in, column);
        }
        if (node->IsCallable({"RangeFinalize", "RangeToPg"}) && node->ChildrenSize() == 1) {
            return DescribeNode(node->HeadPtr(), column, depth + 1, preserveSuffixes);
        }
        if (node->IsCallable({"If", "IfStrict"}) && node->ChildrenSize() == 3) {
            return DescribeConditional(node, column, depth, preserveSuffixes);
        }
        if (node->IsCallable("RangeFor") && node->ChildrenSize() == 3) {
            if (column >= KeyColumns_.size()) {
                return std::nullopt;
            }
            auto set = DescribeRangeFor(*node, Explain_);
            if (!set) {
                return std::nullopt;
            }
            return TDescription{.Width = 1, .Column = std::move(*set),
                .MaxRanges = node->Head().IsAtom({"!=", "NotStartsWith"}) ? 2u : 1u,
                .PointRanges = node->Head().IsAtom({"==", "NotExists"})};
        }
        if (node->IsCallable("RangeEmpty")) {
            if (!node->ChildrenSize()) {
                return TDescription{.KnownWidth = false, .Text = "∅", .MaxRanges = 0, .PointRanges = true};
            }
            const auto empty = ParseRangeEmpty(*node);
            if (!empty) return std::nullopt;
            return TDescription{.Width = empty->Width, .Text = "∅", .MaxRanges = 0, .PointRanges = true};
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
            return DescribeUnionOrIntersection(node, column, depth, preserveSuffixes);
        }
        if (node->IsCallable("RangeMultiply") && node->ChildrenSize() > 1) {
            return DescribeProduct(node, column, depth, preserveSuffixes);
        }
        return std::nullopt;
    }

    std::optional<TDescription> DescribeInRanges(const TExprNode::TPtr& node, const TSqlInRanges& in, size_t column) {
        const size_t width = in.Columns.size();
        if (width > MaxRangeKeyColumns || column + width > KeyColumns_.size()) return std::nullopt;
        if (!TupleCastPreservesItems(in)) return Opaque(node, width);
        const bool limited = !in.Items || in.Items->size() > in.Limit;
        const bool membership = CanDescribeSqlInAsMembership(in);
        const ui64 maxRanges = limited ? Max<ui64>(1, in.Limit) : in.Items->size();
        const bool pointRanges = !limited && AllOf(in.ItemRanges->Children().subspan(1), [](const auto& range) {
            return range->Head().IsAtom("==");
        });
        auto set = DescribeSqlIn(in, Explain_, !membership);
        if (!set) return Opaque(node, width);
        TStringBuilder keys;
        TString values = set->Format();
        if (!in.Items && in.Columns.front().Element) {
            if (!in.TupleSize) return Opaque(node, width);
            TVector<size_t> sources;
            TVector<TString> names(*in.TupleSize);
            bool permutation = *in.TupleSize == width;
            for (size_t i = 0; i < width; ++i) {
                const auto element = in.Columns[i].Element;
                if (!element || *element >= in.KeyOrder.size()) return Opaque(node, width);
                const auto source = in.KeyOrder[*element];
                if (source >= *in.TupleSize) return Opaque(node, width);
                sources.push_back(source);
                permutation &= names[source].empty();
                names[source] = ColumnName(column + i);
            }
            if (width > 1) keys << '(';
            for (size_t i = 0; i < width; ++i) keys << (i ? ", " : "") << (permutation ? names[i] : ColumnName(column + i));
            if (width > 1) keys << ')';
            if (!permutation) {
                TStringBuilder projected;
                projected << "project(" << values;
                for (const auto source : sources) projected << ", " << source;
                values = projected << ')';
            }
        } else {
            if (membership && width == 1) {
                return TDescription{.Width = 1, .Column = std::move(*set), .MaxRanges = maxRanges, .PointRanges = pointRanges};
            }
            if (width > 1) keys << '(';
            for (size_t i = 0; i < width; ++i) keys << (i ? ", " : "") << ColumnName(column + i);
            if (width > 1) keys << ')';
        }
        const TString text = !membership
            ? TString(TStringBuilder() << "in_ranges(" << keys << ", " << values << ", limit=" << in.Limit << ')')
            : TString(TStringBuilder() << keys << " ∈ " << values);
        return TDescription{.Width = width, .Text = text, .MaxRanges = maxRanges, .PointRanges = pointRanges};
    }

    std::optional<TDescription> DescribeConditional(const TExprNode::TPtr& node, size_t column, size_t depth, bool preserveSuffixes) {
        const auto condition = ParseKeyValue(node->HeadPtr());
        if (condition && condition->Kind == TKeyValue::EKind::Data && condition->Slot == NUdf::EDataSlot::Bool) {
            return DescribeNode(condition->Value.Get<bool>() ? node->ChildPtr(1) : node->ChildPtr(2), column, depth + 1, preserveSuffixes);
        }
        auto yes = DescribeNode(node->ChildPtr(1), column, depth + 1, preserveSuffixes);
        auto no = DescribeNode(node->ChildPtr(2), column, depth + 1, preserveSuffixes);
        if (!yes || !no || (yes->KnownWidth && no->KnownWidth && yes->Width != no->Width)) return std::nullopt;
        return TDescription{
            .Width = yes->KnownWidth ? yes->Width : no->Width,
            .KnownWidth = yes->KnownWidth || no->KnownWidth,
            .Text = TStringBuilder() << "if(" << DescribeExprValue(node->HeadPtr(), Explain_) << ", "
                << Render(*yes, column, EOp::None) << ", " << Render(*no, column, EOp::None) << ')',
            .MaxRanges = yes->MaxRanges && no->MaxRanges ? std::optional<ui64>(Max(*yes->MaxRanges, *no->MaxRanges)) : std::nullopt,
            .PointRanges = yes->PointRanges && no->PointRanges,
        };
    }

    std::optional<TDescription> DescribeUnionOrIntersection(const TExprNode::TPtr& node, size_t column, size_t depth, bool preserveSuffixes) {
        const auto op = node->IsCallable("RangeUnion") ? EOp::Union : EOp::Intersect;
        TVector<TDescription> children;
        for (const auto& child : node->Children()) {
            auto description = DescribeNode(child, column, depth + 1, preserveSuffixes);
            if (!description) {
                return std::nullopt;
            }
            children.push_back(std::move(*description));
        }
        std::optional<ui64> maxRanges = 0;
        const bool pointRanges = AllOf(children, [](const auto& child) { return child.PointRanges; });
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
            return TDescription{.Width = children.front().Width, .Column = op == EOp::Union ? UniteSets(sets) : IntersectSets(sets), .MaxRanges = maxRanges, .PointRanges = pointRanges};
        }
        auto result = Join(children, column, op, op == EOp::Union ? " ∪ " : " ∩ ");
        result.MaxRanges = maxRanges;
        result.PointRanges = pointRanges;
        return result;
    }

    std::optional<TDescription> DescribeProduct(const TExprNode::TPtr& node, size_t column, size_t depth, bool preserveSuffixes) {
        ui64 limit;
        if (!node->Head().IsCallable("Uint64") || node->Head().ChildrenSize() != 1
            || !TryFromString(node->Head().Head().Content(), limit)) return std::nullopt;
        std::optional<ui64> maxRanges = 1;
        bool limited = false;
        bool pointRanges = true;
        bool needsProduct = false;
        TVector<TDescription> children;
        TVector<size_t> columns;
        size_t next = column;
        for (size_t i = 1; i < node->ChildrenSize(); ++i) {
            auto description = DescribeNode(node->ChildPtr(i), next, depth + 1, preserveSuffixes || node->ChildrenSize() > 2);
            if (!description || !description->KnownWidth) return std::nullopt;
            if (maxRanges && description->MaxRanges && (!*description->MaxRanges || *maxRanges <= Max<ui64>() / *description->MaxRanges)) {
                *maxRanges *= *description->MaxRanges;
            } else {
                maxRanges.reset();
            }
            if ((i > 1 || node->ChildrenSize() == 2) && (!maxRanges || *maxRanges > limit)) limited = true;
            // An interval prefix cannot enforce a constrained suffix, even below the limit.
            if (i > 1 && !pointRanges && !description->Unconstrained) needsProduct = true;
            pointRanges &= description->PointRanges;
            columns.push_back(next);
            next += description->Width;
            children.push_back(std::move(*description));
        }
        if (limited || needsProduct) {
            TStringBuilder text;
            text << "range_product(";
            for (size_t i = 0; i < children.size(); ++i) {
                text << (i ? ", " : "") << Render(children[i], columns[i], EOp::None);
            }
            text << ", limit=" << limit << ')';
            // A failed multiplication retains the previous prefix, which can exceed the limit.
            if (limited) {
                maxRanges = node->ChildrenSize() == 2 ? std::optional<ui64>(Max<ui64>(1, limit))
                    : children.front().MaxRanges ? std::optional<ui64>(Max(*children.front().MaxRanges, limit)) : std::nullopt;
            }
            return TDescription{.Width = next - column, .Text = text, .MaxRanges = maxRanges};
        }
        TStringBuilder text;
        size_t kept = 0;
        for (size_t i = 0; i < children.size(); ++i) {
            if (children[i].Unconstrained && kept && !preserveSuffixes) continue;
            text << (kept++ ? " × " : "") << Render(children[i], columns[i], EOp::Product);
        }
        if (kept == 1) {
            children.front().Width = next - column;
            children.front().MaxRanges = maxRanges;
            children.front().PointRanges = pointRanges;
            return std::move(children.front());
        }
        return TDescription{.Width = next - column, .Text = text, .Op = EOp::Product,
            .MaxRanges = maxRanges, .PointRanges = pointRanges};
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
};

std::optional<TString> DescribeRangesStructure(const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, TExplainContext& explain) {
    if (!ranges) return std::nullopt;
    if (keyColumns.empty() || !FitsInputBudget(*ranges)) return explain.DescribeUnknownExpression(ranges);
    return TRangesDescriber(keyColumns, explain).Describe(ranges);
}

// Numeric helpers for formatting literal bounds and symbolic comparisons.
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

bool FitsInputBudget(const TExprNode& root) {
    TVector<std::pair<const TExprNode*, size_t>> pending{{&root, 0}};
    THashMap<const TExprNode*, size_t> depths;
    size_t bytes = 0;
    size_t edges = 0;
    while (!pending.empty()) {
        const auto [node, depth] = pending.back();
        pending.pop_back();
        if (!node || depth > MaxRangeDepth) {
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
        if (depths.size() > MaxRangeEvaluationNodes || edges > MaxRangeEvaluationNodes || bytes > MaxRangeEvaluationBytes) {
            return false;
        }
        for (const auto& child : node->Children()) {
            pending.emplace_back(child.Get(), depth + 1);
        }
    }
    return true;
}

std::optional<TBoundary> ParseLiteralBound(const TExprNode& bound, bool isLeft, size_t width, size_t& bytes) {
    if (!bound.IsCallable({"KqlKeyInc", "KqlKeyExc"}) || bound.ChildrenSize() > width) return std::nullopt;
    TBoundary result;
    for (const auto& node : bound.Children()) {
        std::optional<TKeyValue> value;
        if (const auto param = ParameterName(*node)) {
            value = TKeyValue{.Kind = TKeyValue::EKind::Parameter, .Bytes = TString(*param)};
        } else {
            value = ParseKeyValue(node);
        }
        if (!value || value->Bytes.size() > MaxRangeEvaluationBytes - bytes) return std::nullopt;
        bytes += value->Bytes.size();
        result.Components.push_back({.Value = std::move(*value)});
    }
    result.Included = bound.IsCallable("KqlKeyInc") ? (isLeft ? -1 : 1) : 0;
    return result;
}

std::optional<TString> DescribeLiteralKeyRange(const TExprNode::TPtr& keyRange, const TVector<TString>& keyColumns) {
    if (!keyRange->IsList() || keyRange->ChildrenSize() != 2 || !FitsInputBudget(*keyRange)) return std::nullopt;
    size_t bytes = 0;
    auto left = ParseLiteralBound(keyRange->Head(), true, keyColumns.size(), bytes);
    auto right = ParseLiteralBound(keyRange->Tail(), false, keyColumns.size(), bytes);
    if (!left || !right) return std::nullopt;
    auto text = FormatRange({std::move(*left), std::move(*right)}, keyColumns);
    return text.size() <= MaxRangeEvaluationBytes ? std::optional<TString>(std::move(text)) : std::nullopt;
}

TRange DisplayRange(const NEval::TRange& source) {
    const auto boundary = [](const NEval::TBoundary& bound) {
        TBoundary result{.Included = bound.Included};
        result.Components.reserve(bound.Components.size());
        for (const auto& c : bound.Components) {
            result.Components.push_back({.Inf = c.Inf, .Value = {
                .Kind = c.Value.Null ? TKeyValue::EKind::Null : TKeyValue::EKind::Data,
                .Slot = c.Value.Slot, .Bytes = TString(c.Value.Text()), .Value = c.Value.Data}});
        }
        return result;
    };
    return {boundary(source.Left), boundary(source.Right)};
}

bool FitsKeyColumnsBudget(const TVector<TString>& columns) {
    if (columns.empty() || columns.size() > MaxRangeKeyColumns) return false;
    size_t bytes = 0;
    for (const auto& column : columns) {
        if (column.size() > MaxRangeEvaluationBytes - bytes) return false;
        bytes += column.size();
    }
    return true;
}

} // namespace

TReadRangesDescription DescribeReadRanges(const TExprNode::TPtr& literalRange,
    const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, TMaybe<size_t> expectedMaxRanges, TExplainContext* explain) {
    TExplainContext local;
    auto& context = explain ? *explain : local;
    const auto& source = ranges ? ranges : literalRange;
    if (!source) return {};
    try {
        if (FitsKeyColumnsBudget(keyColumns)) {
            if (literalRange) {
                if (auto literal = DescribeLiteralKeyRange(literalRange, keyColumns)) {
                    return {.Ranges = TVector<TString>{std::move(*literal)}};
                }
            }
            if (ranges && expectedMaxRanges && *expectedMaxRanges <= MaxEvaluatedRangeInputs) {
                const auto evaluated = context.Evaluation().Evaluate(ranges);
                if (const auto* error = evaluated.GetError(); error && error->Code == NEval::EError::BudgetExceeded) {
                    return {.Expression = context.DescribeOpaqueExpression(source)};
                }
                if (const auto* value = evaluated.GetValue()) {
                    const auto* list = (*value)->GetRanges();
                    if (list && list->Width <= keyColumns.size() && list->Items.size() <= MaxExplainedReadRanges) {
                        TVector<TString> result;
                        size_t bytes = 0;
                        for (const auto& range : list->Items) {
                            auto text = FormatRange(DisplayRange(range), keyColumns);
                            RequireRange(text.size() <= MaxRangeEvaluationBytes - bytes);
                            bytes += text.size();
                            result.push_back(std::move(text));
                        }
                        return {.Ranges = std::move(result)};
                    }
                }
            }
            return {.Expression = DescribeRangesStructure(source, keyColumns, context)};
        }
    } catch (const TUnsupportedRange&) {
    } catch (const yexception&) {
        // A formatting failure must not invalidate the query plan.
    }
    return {.Expression = context.DescribeUnknownExpression(source)};
}

} // namespace NKikimr::NKqp::NOpt
