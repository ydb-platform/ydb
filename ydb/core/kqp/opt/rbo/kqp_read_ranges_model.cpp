#include "kqp_read_ranges_model.h"

#include "kqp_olap_expr_inspection.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_type_ops.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <util/charset/utf8.h>
#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

#include <cmath>
#include <limits>

namespace NKikimr::NKqp::NOpt::NReadRanges {

using namespace NYql;
using namespace NYql::NNodes;
using NUdf::EDataSlot;
using NDecimal::TInt128;

namespace {

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
        result << NOpt::GetOlapColumnName(keyColumns[i], true) << ": ";
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

} // namespace

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

std::optional<TRangeList> ParseAsRange(const TExprNode& node) {
    TRangeList result;
    for (const auto& range : node.Children()) {
        if (!range->IsList() || range->ChildrenSize() != 2) {
            return std::nullopt;
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

} // namespace NKikimr::NKqp::NOpt::NReadRanges
