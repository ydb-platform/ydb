#include "kqp_read_ranges_model.h"

#include "kqp_olap_expr_inspection.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_type_ops.h>
#include <yql/essentials/types/uuid/uuid.h>
#include <yql/essentials/utils/utf8.h>

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
using NUdf::EDataTypeFeatures;
using NDecimal::TInt128;

namespace {

bool HasFeatures(EDataSlot slot, ui32 features) {
    return NUdf::GetDataTypeInfo(slot).Features & features;
}

bool IsIntegral(EDataSlot slot) {
    return HasFeatures(slot, EDataTypeFeatures::IntegralType);
}

bool IsFloat(EDataSlot slot) {
    return HasFeatures(slot, EDataTypeFeatures::FloatType);
}

// String, Utf8 and Uuid values are bytes, ordered as MiniKQL orders strings.
bool IsBytes(EDataSlot slot) {
    return slot == EDataSlot::String || slot == EDataSlot::Utf8 || slot == EDataSlot::Uuid;
}

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
std::optional<NUdf::TUnboxedValuePod> MakeBounded(TInt128 value) {
    if (value < TInt128(std::numeric_limits<T>::min()) || value > TInt128(std::numeric_limits<T>::max())) {
        return std::nullopt;
    }
    return NUdf::TUnboxedValuePod(static_cast<T>(value));
}

// The value of an integral type, nullopt if the type cannot hold it.
std::optional<NUdf::TUnboxedValuePod> MakeIntegral(EDataSlot slot, TInt128 value) {
    switch (slot) {
        case EDataSlot::Int8: return MakeBounded<i8>(value);
        case EDataSlot::Uint8: return MakeBounded<ui8>(value);
        case EDataSlot::Int16: return MakeBounded<i16>(value);
        case EDataSlot::Uint16: return MakeBounded<ui16>(value);
        case EDataSlot::Int32: return MakeBounded<i32>(value);
        case EDataSlot::Uint32: return MakeBounded<ui32>(value);
        case EDataSlot::Int64: return MakeBounded<i64>(value);
        case EDataSlot::Uint64: return MakeBounded<ui64>(value);
        default: return std::nullopt;
    }
}

std::pair<TInt128, TInt128> GetIntegralBounds(EDataSlot slot) {
    switch (slot) {
        case EDataSlot::Int8: return {Min<i8>(), Max<i8>()};
        case EDataSlot::Uint8: return {0, Max<ui8>()};
        case EDataSlot::Int16: return {Min<i16>(), Max<i16>()};
        case EDataSlot::Uint16: return {0, Max<ui16>()};
        case EDataSlot::Int32: return {Min<i32>(), Max<i32>()};
        case EDataSlot::Uint32: return {0, Max<ui32>()};
        case EDataSlot::Int64: return {Min<i64>(), Max<i64>()};
        default: return {0, Max<ui64>()};
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
            const auto type = ParseKeyType(element->Tail());
            if (!type || type->OptionalDepth == 0) {
                return std::nullopt;
            }
            elements.push_back({.Infinite = type->OptionalDepth == 1});
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

std::optional<TKeyType> ParseKeyType(const TExprNode& node) {
    if (node.IsCallable("OptionalType") && node.ChildrenSize() == 1) {
        auto result = ParseKeyType(node.Head());
        if (result) {
            ++result->OptionalDepth;
        }
        return result;
    }
    if (node.IsCallable("DataType") && node.ChildrenSize() > 0 && node.Head().IsAtom()) {
        const auto slot = NUdf::FindDataSlot(node.Head().Content());
        if (!slot) {
            return std::nullopt;
        }
        TKeyType result{.Slot = *slot};
        if (*slot == EDataSlot::Decimal && (node.ChildrenSize() != 3 || !TryFromString(node.Child(1)->Content(), result.Precision)
                                            || !TryFromString(node.Child(2)->Content(), result.Scale))) {
            return std::nullopt;
        }
        return result;
    }
    if (node.IsCallable("PgType") && node.ChildrenSize() == 1 && node.Head().IsAtom()) {
        return TKeyType{.PgType = TString(node.Head().Content())};
    }
    return std::nullopt;
}

bool HasNaN(const TKeyType& type) {
    return type.Slot && (IsFloat(*type.Slot) || *type.Slot == EDataSlot::Decimal);
}

std::optional<TKeyValue> ParseKeyValue(const TExprNode::TPtr& node) {
    if (node->IsCallable("Nothing")) {
        return TKeyValue{};
    }
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return ParseKeyValue(node->HeadPtr());
    }
    if (node->IsCallable({"SafeCast", "StrictCast"}) && node->ChildrenSize() == 2) {
        const auto value = ParseKeyValue(node->HeadPtr());
        const auto type = ParseKeyType(node->Tail());
        if (!value || !type) {
            return std::nullopt;
        }
        const auto conversion = CastToKey(*value, *type);
        if (conversion.Status == TConversion::EStatus::Unknown) {
            return std::nullopt;
        }
        // StrictCast makes a value without an exact counterpart NULL, while SafeCast may round it, as it does a fraction.
        if (conversion.Status == TConversion::EStatus::Null && value->Kind != TKeyValue::EKind::Null && node->IsCallable("SafeCast")
            && IsFloat(value->Slot)) {
            return std::nullopt;
        }
        return conversion.Status == TConversion::EStatus::Value ? conversion.Value : TKeyValue{};
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

TKeyValue MakeNaN(const TKeyType& type) {
    TKeyValue result{.Kind = TKeyValue::EKind::Data, .Slot = *type.Slot, .Precision = type.Precision, .Scale = type.Scale};
    if (*type.Slot == EDataSlot::Float) {
        result.Value = NUdf::TUnboxedValuePod(std::numeric_limits<float>::quiet_NaN());
    } else if (*type.Slot == EDataSlot::Double) {
        result.Value = NUdf::TUnboxedValuePod(std::numeric_limits<double>::quiet_NaN());
    } else {
        result.Value = NUdf::TUnboxedValuePod(NDecimal::Nan());
    }
    return result;
}

TConversion CastToKey(const TKeyValue& value, const TKeyType& type) {
    using EStatus = TConversion::EStatus;
    auto convert = [&](EDataSlot slot, NUdf::TUnboxedValuePod pod) {
        TKeyValue result = value;
        result.Slot = slot;
        result.Value = pod;
        return TConversion{.Status = EStatus::Value, .Value = std::move(result)};
    };

    if (value.Kind == TKeyValue::EKind::Null) {
        return {.Status = EStatus::Null};
    }
    if (!type.Slot) {
        if (value.Kind == TKeyValue::EKind::Pg && value.PgType == type.PgType) {
            return {.Status = EStatus::Value, .Value = value};
        }
        return {};
    }
    if (value.Kind != TKeyValue::EKind::Data) {
        return {};
    }

    const auto from = value.Slot;
    const auto to = *type.Slot;
    if (from == to) {
        if (to == EDataSlot::Decimal && (value.Precision != type.Precision || value.Scale != type.Scale)) {
            return {};
        }
        return {.Status = EStatus::Value, .Value = value};
    }
    if (from == EDataSlot::Utf8 && to == EDataSlot::String) {
        return convert(to, value.Value);
    }
    if (from == EDataSlot::String && to == EDataSlot::Utf8) {
        return IsUtf(value.Bytes) ? convert(to, value.Value) : TConversion{.Status = EStatus::Null};
    }
    if (IsIntegral(from) && IsIntegral(to)) {
        const auto converted = MakeIntegral(to, *GetInteger(value));
        return converted ? convert(to, *converted) : TConversion{.Status = EStatus::Null};
    }
    if (IsFloat(from) && IsIntegral(to)) {
        // Only a whole number within the bounds of the type converts without a loss.
        const double number = from == EDataSlot::Float ? value.Value.Get<float>() : value.Value.Get<double>();
        const auto [min, max] = GetIntegralBounds(to);
        if (!std::isfinite(number) || std::trunc(number) != number || number < static_cast<double>(min) || number > static_cast<double>(max)) {
            return {.Status = EStatus::Null};
        }
        const auto converted = MakeIntegral(to, static_cast<TInt128>(number));
        return converted ? convert(to, *converted) : TConversion{.Status = EStatus::Null};
    }
    // Conversions YQL makes without a loss, such as Int32 to Double.
    const auto castResult = NUdf::GetCastResult(from, to);
    if (castResult && *castResult == NUdf::ECastOptions::Complete && IsFloat(to)) {
        double converted;
        if (IsIntegral(from)) {
            converted = static_cast<double>(*GetInteger(value));
        } else if (from == EDataSlot::Float) {
            converted = value.Value.Get<float>();
        } else {
            return {};
        }
        return convert(to, to == EDataSlot::Float ? NUdf::TUnboxedValuePod(static_cast<float>(converted))
                                                  : NUdf::TUnboxedValuePod(converted));
    }
    return {};
}

TConversion RoundToKey(const TKeyValue& value, const TKeyType& type, bool down) {
    using EStatus = TConversion::EStatus;
    if (value.Kind != TKeyValue::EKind::Data || !type.Slot) {
        return {};
    }
    const auto from = value.Slot;
    const auto to = *type.Slot;
    TKeyValue result = value;
    result.Slot = to;
    if (IsIntegral(from) && IsIntegral(to)) {
        // As the runtime rounds integers: a value past the bounds of the type rounds to the nearer bound or to NULL.
        const auto integer = *GetInteger(value);
        const auto [min, max] = GetIntegralBounds(to);
        if (integer < min) {
            if (down) {
                return {.Status = EStatus::Null};
            }
            result.Value = *MakeIntegral(to, min);
        } else if (integer > max) {
            if (!down) {
                return {.Status = EStatus::Null};
            }
            result.Value = *MakeIntegral(to, max);
        } else {
            result.Value = *MakeIntegral(to, integer);
        }
        return {.Status = EStatus::Value, .Value = std::move(result)};
    }
    if (from == EDataSlot::String && to == EDataSlot::Utf8) {
        const auto rounded = NYql::RoundToNearestValidUtf8(value.Bytes, down);
        if (!rounded) {
            return {.Status = EStatus::Null};
        }
        result.Bytes = TString(*rounded);
        return {.Status = EStatus::Value, .Value = std::move(result)};
    }
    return {};
}

std::optional<TKeyValue> WidePointEnd(const TKeyValue& value) {
    if (value.Kind != TKeyValue::EKind::Data) {
        return std::nullopt;
    }
    // The largest value of every type below, as MakeWidePointRangeLambda in yql_opt_range.cpp has it.
    TInt128 max;
    switch (value.Slot) {
        case EDataSlot::Date: max = NUdf::MAX_DATE - 1; break;
        case EDataSlot::Datetime: max = NUdf::MAX_DATETIME - 1; break;
        case EDataSlot::Timestamp: max = NUdf::MAX_TIMESTAMP - 1; break;
        case EDataSlot::Date32: max = NUdf::MAX_DATE32; break;
        case EDataSlot::Datetime64: max = NUdf::MAX_DATETIME64; break;
        case EDataSlot::Timestamp64: max = NUdf::MAX_TIMESTAMP64; break;
        default:
            if (!IsIntegral(value.Slot)) {
                return std::nullopt;
            }
            max = GetIntegralBounds(value.Slot).second;
    }
    const auto integer = *GetInteger(value);
    if (integer >= max) {
        return std::nullopt;
    }
    TKeyValue result = value;
    switch (value.Slot) {
        case EDataSlot::Date: result.Value = NUdf::TUnboxedValuePod(static_cast<ui16>(integer + 1)); break;
        case EDataSlot::Datetime: result.Value = NUdf::TUnboxedValuePod(static_cast<ui32>(integer + 1)); break;
        case EDataSlot::Timestamp: result.Value = NUdf::TUnboxedValuePod(static_cast<ui64>(integer + 1)); break;
        case EDataSlot::Date32: result.Value = NUdf::TUnboxedValuePod(static_cast<i32>(integer + 1)); break;
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
            result.Value = NUdf::TUnboxedValuePod(static_cast<i64>(integer + 1));
            break;
        default:
            result.Value = *MakeIntegral(value.Slot, integer + 1);
    }
    return result;
}

std::optional<TKeyValue> NextKeyValue(const TKeyValue& value) {
    Y_ENSURE(value.Kind == TKeyValue::EKind::Data && (value.Slot == EDataSlot::String || value.Slot == EDataSlot::Utf8));
    const auto next = value.Slot == EDataSlot::Utf8 ? NYql::NextValidUtf8(value.Bytes) : NYql::NextLexicographicString(value.Bytes);
    if (!next) {
        return std::nullopt;
    }
    TKeyValue result = value;
    result.Bytes = TString(*next);
    return result;
}

std::optional<int> CompareKeyValues(const TKeyValue& left, const TKeyValue& right) {
    using EKind = TKeyValue::EKind;
    if (left.Kind == EKind::Parameter || right.Kind == EKind::Parameter) {
        return std::nullopt;
    }
    if (left.Kind == EKind::Null || right.Kind == EKind::Null) {
        return int(right.Kind == EKind::Null) - int(left.Kind == EKind::Null);
    }
    if (left.Kind != right.Kind) {
        return std::nullopt;
    }
    if (left.Kind == EKind::Pg) {
        // Only Pg integers have an order explain knows; text follows a collation.
        if (left.PgType != right.PgType) {
            return std::nullopt;
        }
        if (left.Bytes == right.Bytes) {
            return 0;
        }
        i64 l, r;
        if ((left.PgType == "int2" || left.PgType == "int4" || left.PgType == "int8")
            && TryFromString(left.Bytes, l) && TryFromString(right.Bytes, r)) {
            return (l > r) - (l < r);
        }
        return std::nullopt;
    }
    if (left.Slot != right.Slot || (left.Slot == EDataSlot::Decimal && (left.Precision != right.Precision || left.Scale != right.Scale))) {
        return std::nullopt;
    }
    if (IsBytes(left.Slot)) {
        const int cmp = left.Bytes.compare(right.Bytes);
        return (cmp > 0) - (cmp < 0);
    }
    return NUdf::CompareValues(left.Slot, left.Value, right.Value);
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
