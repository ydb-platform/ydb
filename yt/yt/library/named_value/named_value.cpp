#include "named_value.h"

#include <yt/yt/core/ytree/convert.h>

#include <util/string/escape.h>

namespace NYT::NNamedValue {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow MakeRow(
    const TNameTablePtr& nameTable,
    const std::initializer_list<TNamedValue>& values)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& v : values) {
        builder.AddValue(v.ToUnversionedValue(nameTable));
    }
    return builder.FinishRow();
}

TUnversionedOwningRow MakeRow(
    const TNameTablePtr& nameTable,
    const std::vector<TNamedValue>& values)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& v : values) {
        builder.AddValue(v.ToUnversionedValue(nameTable));
    }
    return builder.FinishRow();
}

std::vector<TNamedValue> MakeNamedValueList(
    const TNameTablePtr& nameTable,
    TUnversionedRow row)
{
    std::vector<TNamedValue> result;
    result.reserve(row.GetCount());

    for (const auto& value : row) {
        auto namedValue = TNamedValue(TString(nameTable->GetNameOrThrow(value.Id)), TNamedValue::ExtractValue(value));
        result.push_back(std::move(namedValue));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue TNamedValue::ToUnversionedValue(const TNameTablePtr& nameTable) const
{
    const int valueId = nameTable->GetIdOrRegisterName(Name_);
    return std::visit([valueId] (const auto& value) -> TUnversionedValue {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, i64>) {
            return MakeUnversionedInt64Value(value, valueId);
        } else if constexpr (std::is_same_v<T, ui64>) {
            return MakeUnversionedUint64Value(value, valueId);
        } else if constexpr (std::is_same_v<T, double>) {
            return MakeUnversionedDoubleValue(value, valueId);
        } else if constexpr (std::is_same_v<T, bool>) {
            return MakeUnversionedBooleanValue(value, valueId);
        } else if constexpr (std::is_same_v<T, TString>) {
            return MakeUnversionedStringValue(value, valueId);
        } else if constexpr (std::is_same_v<T, TAny>) {
            return MakeUnversionedAnyValue(value.Value, valueId);
        } else if constexpr (std::is_same_v<T, TComposite>) {
            return MakeUnversionedCompositeValue(value.Value, valueId);
        } else {
            static_assert(std::is_same_v<T, std::nullptr_t>);
            return MakeUnversionedSentinelValue(EValueType::Null, valueId);
        }
    }, Value_);
}

TNamedValue::TValue TNamedValue::ExtractValue(const TUnversionedValue& value)
{
    auto getString = [] (const TUnversionedValue& value) {
        return value.AsString();
    };
    switch (value.Type) {
        case EValueType::Null:
            return nullptr;
        case EValueType::Int64:
            return value.Data.Int64;
        case EValueType::Uint64:
            return value.Data.Uint64;
        case EValueType::Boolean:
            return value.Data.Boolean;
        case EValueType::Double:
            return value.Data.Double;
        case EValueType::String:
            return getString(value);
        case EValueType::Any:
            return TAny{getString(value)};
        case EValueType::Composite:
            return TComposite{getString(value)};
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    YT_ABORT();
}

TNamedValue::TValue TNamedValue::ToValue(EValueType valueType, TStringBuf value)
{
    if (valueType == EValueType::String) {
        return TString(value);
    } else if (valueType == EValueType::Any) {
        return TAny{TString(value)};
    } else if (valueType == EValueType::Composite) {
        return TComposite{TString(value)};
    } else {
        YT_ABORT();
    }
}

void FormatValue(TStringBuilderBase* builder, const TNamedValue& value, TStringBuf /*spec*/)
{
    using namespace NYson;

    builder->AppendFormat("%Qv=", value.Name_);
    auto text = std::visit([] (const auto& value) -> TString {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, TNamedValue::TAny>) {
            auto result = TString("<type=any>");
            result += value.Value;
            return ConvertToYsonString(TYsonString(result), EYsonFormat::Text).ToString();
        } else if constexpr (std::is_same_v<T, TNamedValue::TComposite>) {
            auto result = TString("<type=composite>");
            result += ConvertToYsonString(TYsonString(value.Value)).ToString();
            return ConvertToYsonString(TYsonString(result), EYsonFormat::Text).ToString();
        } else if constexpr (std::is_same_v<T, std::nullptr_t>) {
            return "#";
        } else {
            return ConvertToYsonString(value, EYsonFormat::Text).ToString();
        }
    }, value.Value_);

    builder->AppendString(ConvertToYsonString(text, EYsonFormat::Text).AsStringBuf());
}

void PrintTo(const TNamedValue& value, std::ostream* os)
{
    *os << Format("%v", value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
