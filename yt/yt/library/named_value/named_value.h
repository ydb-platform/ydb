#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>

#include <initializer_list>

namespace NYT::NNamedValue {

class TNamedValue;

////////////////////////////////////////////////////////////////////////////////

//! Not efficient but convenient way to make a row, useful in tests.
/*!
 *   Meant to be used like this:
 *
 *      auto owningRow = MakeRow(namedTable, {
 *          {"string-column-name", "value"},
 *          {"int-column-name", -42},
 *          {"uint-column-name", 42u},
 *          {"null-column-name", nullptr},
 *          {"any-column-name", EValueType::Any, "{foo=bar}"},
 *          {"composite-column-name", EValueType::Composite, "[1;2;3]"},
 *     });
 */
NTableClient::TUnversionedOwningRow MakeRow(
    const NTableClient::TNameTablePtr& nameTable,
    const std::initializer_list<TNamedValue>& values);

NTableClient::TUnversionedOwningRow MakeRow(
    const NTableClient::TNameTablePtr& nameTable,
    const std::vector<TNamedValue>& values);


std::vector<TNamedValue> MakeNamedValueList(const NTableClient::TNameTablePtr& nameTable, NTableClient::TUnversionedRow row);

////////////////////////////////////////////////////////////////////////////////

//! Slow but convenient analogue of TUnversionedValue
class TNamedValue
{
public:
    struct TAny
    {
        std::string Value;
        friend bool operator ==(const TNamedValue::TAny& lhs, const TNamedValue::TAny& rhs) = default;
    };

    struct TComposite
    {
        std::string Value;
        friend bool operator ==(const TNamedValue::TComposite& lhs, const TNamedValue::TComposite& rhs) = default;
    };

    using TValue = std::variant<std::nullptr_t, i64, ui64, double, bool, std::string, TAny, TComposite>;

public:
    template <typename T>
    TNamedValue(std::string name, T value)
        : Name_(std::move(name))
        , Value_(std::move(value))
    { }

    template <>
    TNamedValue(std::string name, std::nullptr_t)
        : Name_(std::move(name))
    { }

    template <>
    TNamedValue(std::string name, unsigned value)
        : Name_(std::move(name))
        , Value_(static_cast<ui64>(value))
    { }

    template <>
    TNamedValue(std::string name, TStringBuf value)
        : Name_(std::move(name))
        , Value_(std::string(value))
    { }

    TNamedValue(std::string name, NTableClient::EValueType valueType, TStringBuf value)
        : Name_(std::move(name))
        , Value_(ToValue(valueType, std::string(value)))
    { }

    NTableClient::TUnversionedValue ToUnversionedValue(const NTableClient::TNameTablePtr& nameTable) const;

    static TValue ExtractValue(const NTableClient::TUnversionedValue& value);

private:
    TValue ToValue(NTableClient::EValueType valueType, TStringBuf value);

private:
    std::string Name_;
    TValue Value_;

    friend bool operator ==(const TNamedValue& lhs, const TNamedValue& rhs) = default;
    friend void PrintTo(const TNamedValue& value, std::ostream* os);
    friend void FormatValue(TStringBuilderBase* builder, const TNamedValue& value, TStringBuf spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNamedValue
