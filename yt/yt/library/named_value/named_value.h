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

////////////////////////////////////////////////////////////////////////////////

//! Slow but convenient analogue of TUnversionedValue
class TNamedValue
{
public:
    struct TAny {
        TString Value;
    };

    struct TComposite {
        TString Value;
    };

    using TValue = std::variant<std::nullptr_t, i64, ui64, double, bool, TString, TAny, TComposite>;

public:
    template <typename T>
    TNamedValue(TString name, T value)
        : Name_(std::move(name))
        , Value_(std::move(value))
    { }

    template <>
    TNamedValue(TString name, std::nullptr_t)
        : Name_(std::move(name))
        , Value_()
    { }

    template <>
    TNamedValue(TString name, unsigned value)
        : Name_(std::move(name))
        , Value_(static_cast<ui64>(value))
    { }

    template <>
    TNamedValue(TString name, TStringBuf value)
        : Name_(std::move(name))
        , Value_(TString(value))
    { }

    TNamedValue(TString name, NTableClient::EValueType valueType, TStringBuf value)
        : Name_(std::move(name))
        , Value_(ToValue(valueType, TString(value)))
    { }

    NTableClient::TUnversionedValue ToUnversionedValue(const NTableClient::TNameTablePtr& nameTable) const;

    static TValue ExtractValue(const NTableClient::TUnversionedValue& value);

private:
    TValue ToValue(NTableClient::EValueType valueType, TStringBuf value);

private:
    TString Name_;
    TValue Value_;
};

////////////////////////////////////////////////////////////////////////////////

bool operator ==(const TNamedValue::TAny& lhs, const TNamedValue::TAny& rhs);
bool operator ==(const TNamedValue::TComposite& lhs, const TNamedValue::TComposite& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNamedValue
