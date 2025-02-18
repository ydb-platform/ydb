#include "value_examples.h"

#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/library/decimal/decimal.h>

#include <cmath>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NLogicalTypeShortcuts;
using namespace NNamedValue;

////////////////////////////////////////////////////////////////////////////////

TValueExample::TValueExample(TLogicalTypePtr logicalType, TNamedValue::TValue value, TString prettyYson)
    : LogicalType(std::move(logicalType))
    , Value(std::move(value))
    , PrettyYson(std::move(prettyYson))
{ }

////////////////////////////////////////////////////////////////////////////////

std::vector<TValueExample> GetPrimitiveValueExamples()
{
    static const std::vector<TValueExample> valueExamples = {
        TValueExample{Int8(), 0, "0"},
        TValueExample{Int8(), -5, "-5"},
        TValueExample{Int8(), 42, "42"},
        TValueExample{Int8(), -128, "-128"},
        TValueExample{Int8(), 127, "127"},

        TValueExample{Int16(), 0, "0"},
        TValueExample{Int16(), -6, "-6"},
        TValueExample{Int16(), 43, "43"},
        TValueExample{Int16(), 0x7FFF, "32767"},
        TValueExample{Int16(), -0x8000, "-32768"},

        TValueExample{Int32(), 0, "0"},
        TValueExample{Int32(), -7, "-7"},
        TValueExample{Int32(), 44, "44"},
        TValueExample{Int32(), 0x7FFFFFFF, "2147483647"},
        TValueExample{Int32(), -0x80000000ll, "-2147483648"},

        TValueExample{Int64(), 0, "0"},
        TValueExample{Int64(), -7, "-7"},
        TValueExample{Int64(), 45, "45"},
        TValueExample{Int64(), 0x7FFFFFFFFFFFFFFFll, "9223372036854775807"},
        TValueExample{Int64(), i64(-0x8000000000000000ll), "-9223372036854775808"},

        TValueExample{Uint8(), 0ull, "0u"},
        TValueExample{Uint8(), 46ull, "46u"},
        TValueExample{Uint8(), 255ull, "255u"},

        TValueExample{Uint16(), 0ull, "0u"},
        TValueExample{Uint16(), 47ull, "47u"},
        TValueExample{Uint16(), 0xFFFFull, "65535u"},

        TValueExample{Uint32(), 0ull, "0u"},
        TValueExample{Uint32(), 48ull, "48u"},
        TValueExample{Uint32(), 0xFFFFFFFFull, "4294967295u"},

        TValueExample{Uint64(), 0ull, "0u"},
        TValueExample{Uint64(), 49ull, "49u"},
        TValueExample{Uint64(), 0xFFFFFFFFFFFFFFFFull, "18446744073709551615u"},

        TValueExample{String(), "", R"("")"},
        TValueExample{String(), "foo", R"("foo")"},
        TValueExample{String(), TString(TStringBuf("\xf0\x00"sv)), R"("\xf0\x00")"},

        TValueExample{Utf8(), "", R"("")"},
        TValueExample{Utf8(), "bar", R"("bar")"},

        TValueExample{Bool(), true, "%true"},
        TValueExample{Bool(), false, "%false"},

        // NB. .125 = 1 / 8 is
        TValueExample{Double(), 3.125, "3.125"},
        TValueExample{Double(), 2.775, "2.775"},
        // TPrimitiveTypeExample{Double(), std::nan("1"), "%nan"},
        TValueExample{Double(), INFINITY, "%inf"},
        TValueExample{Double(), -INFINITY, "%-inf"},

        TValueExample{Float(), 5.125, "5.125"},
        TValueExample{Float(), 6.775, "6.775"},

        TValueExample{Null(), nullptr, "#"},
        TValueExample{Void(), nullptr, "#"},

        TValueExample{Json(), "83", R"("83")"},
        TValueExample{Json(), "[]", R"("[]")"},

        TValueExample{
            Uuid(),
            TString(16, 0),
            TString(TStringBuf(R"("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")"))
        },
        TValueExample{
            Uuid(),
            TString(TStringBuf("\x01\x23\x45\x67\x89\xAB\xCD\xEF\xFE\xDC\xBA\x98\x76\x54\x32\x10"sv)),
            TString(TStringBuf(R"("\x01\x23\x45\x67\x89\xAB\xCD\xEF\xFE\xDC\xBA\x98\x76\x54\x32\x10")"))
        },

        TValueExample{Date(), 0ull, "0u"},
        TValueExample{Date(), 18431ull, "18431u"},
        TValueExample{Date(), 49672ull, "49672u"},

        TValueExample{Datetime(), 0ull, "0u"},
        TValueExample{Datetime(), 668800588ull, "668800588u"},
        TValueExample{Datetime(), 4291747199ull, "4291747199u"},

        TValueExample{Timestamp(), 0ull, "0u"},
        TValueExample{Timestamp(), 2508452463052426ull, "2508452463052426u"},
        TValueExample{Timestamp(), 4291747199999999ull, "4291747199999999u"},

        TValueExample{Interval(), 0, "0"},
        TValueExample{Timestamp(), 2208610308646589ll, "2208610308646589"},
        TValueExample{Timestamp(), 1187314596653899ll, "1187314596653899"},
        TValueExample{Timestamp(), 4291747199999999ll, "4291747199999999"},
        TValueExample{Timestamp(), -4291747199999999ll, "-4291747199999999"},

        TValueExample{Date32(), -53375809, "-53375809"},
        TValueExample{Date32(), 0, "0"},
        TValueExample{Date32(), 53375807, "53375807"},

        TValueExample{Datetime64(), -4611669897600ll, "-4611669897600"},
        TValueExample{Datetime64(), 42, "42"},
        TValueExample{Datetime64(), 4611669811199ll, "4611669811199"},

        TValueExample{Timestamp64(), -4611669897600000000ll, "-4611669897600000000"},
        TValueExample{Timestamp64(), 42, "42"},
        TValueExample{Timestamp64(), 4611669811199999999l, "4611669811199999999"},

        TValueExample{Interval64(), -9223339708799999999ll, "-9223339708799999999"},
        TValueExample{Interval64(), 0, "0"},
        TValueExample{Interval64(), 9223339708799999999ll, "9223339708799999999"},

        TValueExample{Yson(), "qux", R"("qux")"},

        TValueExample{Decimal(3, 2), NDecimal::TDecimal::TextToBinary("3.14", 3, 2), R"("\x80\x00\x01\x3a")"},
    };

    THashSet<ESimpleLogicalValueType> allValueTypes;
    for (const auto value : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
        allValueTypes.insert(value);
    }
    for (const auto& example : valueExamples) {
        if (example.LogicalType->GetMetatype() == ELogicalMetatype::Simple) {
            allValueTypes.erase(example.LogicalType->AsSimpleTypeRef().GetElement());
        }
    }
    if (!allValueTypes.empty()) {
        THROW_ERROR_EXCEPTION("PrimitiveTypeExample variable doesn't contain values: %v",
            allValueTypes);
    }
    return valueExamples;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
