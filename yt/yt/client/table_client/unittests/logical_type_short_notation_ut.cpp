#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <util/string/escape.h>

template <>
void Out<NYT::NTableClient::TLogicalTypePtr>(IOutputStream& out, const NYT::NTableClient::TLogicalTypePtr& value)
{
    out << NYT::NTableClient::ToString(*value);
}

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TEST(TParseDataTypesTest, TestBasicTypes)
{
    for (auto simpleType : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
        if (simpleType == ESimpleLogicalValueType::Any || simpleType == ESimpleLogicalValueType::Boolean) {
            continue;
        }
        EXPECT_EQ(*SimpleLogicalType(simpleType), *ParseType(ToString(simpleType)));
    }

    EXPECT_EQ(
        *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any)),
        *ParseType("Optional<Any>"));

    EXPECT_EQ(
        *SimpleLogicalType(ESimpleLogicalValueType::Boolean),
        *ParseType("Bool"));

    EXPECT_ANY_THROW(ParseType("Boolean"));
}

TEST(TParseDataTypesTest, TestOptional)
{
    EXPECT_EQ(
        *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Date32)),
        *ParseType("Optional<Date32>"));
    EXPECT_EQ(
        *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Datetime)),
        *ParseType("Optional  <  Datetime >    "));
    EXPECT_EQ(
        *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Timestamp)),
        *ParseType("Timestamp?"));
    EXPECT_EQ(
        *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Float)),
        *ParseType("\nFloat\t\t?\t\n"));
    EXPECT_EQ(
        *OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Json))),
        *ParseType("Json? ?"));

    EXPECT_ANY_THROW(ParseType("Optional"));
    EXPECT_ANY_THROW(ParseType("Optional<List<>"));
}

TEST(TParseDataTypesTest, TestList)
{
    EXPECT_EQ(
        *ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uuid)),
        *ParseType("List<Uuid>"));

    EXPECT_ANY_THROW(ParseType("List<>"));
    EXPECT_ANY_THROW(ParseType("List<Int32, String>"));
    EXPECT_ANY_THROW(ParseType("List<'String literal'>"));
    EXPECT_ANY_THROW(ParseType("'List'<Int32>"));
}

TEST(TParseDataTypesTest, TestDict)
{
    EXPECT_EQ(
        *DictLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::Datetime)),
        *ParseType("Dict<Int64, Datetime>"));

    EXPECT_ANY_THROW(ParseType("Dict<>"));
    EXPECT_ANY_THROW(ParseType("Dict<Int32>"));
    EXPECT_ANY_THROW(ParseType("Dict<Int32, String, String>"));
    EXPECT_ANY_THROW(ParseType("Dict<'String literal', String>"));
    EXPECT_ANY_THROW(ParseType("'Dict'<Int8, Int16>"));
}

TEST(TParseDataTypesTest, TestDecimal)
{
    EXPECT_EQ(
        *DecimalLogicalType(3, 2),
        *ParseType("Decimal(3, 2)"));

    EXPECT_ANY_THROW(ParseType("Decimal(1, -1)"));
    EXPECT_ANY_THROW(ParseType("Decimal(-1, 1)"));
    EXPECT_ANY_THROW(ParseType("Decimal"));
    EXPECT_ANY_THROW(ParseType("Decimal<1, 1>"));
}

TEST(TParseDataTypesTest, TestVariant)
{
    EXPECT_EQ(
        *VariantTupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            SimpleLogicalType(ESimpleLogicalValueType::Interval),
        }),
        *ParseType("Variant<Bool, Interval>"));

    auto variantStructType = VariantStructLogicalType(std::vector<TStructField>{
        {
            "Am",
            SimpleLogicalType(ESimpleLogicalValueType::Interval64),
        },
        {
            "Cmaj7",
            SimpleLogicalType(ESimpleLogicalValueType::Void),
        },
    });

    EXPECT_EQ(*variantStructType, *ParseType("Variant<Am:Interval64,Cmaj7:Void>"));
    EXPECT_EQ(*variantStructType, *ParseType("  Variant <  Am   : Interval64    , Cmaj7\t:\nVoid  >   "));
    EXPECT_EQ(*variantStructType, *ParseType("Variant<'Am':Interval64,'Cmaj7':Void>"));

    // TODO(ermolovd): Alter this test once empty variants are verboten.
    EXPECT_EQ(
        *VariantTupleLogicalType({}),
        *ParseType("Variant<>"));

    EXPECT_ANY_THROW(ParseType("Variant<Interval64:Am,Void:Cmaj7>"));
    EXPECT_ANY_THROW(ParseType("Variant<Interval64:Am,Void>"));
    EXPECT_ANY_THROW(ParseType("Variant<Interval64:Am,Void>"));
}

TEST(TParseDataTypesTest, TestTuple)
{
    EXPECT_EQ(*TupleLogicalType({}), *ParseType("Tuple<>"));
    EXPECT_EQ(*TupleLogicalType({TupleLogicalType({})}), *ParseType("Tuple<Tuple<>>"));
    EXPECT_EQ(
        *TupleLogicalType({
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any)),
            SimpleLogicalType(ESimpleLogicalValueType::Void),
            SimpleLogicalType(ESimpleLogicalValueType::Timestamp64),
        }),
        *ParseType("Tuple<Any?, Void, Timestamp64>"));

    EXPECT_ANY_THROW(ParseType("Tuple"));
    EXPECT_ANY_THROW(ParseType("Tuple<Am:Int64,Cmaj7:Datetime>"));
}

TEST(TParseDataTypesTest, TestTagged)
{
    EXPECT_EQ(
        *TaggedLogicalType("TAG", SimpleLogicalType(ESimpleLogicalValueType::Uint16)),
        *ParseType("Tagged<Uint16, TAG>"));
    EXPECT_EQ(
        *TaggedLogicalType("TAG", SimpleLogicalType(ESimpleLogicalValueType::Uint8)),
        *ParseType("Tagged<Uint8, 'TAG'>"));

    EXPECT_ANY_THROW(ParseType("Tagged<'Uint64', TAG>"));
    EXPECT_ANY_THROW(ParseType("Tagged<Uint8, 'TAG>"));
    EXPECT_ANY_THROW(ParseType("Tagged<Uint8>"));
    EXPECT_ANY_THROW(ParseType("Tagged<>"));
    EXPECT_ANY_THROW(ParseType("Tagged"));
    EXPECT_ANY_THROW(ParseType("Tagged<Int32, Int64, TAG>"));
}

TEST(TParseDataTypesTest, TestStruct)
{
    auto structType = StructLogicalType(std::vector<TStructField>{
        {
            "Am",
            SimpleLogicalType(ESimpleLogicalValueType::Utf8),
        },
        {
            "Cmaj7",
            SimpleLogicalType(ESimpleLogicalValueType::Null),
        },
    });

    EXPECT_EQ(*structType, *ParseType("Struct<Am:Utf8,Cmaj7:Null>"));
}

TEST(TParseDataTypesTest, TestEscape)
{
    auto taggedType = TaggedLogicalType("'quote_and_single_slash\\", SimpleLogicalType(ESimpleLogicalValueType::Interval));
    EXPECT_EQ(*taggedType, *ParseType("Tagged<Interval, '\\\'quote_and_single_slash\\\\'>"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPrintDataTypesTest, TestVarious)
{
    auto types = std::vector<TLogicalTypePtr>{
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
        TaggedLogicalType(
            "tag",
            DictLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::String),
                SimpleLogicalType(ESimpleLogicalValueType::Int64))),
        VariantTupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        }),
        VariantStructLogicalType(std::vector<TStructField>{
            {
                "Am",
                SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            },
            {
                "Cmaj7",
                SimpleLogicalType(ESimpleLogicalValueType::String),
            }
        }),
        DecimalLogicalType(2, 1),
        StructLogicalType({
            {"number",  SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"english", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"russian", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        }),
        ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        TupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::String),
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)),
        }),
    };

    for (const auto& type : types) {
        auto typeString = ToString(type);
        auto parsedType = ParseType(typeString);

        EXPECT_EQ(*type, *parsedType);
    }
}

TEST(TPrintDataTypesTest, TestEscape)
{
    auto taggedType = TaggedLogicalType("'quote_and_single_slash\\", SimpleLogicalType(ESimpleLogicalValueType::Interval));
    auto typeString = ToString(*taggedType);
    auto expected = TString("Tagged<Interval,'\\\'quote_and_single_slash\\\\'>");

    EXPECT_EQ(typeString, expected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
