#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TUnversionedOwningValueTest, DefaultCtor)
{
    TUnversionedOwningValue owningValue;
    ASSERT_EQ(owningValue.Type(), EValueType::TheBottom);
}

TEST(TUnversionedOwningValueTest, String)
{
    std::string string = "Hello world!";
    TUnversionedOwningValue owningValue(MakeUnversionedStringValue(string));
    TUnversionedValue value = owningValue;
    ASSERT_EQ(owningValue.Type(), EValueType::String);
    ASSERT_EQ(TStringBuf(value.Data.String, value.Length), string);
    ASSERT_EQ(owningValue.GetStringRef().ToStringBuf(), string);
}

TEST(TUnversionedOwningValueTest, FromSharedRef)
{
    auto string = TSharedRef::FromString("Hello world!");
    auto owningValue = MakeUnversionedStringOwningValue(string);
    TUnversionedValue value = owningValue;
    ASSERT_EQ(owningValue.Type(), EValueType::String);
    ASSERT_EQ(static_cast<const void*>(value.Data.String), static_cast<const void*>(string.data()));
    ASSERT_EQ(owningValue.GetStringRef().ToStringBuf(), string.ToStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TUnversionedOwningRowTest, DefaultCtor)
{
    TUnversionedOwningRow owningRow;
    ASSERT_EQ(owningRow.GetSpaceUsed(), 0ull);
}

TEST(TUnversionedOwningRowTest, ConstructFromUnversionedRow)
{
    auto buffer = New<TRowBuffer>();
    TUnversionedRowBuilder rowBuilder;
    rowBuilder.AddValue(MakeUnversionedInt64Value(123, 0));
    TUnversionedRow row = rowBuilder.GetRow();

    TUnversionedOwningRow owningRow(row);
    ASSERT_EQ(owningRow.GetCount(), 1);
    ASSERT_GT(owningRow.GetSpaceUsed(), 0ull);
}

TEST(TUnversionedOwningRowTest, MakeUnversionedOwningRowEmpty)
{
    auto owningRow = MakeUnversionedOwningRow();

    TUnversionedOwningRowBuilder builder(0);
    auto builderRow = builder.FinishRow();

    EXPECT_EQ(owningRow, builderRow);
}

TEST(TUnversionedOwningRowTest, MakeUnversionedOwningRowAllInline)
{
    auto owningRow = MakeUnversionedOwningRow(
        std::nullopt,
        i64(123),
        ui64(456),
        i32(789),
        ui32(101112),
        i16(1314),
        ui16(1516),
        i8(17),
        ui8(18),
        bool(true),
        double(3.14),
        TInstant::Seconds(12345),
        TDuration::Seconds(67)
    );

    TUnversionedOwningRowBuilder builder(13);
    builder.AddValue(MakeUnversionedNullValue(0));
    builder.AddValue(MakeUnversionedInt64Value(123, 1));
    builder.AddValue(MakeUnversionedUint64Value(456, 2));
    builder.AddValue(MakeUnversionedInt64Value(789, 3));
    builder.AddValue(MakeUnversionedUint64Value(101112, 4));
    builder.AddValue(MakeUnversionedInt64Value(1314, 5));
    builder.AddValue(MakeUnversionedUint64Value(1516, 6));
    builder.AddValue(MakeUnversionedInt64Value(17, 7));
    builder.AddValue(MakeUnversionedUint64Value(18, 8));
    builder.AddValue(MakeUnversionedBooleanValue(true, 9));
    builder.AddValue(MakeUnversionedDoubleValue(3.14, 10));
    builder.AddValue(MakeUnversionedUint64Value(TInstant::Seconds(12345).MicroSeconds(), 11));
    builder.AddValue(MakeUnversionedUint64Value(TDuration::Seconds(67).MicroSeconds(), 12));
    auto builderRow = builder.FinishRow();

    EXPECT_EQ(owningRow, builderRow);
}

TEST(TUnversionedOwningRowTest, MakeUnversionedOwningRowWithNonInline)
{
    auto owningRow = MakeUnversionedOwningRow(
        i64(123),
        TString("hello"),
        ui64(456),
        TStringBuf("world"),
        bool(true)
    );

    TUnversionedOwningRowBuilder builder(5);
    builder.AddValue(MakeUnversionedInt64Value(123, 0));
    builder.AddValue(MakeUnversionedStringValue("hello", 1));
    builder.AddValue(MakeUnversionedUint64Value(456, 2));
    builder.AddValue(MakeUnversionedStringValue("world", 3));
    builder.AddValue(MakeUnversionedBooleanValue(true, 4));
    auto builderRow = builder.FinishRow();

    EXPECT_EQ(owningRow, builderRow);
}

////////////////////////////////////////////////////////////////////////////////

class TToUnversionedCompositeValueTest
    : public testing::TestWithParam<TYsonStringBuf>
{ };

INSTANTIATE_TEST_SUITE_P(
    Basic,
    TToUnversionedCompositeValueTest,
    ::testing::Values(
        "{foo=bar;baz=1234}",
        "[1;2;3;4;5]"));

TEST_P(TToUnversionedCompositeValueTest, Basic)
{
    auto buffer = New<TRowBuffer>();

#define XX(paramType) \
    do { \
        paramType param{GetParam()}; \
        auto value = ToUnversionedCompositeValue(param, buffer, 42); \
        EXPECT_EQ(value.Id, 42); \
        EXPECT_EQ(value.Type, EValueType::Composite); \
        EXPECT_EQ(value.AsStringBuf(), param.AsStringBuf()); \
        \
        paramType reconstructedParam; \
        FromUnversionedValue(&reconstructedParam, value); \
        EXPECT_EQ(param, reconstructedParam); \
    } while (false)

    XX(TYsonStringBuf);
    XX(TYsonString);

#undef XX
}

TEST_P(TToUnversionedCompositeValueTest, OptionalValue)
{
    auto buffer = New<TRowBuffer>();

#define XX(paramType) \
    do { \
        paramType param{GetParam()}; \
        auto value = ToUnversionedCompositeValue(param, buffer, 42); \
        EXPECT_EQ(value.Id, 42); \
        EXPECT_EQ(value.Type, EValueType::Composite); \
        EXPECT_EQ(value.AsStringBuf(), param->AsStringBuf()); \
        \
        paramType reconstructedParam; \
        FromUnversionedValue(&reconstructedParam, value); \
        EXPECT_EQ(param, reconstructedParam); \
    } while (false)

    XX(std::optional<TYsonStringBuf>);
    XX(std::optional<TYsonString>);

#undef XX
}


TEST_F(TToUnversionedCompositeValueTest, NullValue)
{
    auto buffer = New<TRowBuffer>();

#define XX(paramType) \
    do { \
        paramType param{}; \
        auto value = ToUnversionedCompositeValue(param, buffer, 42); \
        EXPECT_EQ(value.Id, 42); \
        EXPECT_EQ(value.Type, EValueType::Null); \
        \
        paramType reconstructedParam; \
        FromUnversionedValue(&reconstructedParam, value); \
        EXPECT_EQ(param, reconstructedParam); \
    } while (false)

    XX(std::optional<TYsonStringBuf>);
    XX(std::optional<TYsonString>);

#undef XX

}

TEST(TUnversionedRowTest, YsonNullValue)
{
    auto unversioned = MakeUnversionedNullValue();

    TYsonString ysonMap = NYTree::BuildYsonStringFluently().BeginMap().EndMap();
    TYsonString str = ysonMap;
    FromUnversionedValue(&str, unversioned);
    EXPECT_FALSE(str);

    TYsonStringBuf buf = ysonMap;
    FromUnversionedValue(&buf, unversioned);
    EXPECT_FALSE(buf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
