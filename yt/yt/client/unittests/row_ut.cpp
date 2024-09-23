#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <limits>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckSerialize(TUnversionedRow original)
{
    auto serialized = NYT::ToProto<TString>(original);
    auto deserialized =  NYT::FromProto<TUnversionedOwningRow>(serialized);

    ASSERT_EQ(original, deserialized);
}

TEST(TUnversionedRowTest, Serialize1)
{
    TUnversionedOwningRowBuilder builder;
    auto row = builder.FinishRow();
    CheckSerialize(row);
}

TEST(TUnversionedRowTest, Serialize2)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));
    builder.AddValue(MakeUnversionedInt64Value(42, 1));
    builder.AddValue(MakeUnversionedDoubleValue(0.25, 2));
    CheckSerialize(builder.FinishRow());
}

TEST(TUnversionedRowTest, Serialize3)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue("string1", 10));
    builder.AddValue(MakeUnversionedInt64Value(1234, 20));
    builder.AddValue(MakeUnversionedStringValue("string2", 30));
    builder.AddValue(MakeUnversionedDoubleValue(4321.0, 1000));
    builder.AddValue(MakeUnversionedStringValue("", 10000));
    CheckSerialize(builder.FinishRow());
}

TEST(TUnversionedRowTest, Serialize4)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue("string1"));
    builder.AddValue(MakeUnversionedStringValue("string2"));
    CheckSerialize(builder.GetRow());
}

TEST(TUnversionedRowTest, Serialize5)
{
    CheckSerialize(TUnversionedRow());
}

TEST(TUnversionedValueTest, CompareNaN)
{
    auto nanValue = MakeUnversionedDoubleValue(std::numeric_limits<double>::quiet_NaN());
    auto doubleValue = MakeUnversionedDoubleValue(3.14);
    EXPECT_EQ(CompareRowValues(nanValue, nanValue), 0);
    EXPECT_EQ(CompareRowValues(nanValue, doubleValue), 1);
    EXPECT_EQ(CompareRowValues(doubleValue, nanValue), -1);

    static const char* stringValueData = "foo";
    auto stringValue = MakeUnversionedStringValue(stringValueData);

    EXPECT_NO_THROW(CompareRowValues(nanValue, stringValue));
    EXPECT_NO_THROW(CompareRowValues(stringValue, nanValue));
}

TEST(TUnversionedValueTest, CompareComposite)
{
    auto compositeValue = MakeUnversionedCompositeValue("[]");
    auto stringValue = MakeUnversionedStringValue("foo");
    auto anyValue = MakeUnversionedAnyValue("[]");
    auto nullValue = MakeUnversionedSentinelValue(EValueType::Null);
    EXPECT_THROW_WITH_SUBSTRING(CompareRowValues(compositeValue, stringValue), "Cannot compare values of types");
    EXPECT_THROW_WITH_SUBSTRING(CompareRowValues(stringValue, compositeValue), "Cannot compare values of types");

    EXPECT_THROW_WITH_SUBSTRING(CompareRowValues(compositeValue, anyValue), "Cannot compare values of types");
    EXPECT_THROW_WITH_SUBSTRING(CompareRowValues(anyValue, compositeValue), "Cannot compare values of types");

    EXPECT_TRUE(CompareRowValues(compositeValue, nullValue) > 0);
    EXPECT_TRUE(CompareRowValues(nullValue, compositeValue) < 0);
}

TEST(TUnversionedValueTest, CompareAny)
{
    auto intListValue = MakeUnversionedAnyValue("[123]");
    auto stringListValue = MakeUnversionedAnyValue("[\"0\"]");
    auto emptyListValue = MakeUnversionedAnyValue("[]");
    auto listListValue = MakeUnversionedAnyValue("[[abc]]");
    auto stringValue = MakeUnversionedStringValue("foo");
    auto intValue = MakeUnversionedInt64Value(123);
    auto nullValue = MakeUnversionedSentinelValue(EValueType::Null);

    // Any vs just value
    EXPECT_TRUE(CompareRowValues(stringValue, intListValue) < 0);
    EXPECT_TRUE(CompareRowValues(intListValue, stringValue) > 0);

    // String vs int as any & just value
    EXPECT_TRUE(CompareRowValues(stringValue, intValue) > 0);
    EXPECT_TRUE(CompareRowValues(intValue, stringValue) < 0);

    EXPECT_TRUE(CompareRowValues(stringListValue, intListValue) > 0);
    EXPECT_TRUE(CompareRowValues(intListValue, stringListValue) < 0);

    // Null, empty list
    EXPECT_TRUE(CompareRowValues(emptyListValue, intListValue) < 0);
    EXPECT_TRUE(CompareRowValues(intListValue, emptyListValue) > 0);

    EXPECT_TRUE(CompareRowValues(intListValue, nullValue) > 0);
    EXPECT_TRUE(CompareRowValues(nullValue, intListValue) < 0);

    EXPECT_TRUE(CompareRowValues(emptyListValue, nullValue) > 0);
    EXPECT_TRUE(CompareRowValues(nullValue, emptyListValue) < 0);

    // List vs int as any & just value
    EXPECT_TRUE(CompareRowValues(intValue, intListValue) < 0);
    EXPECT_TRUE(CompareRowValues(intListValue, intValue) > 0);

    EXPECT_TRUE(CompareRowValues(intListValue, listListValue) < 0);
    EXPECT_TRUE(CompareRowValues(listListValue, intListValue) > 0);

    // Any map & attrs
    auto mapValue = MakeUnversionedAnyValue("{a=123}");
    EXPECT_THROW_WITH_ERROR_CODE(CompareRowValues(intListValue, mapValue), EErrorCode::IncomparableComplexValues);

    auto annotatedValue = MakeUnversionedAnyValue("[<a=10>123]");
    EXPECT_THROW_WITH_ERROR_CODE(CompareRowValues(intListValue, annotatedValue), EErrorCode::IncomparableComplexValues);

    // Lazy comparison: we assume that such values are filtered before write to sorted column
    auto listWithMapValue = MakeUnversionedAnyValue("[122, {a=123}]");
    EXPECT_TRUE(CompareRowValues(intListValue, listWithMapValue) > 0);
    EXPECT_TRUE(CompareRowValues(listWithMapValue, intListValue) < 0);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFormatTest, UnversionedValue)
{
    auto value = MakeUnversionedInt64Value(123, 7, EValueFlags::Aggregate | EValueFlags::Hunk);

    EXPECT_EQ(Format("%v", value), "%&7#123");
    EXPECT_EQ(Format("%kv", value), "123");
}

TEST(TFormatTest, VersionedValue)
{
    auto value = MakeInt64Value<TVersionedValue>(123, 7, EValueFlags::Aggregate | EValueFlags::Hunk);
    value.Timestamp = 0x1234567890abcdef;

    EXPECT_EQ(Format("%v", value), "%&7#123@1234567890abcdef");
}

TEST(TFormatTest, UnversionedRow)
{
    auto value1 = MakeUnversionedInt64Value(123, 2, EValueFlags::Aggregate);
    auto value2 = MakeUnversionedInt64Value(234, 3, EValueFlags::Hunk);
    auto value3 = MakeUnversionedInt64Value(345, 4, EValueFlags::Aggregate | EValueFlags::Hunk);

    TUnversionedRowBuilder builder;
    builder.AddValue(value1);
    builder.AddValue(value2);
    builder.AddValue(value3);
    auto row = builder.GetRow();

    EXPECT_EQ(Format("%v", row), "[%2#123, &3#234, %&4#345]");
    EXPECT_EQ(Format("%kv", row), "[123, 234, 345]");
}

TEST(TFormatTest, VersionedRow)
{
    auto key1 = MakeUnversionedInt64Value(6, 0);
    auto key2 = MakeUnversionedInt64Value(7, 1, EValueFlags::Hunk);
    auto key3 = MakeUnversionedInt64Value(8, 2, EValueFlags::Aggregate);

    auto value1 = MakeInt64Value<TVersionedValue>(123, 3, EValueFlags::Aggregate);
    value1.Timestamp = 0xaaa;
    auto value2 = MakeInt64Value<TVersionedValue>(234, 4, EValueFlags::Hunk);
    value2.Timestamp = 0xbbb;
    auto value3 = MakeInt64Value<TVersionedValue>(345, 5, EValueFlags::Aggregate | EValueFlags::Hunk);
    value3.Timestamp = 0xccc;

    TVersionedRowBuilder builder(New<TRowBuffer>());
    builder.AddKey(key1);
    builder.AddKey(key2);
    builder.AddKey(key3);

    builder.AddValue(value1);
    builder.AddValue(value2);
    builder.AddValue(value3);

    builder.AddDeleteTimestamp(0xeee);

    auto row = builder.FinishRow();

    EXPECT_EQ(Format("%v", row), "[6, 7, 8 | %3#123@aaa, &4#234@bbb, %&5#345@ccc | ccc, bbb, aaa | eee]");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
