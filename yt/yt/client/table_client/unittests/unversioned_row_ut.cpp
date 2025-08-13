#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
