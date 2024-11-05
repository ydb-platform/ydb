#include <yt/yt/client/table_client/key.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyTest, Simple)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedDoubleValue(3.14, 0));
    builder.AddValue(MakeUnversionedInt64Value(-42, 1));
    builder.AddValue(MakeUnversionedUint64Value(27, 2));
    TString str = "Foo";
    builder.AddValue(MakeUnversionedStringValue(str, 3));

    auto row = builder.FinishRow();
    {
        auto key = TKey::FromRow(row);
        EXPECT_EQ(row, key.AsOwningRow());
        EXPECT_EQ(row.Begin(), key.Begin());
        EXPECT_EQ(4, key.GetLength());
    }
    {
        TUnversionedOwningRow shortenedRow(row.FirstNElements(2));
        auto key = TKey::FromRow(row, /*length*/ 2);
        EXPECT_EQ(shortenedRow, key.AsOwningRow());
        EXPECT_EQ(row.Begin(), key.Begin());
        EXPECT_EQ(2, key.GetLength());
    }
}

TEST(TKeyTest, Serialization)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedDoubleValue(3.14, 0));
    builder.AddValue(MakeUnversionedInt64Value(-42, 1));
    builder.AddValue(MakeUnversionedUint64Value(27, 2));
    TString str = "Foo";
    builder.AddValue(MakeUnversionedStringValue(str, 3));
    auto row = builder.FinishRow();
    TKey key = TKey::FromRow(row);

    EXPECT_EQ(
        ConvertToYsonString(key, EYsonFormat::Text).AsStringBuf(),
        "[3.14;-42;27u;\"Foo\";]");
    EXPECT_EQ(
        ConvertToYsonString(TKey(), EYsonFormat::Text).AsStringBuf(),
        "#");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
