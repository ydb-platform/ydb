#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

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

} // namespace
} // namespace NYT::NTableClient
