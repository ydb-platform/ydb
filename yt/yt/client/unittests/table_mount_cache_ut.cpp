#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NTabletClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TTableMountInfoPtr CreateOrderedTableMountInfo(int tabletCount)
{
    auto tableInfo = New<TTableMountInfo>();
    tableInfo->Path = "//tmp/t";
    for (int index = 0; index < tabletCount; ++index) {
        tableInfo->Tablets.push_back(New<TTabletInfo>());
    }

    return tableInfo;
}

TEST(TTableMountInfoTest, GetTabletByIndexOrThrow)
{
    auto tableInfo = CreateOrderedTableMountInfo(3);

    EXPECT_EQ(tableInfo->GetTabletByIndexOrThrow(0), tableInfo->Tablets[0]);
    EXPECT_EQ(tableInfo->GetTabletByIndexOrThrow(2), tableInfo->Tablets[2]);

    EXPECT_THROW_WITH_SUBSTRING(
        tableInfo->GetTabletByIndexOrThrow(-1),
        "Invalid tablet index");

    EXPECT_THROW_WITH_SUBSTRING(
        tableInfo->GetTabletByIndexOrThrow(3),
        "Invalid tablet index");
}

TEST(TTableMountInfoTest, GetTabletByIndexOrThrowRejectsNarrowedIndex)
{
    auto tableInfo = CreateOrderedTableMountInfo(3);
    EXPECT_THROW_WITH_SUBSTRING(
        tableInfo->GetTabletByIndexOrThrow((i64(9994) << 32) | 1),
        "Invalid tablet index");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletClient
