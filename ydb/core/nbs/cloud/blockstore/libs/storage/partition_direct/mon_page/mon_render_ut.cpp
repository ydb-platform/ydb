#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMonRenderTest)
{
    TMonPageData MakeData()
    {
        TMonPageData data;
        data.Page = EMonPage::Overview;
        data.TabletInfo.TabletId = 42;
        data.TabletInfo.Generation = 7;
        data.TabletInfo.DiskId = "vol-1";
        data.TabletInfo.State = "WORK";

        TFastPathServiceInfo info;
        info.LsnCounter = 100;
        info.TotalVChunks = 7;
        info.DbgCount = 3;
        data.FastPathServiceInfo = info;
        return data;
    }

    Y_UNIT_TEST(OverviewShowsHeaderAndSummary)
    {
        const TString html = RenderMonPage(MakeData());
        UNIT_ASSERT_STRING_CONTAINS(html, "partition_direct tablet");
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "DirectBlockGroups");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunks (total)");
        UNIT_ASSERT_STRING_CONTAINS(html, "LSN counter");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
    }

    Y_UNIT_TEST(EscapesHtmlInHeader)
    {
        TMonPageData data = MakeData();
        data.TabletInfo.DiskId = "<script>alert(1)</script>";

        const TString html = RenderMonPage(data);
        UNIT_ASSERT(!html.Contains("<script>alert(1)</script>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "&lt;script&gt;");
    }

    Y_UNIT_TEST(InitBannerWhenNoRuntime)
    {
        TMonPageData data = MakeData();
        data.FastPathServiceInfo.reset();

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "initializing");
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
