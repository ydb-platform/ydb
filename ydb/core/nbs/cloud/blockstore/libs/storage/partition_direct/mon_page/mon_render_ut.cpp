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

    TDbgSnapshot MakeDbg(size_t index)
    {
        TDbgSnapshot dbg;
        dbg.Index = index;
        dbg.VChunkCount = 32;

        THostSnapshot online;
        online.Index = 0;
        online.State = EHostState::Online;
        online.Health = EHostHealthView::Online;
        online.InflightByOp[static_cast<size_t>(EOperation::WriteToPBuffer)] =
            3;
        online.Errors.ErrorCount = 1;
        online.PBufferUsedSize = 4096;
        dbg.Hosts.push_back(online);

        THostSnapshot sufferer;
        sufferer.Index = 1;
        sufferer.Health = EHostHealthView::Sufferer;
        dbg.Hosts.push_back(sufferer);
        return dbg;
    }

    Y_UNIT_TEST(OverviewShowsHeaderAndSummary)
    {
        const TString html = RenderMonPage(MakeData());
        UNIT_ASSERT_STRING_CONTAINS(html, "partition_direct tablet");
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg");
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

    Y_UNIT_TEST(RuntimeErrorBanner)
    {
        TMonPageData data = MakeData();
        data.FastPathServiceInfo.reset();
        data.RuntimeError = "tablet is initializing";

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "initializing");
    }

    Y_UNIT_TEST(DbgListShowsRollupAndDrilldownLinks)
    {
        TMonPageData data;
        data.Page = EMonPage::Dbg;
        data.TabletInfo.TabletId = 42;
        data.Dbgs.push_back(MakeDbg(0));
        data.Dbgs.push_back(MakeDbg(1));

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "Direct Block Groups");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=0");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1");
        // Health rollup: one Online host + one Sufferer host per DBG.
        UNIT_ASSERT_STRING_CONTAINS(html, "1 Online");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 Sufferer");
    }

    Y_UNIT_TEST(DbgDetailShowsHostsTable)
    {
        TMonPageData data;
        data.Page = EMonPage::Dbg;
        data.TabletInfo.TabletId = 42;
        data.SelectedDbg = 1;
        data.Dbgs.push_back(MakeDbg(1));   // only the selected DBG is gathered

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "DBG #1");
        UNIT_ASSERT_STRING_CONTAINS(html, "WriteToPBuffer");   // op column
        UNIT_ASSERT_STRING_CONTAINS(html, "back to DBGs");
    }

    Y_UNIT_TEST(DbgDetailNotFound)
    {
        TMonPageData data;
        data.Page = EMonPage::Dbg;
        data.SelectedDbg = 9;
        data.Dbgs.push_back(MakeDbg(0));

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "not found");
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
