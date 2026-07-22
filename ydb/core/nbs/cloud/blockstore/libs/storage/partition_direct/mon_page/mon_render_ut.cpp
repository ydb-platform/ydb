#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMonRenderTest)
{
    TMonPageData MakeData()
    {
        return {
            .Page = EMonPage::Overview,
            .TabletInfo =
                {.TabletId = 42,
                 .Generation = 7,
                 .DiskId = "vol-1",
                 .State = "WORK"},
            .FastPathServiceInfo =
                TFastPathServiceInfo{
                    .LsnCounter = 100,
                    .TotalVChunks = 7,
                    .DbgCount = 3},
        };
    }

    TDbgSnapshot MakeDbg(size_t index)
    {
        TInflightByOperation inflightByOperation{};
        inflightByOperation[static_cast<size_t>(EOperation::WriteToPBuffer)] =
            3;

        THostSnapshot online{
            .Index = 0,
            .State = EHostState::Online,
            .Health = EHostHealth::Online,
            .InflightByOperation = inflightByOperation,
            .Errors =
                {.ConsecutiveErrorCount = 1, .ConsecutiveSuccessCount = 7},
            .PBufferUsedSize = 4096,
        };
        THostSnapshot sufferer{
            .Index = 1,
            .Health = EHostHealth::Sufferer,
        };
        TConnectionSnapshot locked{
            .HostIndex = 0,
            .DDiskId = {/*nodeId*/ 1, /*pdiskId*/ 1000, /*ddiskSlotId*/ 17},
            .PBufferId = {{/*nodeId*/ 1, /*pdiskId*/ 1000, /*ddiskSlotId*/ 18}},
            .DDiskSession = "Locked",
            .PBufferConnected = true,
        };
        TConnectionSnapshot notLocked{
            .HostIndex = 1,
            .DDiskSession = "NotLocked",
        };
        return {
            .Index = index,
            .VChunkCount = 32,
            .Hosts = {online, sufferer},
            .Connections = {locked, notLocked},
        };
    }

    Y_UNIT_TEST(OverviewShowsHeaderAndSummary)
    {
        const TString html = RenderMonPage(MakeData());
        UNIT_ASSERT_STRING_CONTAINS(html, "partition_direct tablet");
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=localdb");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunk");
        UNIT_ASSERT_STRING_CONTAINS(html, "DirectBlockGroups");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunks (total)");
        UNIT_ASSERT_STRING_CONTAINS(html, "LSN counter");
        UNIT_ASSERT_STRING_CONTAINS(html, "Last safe barrier");
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
        const TMonPageData data{
            .Page = EMonPage::Dbg,
            .TabletInfo = {.TabletId = 42},
            .Dbgs = {MakeDbg(0), MakeDbg(1)},
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "Direct Block Groups");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=0");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 Online");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 Sufferer");
        UNIT_ASSERT_STRING_CONTAINS(html, "Consecutive success");
        // The add-host button lives on the detail page only.
        UNIT_ASSERT(!html.Contains("action=addhost"));
    }

    Y_UNIT_TEST(DbgDetailShowsHostsTable)
    {
        const TMonPageData data{
            .Page = EMonPage::Dbg,
            .TabletInfo = {.TabletId = 42},
            .Dbgs = {MakeDbg(1)},   // only the selected DBG is gathered
            .SelectedDbg = 1,
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "DBG #1");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "WriteToPBuffer");   // operation column
        UNIT_ASSERT_STRING_CONTAINS(html, "back to DBGs");
        // Host indexes render in the log format ("H0"), not as raw ui8 bytes.
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>H0</td>");
        // The add-host form: POST with parameters both in the URL (read by
        // the tablet) and as hidden fields (read by the mon proxy router).
        UNIT_ASSERT_STRING_CONTAINS(html, "<form method='post'");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1&action=addhost");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<input type='hidden' name='TabletID' value='42'/>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<input type='hidden' name='dbg' value='1'/>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<input type='hidden' name='action' value='addhost'/>");
        UNIT_ASSERT_STRING_CONTAINS(html, "Add host");
        UNIT_ASSERT_STRING_CONTAINS(html, "Connections");
        UNIT_ASSERT_STRING_CONTAINS(html, "DDisk session");
        // The DDisk id links to its actor page on the owning node (1).
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<a href='/node/1/actors/ddisks/ddisk_p000001000_s000000017'>"
            "1:1000:17</a>");
        // The PBuffer id links to the node's Persistent Buffer page filtered
        // to this pbuffer's service actor.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "/node/1/actors/persistent_buffer?pb=");
        UNIT_ASSERT_STRING_CONTAINS(html, ">1:1000:18</a>");
        UNIT_ASSERT_STRING_CONTAINS(html, "Locked");
        UNIT_ASSERT_STRING_CONTAINS(html, "yes");
    }

    Y_UNIT_TEST(DbgDetailNotFound)
    {
        const TMonPageData data{
            .Page = EMonPage::Dbg,
            .Dbgs = {MakeDbg(0)},
            .SelectedDbg = 9,
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "not found");
    }

    Y_UNIT_TEST(VChunkPageShowsInputForm)
    {
        const TMonPageData data{
            .Page = EMonPage::VChunk,
            .TabletInfo = {.TabletId = 42},
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "name='vchunk'");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<input type='hidden' name='page' value='vchunk'/>");
        UNIT_ASSERT(!html.Contains("not found"));
    }

    Y_UNIT_TEST(VChunkPageShowsSnapshot)
    {
        auto config = TVChunkConfig::MakeDefault(
            /*vChunkIndex*/ 5,
            /*hostCount*/ 3,
            /*primaryCount*/ 1);
        config.SetDBGIndex(1);
        config.SetWatermark(0, 7);
        const TMonPageData data{
            .Page = EMonPage::VChunk,
            .TabletInfo = {.TabletId = 42},
            .SelectedVChunk = 5,
            .VChunk =
                TVChunkSnapshot{
                    .VChunkConfig = config,
                    .SafeBarrier = 100,
                    .DirtyMapDump = "DDiskStates: dump-text",
                },
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunk #5");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "Safe barrier");
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>H0</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "Primary");
        UNIT_ASSERT_STRING_CONTAINS(html, "HandOff");
        // Host 0's watermark set above renders in its row.
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>7</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "DDiskStates: dump-text");
    }

    Y_UNIT_TEST(VChunkPageNotFound)
    {
        const TMonPageData data{
            .Page = EMonPage::VChunk,
            .TabletInfo = {.TabletId = 42},
            .SelectedVChunk = 999,
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunk #999 not found");
    }

    Y_UNIT_TEST(LocalDbShowsPersistedState)
    {
        const TMonPageData data{
            .Page = EMonPage::LocalDb,
            .TabletInfo = {.TabletId = 42},
            .LocalDb =
                TLocalDbContents{
                    .VolumeConfig = "DiskId: vol-1",
                    .VChunkConfigs = {TVChunkConfig::MakeDefault(3, 5, 3)},
                },
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "Local DB");
        // Long proto dumps are collapsed; the summary is styled to look
        // clickable (fold triangle + pointer).
        UNIT_ASSERT_STRING_CONTAINS(html, "<details");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<summary style='display:list-item; cursor:pointer;");
        UNIT_ASSERT_STRING_CONTAINS(html, "DiskId: vol-1");
        // DirectBlockGroupsConnections / AddHostInProgress not persisted.
        UNIT_ASSERT_STRING_CONTAINS(html, "(none)");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "VChunkConfigs (persisted overrides)");
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
