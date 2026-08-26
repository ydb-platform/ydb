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
            .PBuffersUsage{.Count = 1, .Size = 4096},
            .AheadBlocks{.Count = 2, .Size = 8192},
            .BehindBlocks{.Count = 3, .Size = 12288},
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
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunkcounters");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=latency");
        UNIT_ASSERT_STRING_CONTAINS(html, "DirectBlockGroups");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunks (total)");
        UNIT_ASSERT_STRING_CONTAINS(html, "LSN counter");
        UNIT_ASSERT_STRING_CONTAINS(html, "Last safe barrier");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
    }

    TLatencyStats MakeStats(
        size_t count,
        TDuration min,
        TDuration p50,
        TDuration p90,
        TDuration p99,
        TDuration max)
    {
        return {
            .Count = count,
            .Min = min,
            .P50 = p50,
            .P90 = p90,
            .P99 = p99,
            .Max = max,
        };
    }

    // Two DBGs whose host 0 lands on the same node (1) through different
    // slots — the "32 DBGs on 8 nodes" case in miniature.
    TDbgSnapshot MakeLatencyDbg(
        size_t index,
        ui32 pdiskId,
        ui32 ddiskSlotId,
        ui32 pbufferSlotId,
        const TLatencyStats& writeStats,
        const TLatencyStats& readDDiskStats)
    {
        THostSnapshot host{
            .Index = 0,
            .State = EHostState::Online,
            .Health = EHostHealth::Online,
        };
        host.LatencyByOperation[static_cast<size_t>(
            EOperation::WriteToPBuffer)] = writeStats;
        host.LatencyByOperation[static_cast<size_t>(
            EOperation::ReadFromDDisk)] = readDDiskStats;

        TConnectionSnapshot connection{
            .HostIndex = 0,
            .DDiskId = {
                /*nodeId*/ 1,
                /*pdiskId*/ pdiskId,
                /*ddiskSlotId*/ ddiskSlotId},
            .PBufferId =
                {{/*nodeId*/ 1,
                  /*pdiskId*/ pdiskId,
                  /*ddiskSlotId*/ pbufferSlotId}},
            .DDiskSession = "Locked",
            .PBufferConnected = true,
        };
        return {
            .Index = index,
            .VChunkCount = 32,
            .Hosts = {host},
            .Connections = {connection},
            .LatencyHistoryCapacity = 10,
        };
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
        UNIT_ASSERT_STRING_CONTAINS(html, "PBuffers usage");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 / 4.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "2 / 8.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "3 / 12.00 KiB");
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
        UNIT_ASSERT_STRING_CONTAINS(html, "1 / 4.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "2 / 8.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "3 / 12.00 KiB");
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
                    .VChunkConfigs = {{3, TVChunkConfig::MakeDefault(3, 5, 3)}},
                },
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "Local DB");
        // Long proto dumps are collapsed; the summary is styled to look
        // clickable (fold triangle + pointer).
        UNIT_ASSERT_STRING_CONTAINS(html, "<details");
        UNIT_ASSERT_STRING_CONTAINS(html, "<summary class='pd-summary'");
        UNIT_ASSERT_STRING_CONTAINS(html, "DiskId: vol-1");
        // DirectBlockGroupsConnections / AddHostInProgress not persisted.
        UNIT_ASSERT_STRING_CONTAINS(html, "(none)");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "VChunkConfigs (persisted overrides)");
    }

    Y_UNIT_TEST(LatencyPageShowsHeatmapAndSlots)
    {
        const auto writeStats = MakeStats(
            10,
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(2),
            TDuration::MilliSeconds(3),
            TDuration::MilliSeconds(4),
            TDuration::MilliSeconds(5));
        const auto readStats = MakeStats(
            5,
            TDuration::MicroSeconds(100),
            TDuration::MicroSeconds(200),
            TDuration::MicroSeconds(300),
            TDuration::MicroSeconds(400),
            TDuration::MicroSeconds(500));

        const TMonPageData data{
            .Page = EMonPage::Latency,
            .TabletInfo = {.TabletId = 42},
            .Dbgs =
                {// Same node, two pdisks — exercises pdisk grouping.
                 MakeLatencyDbg(
                     0,
                     /*pdisk*/ 1000,
                     /*ddisk*/ 17,
                     /*pbuffer*/ 18,
                     writeStats,
                     readStats),
                 MakeLatencyDbg(
                     1,
                     /*pdisk*/ 2000,
                     /*ddisk*/ 19,
                     /*pbuffer*/ 20,
                     writeStats,
                     readStats)},
            .SelectedPercentile = ELatencyPercentile::P99,
        };

        const TString html = RenderMonPage(data);
        // No top-level "Latency" section heading — only the three subsections.
        UNIT_ASSERT(!html.Contains("<h3>Latency</h3>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "Latency by node");
        UNIT_ASSERT_STRING_CONTAINS(html, "Latency by slot");
        UNIT_ASSERT_STRING_CONTAINS(html, "Latency detail");
        UNIT_ASSERT_STRING_CONTAINS(html, "WriteToPBuffer");
        UNIT_ASSERT_STRING_CONTAINS(html, "ReadFromDDisk");
        // Percentile selector lives under Latency by node.
        UNIT_ASSERT_STRING_CONTAINS(html, "Percentile:");
        // Operation selector lives under Latency by slot.
        UNIT_ASSERT_STRING_CONTAINS(html, "Slot grid operation:");
        // Auto refresh re-fetches live content (no full page reload).
        UNIT_ASSERT_STRING_CONTAINS(html, "latencyAutoRefresh");
        UNIT_ASSERT_STRING_CONTAINS(html, "latencyRefreshRate");
        UNIT_ASSERT_STRING_CONTAINS(html, "latencyLiveContent");
        UNIT_ASSERT_STRING_CONTAINS(html, "refreshLive");
        UNIT_ASSERT(!html.Contains("location.reload("));
        // Script must come after live content so Show slots / Show data bind
        // on first paint (not only after an auto-refresh swap).
        UNIT_ASSERT(
            html.find("id='latencyLiveContent'") < html.find("refreshLive"));
        // Slots / detail are hidden by default (checkboxes off).
        UNIT_ASSERT_STRING_CONTAINS(html, "latShowSlots");
        UNIT_ASSERT_STRING_CONTAINS(html, "latSlotNodeFilter");
        UNIT_ASSERT_STRING_CONTAINS(html, "latShowDetail");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "id='latSlotsBody' class='lat-hidden'");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "id='latDetailBody' class='lat-hidden'");
        // Single node row for node 1 (both DBGs share it).
        UNIT_ASSERT_STRING_CONTAINS(html, "node 1");
        // Pdisk groups labelled (each pdisk on its own row).
        UNIT_ASSERT_STRING_CONTAINS(html, "pdisk 1000");
        UNIT_ASSERT_STRING_CONTAINS(html, "pdisk 2000");
        // Proportional bar track comes from the stylesheet; fill colour stays
        // inline (data-driven).
        UNIT_ASSERT_STRING_CONTAINS(html, "lat-bar-track");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "background:#90ee90");   // read <500us
        UNIT_ASSERT_STRING_CONTAINS(html, "background:#ffd54f");   // write 4ms
        // Pbuffer / ddisk actor links in the detail table.
        UNIT_ASSERT_STRING_CONTAINS(html, ">1:1000:18</a>");
        UNIT_ASSERT_STRING_CONTAINS(html, ">1:2000:20</a>");
        UNIT_ASSERT_STRING_CONTAINS(html, ">1:1000:17</a>");
        UNIT_ASSERT_STRING_CONTAINS(html, ">1:2000:19</a>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "/node/1/actors/ddisks/ddisk_p000001000_s000000017");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "/node/1/actors/persistent_buffer?pb=");
        // Detail table: filters + sortable columns + PDisk column.
        UNIT_ASSERT_STRING_CONTAINS(html, "latFilterNode");
        UNIT_ASSERT_STRING_CONTAINS(html, "latFilterPdisk");
        UNIT_ASSERT_STRING_CONTAINS(html, "latFilterType");
        UNIT_ASSERT_STRING_CONTAINS(html, "latFilterOp");
        UNIT_ASSERT_STRING_CONTAINS(html, "data-sort='count'");
        UNIT_ASSERT_STRING_CONTAINS(html, "data-sort='p99'");
        UNIT_ASSERT_STRING_CONTAINS(html, "<th>PDisk</th>");
        // p99 of write (4.000ms) appears in the heatmap / detail table.
        UNIT_ASSERT_STRING_CONTAINS(html, "4.000ms");
        // Percentile / operation selectors redraw client-side (no fetch).
        UNIT_ASSERT_STRING_CONTAINS(html, "lat-nav");
        UNIT_ASSERT_STRING_CONTAINS(html, "redrawViews");
        UNIT_ASSERT_STRING_CONTAINS(html, "data-p50=");
        UNIT_ASSERT_STRING_CONTAINS(html, "data-ops=");
        UNIT_ASSERT_STRING_CONTAINS(html, "data-op-names=");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=latency&p=50");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=latency&p=99");
        UNIT_ASSERT_STRING_CONTAINS(html, "history.pushState");
        // lat-nav must not trigger a data refetch.
        UNIT_ASSERT(html.Contains("redrawViews();"));
        UNIT_ASSERT(
            !html.Contains("history.pushState(null,'',href);"
                           "refreshLive();"));
        // JS/CSS resources resolved (inlined into the page).
        UNIT_ASSERT(
            html.Contains("<style>") && html.Contains(".lat-bar-track"));
        UNIT_ASSERT(html.Contains("<script>") && html.Contains("refreshLive"));
        UNIT_ASSERT(!html.Contains("<!-- resource "));
    }

    Y_UNIT_TEST(LatencyPageSelectedPercentileAndOperation)
    {
        const auto writeStats = MakeStats(
            10,
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(2),
            TDuration::MilliSeconds(3),
            TDuration::MilliSeconds(4),
            TDuration::MilliSeconds(5));
        const auto readStats = MakeStats(
            5,
            TDuration::MicroSeconds(100),
            TDuration::MicroSeconds(200),
            TDuration::MicroSeconds(300),
            TDuration::MicroSeconds(400),
            TDuration::MicroSeconds(500));

        const TMonPageData data{
            .Page = EMonPage::Latency,
            .TabletInfo = {.TabletId = 42},
            .Dbgs = {MakeLatencyDbg(
                0,
                /*pdisk*/ 1000,
                /*ddisk*/ 17,
                /*pbuffer*/ 18,
                writeStats,
                readStats)},
            .SelectedPercentile = ELatencyPercentile::P50,
            .SelectedLatencyOperation = EOperation::WriteToPBuffer,
        };

        const TString html = RenderMonPage(data);
        // p50 of write is 2.000ms.
        UNIT_ASSERT_STRING_CONTAINS(html, "2.000ms");
        // Operation filter link highlights WriteToPBuffer and keeps p=50.
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "page=latency&p=50&op=" +
                ToString(static_cast<size_t>(EOperation::WriteToPBuffer)));
        UNIT_ASSERT_STRING_CONTAINS(html, "for WriteToPBuffer");
        UNIT_ASSERT_STRING_CONTAINS(html, "pbuffer");
        UNIT_ASSERT_STRING_CONTAINS(html, "ddisk");
    }

    Y_UNIT_TEST(LatencyPageDisabledWhenCapacityZero)
    {
        TDbgSnapshot dbg = MakeLatencyDbg(
            0,
            /*pdisk*/ 1000,
            17,
            18,
            MakeStats(
                10,
                TDuration::MilliSeconds(1),
                TDuration::MilliSeconds(1),
                TDuration::MilliSeconds(1),
                TDuration::MilliSeconds(1),
                TDuration::MilliSeconds(1)),
            {});
        dbg.LatencyHistoryCapacity = 0;

        const TMonPageData data{
            .Page = EMonPage::Latency,
            .TabletInfo = {.TabletId = 42},
            .Dbgs = {dbg},
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "TimePredictionHistorySize");
        UNIT_ASSERT(!html.Contains("Latency by node"));
        UNIT_ASSERT(!html.Contains("Latency by slot"));
    }

    Y_UNIT_TEST(LatencyPageShowsDashForEmptyOperation)
    {
        // Only WriteToPBuffer has samples; ReadFromDDisk is empty -> dash.
        const auto writeStats = MakeStats(
            3,
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(1));

        const TMonPageData data{
            .Page = EMonPage::Latency,
            .TabletInfo = {.TabletId = 42},
            .Dbgs = {MakeLatencyDbg(
                0,
                /*pdisk*/ 1000,
                /*ddisk*/ 17,
                /*pbuffer*/ 18,
                writeStats,
                /*readDDiskStats*/ {})},
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "<span class='lat-none'>-</span>");
        UNIT_ASSERT_STRING_CONTAINS(html, "WriteToPBuffer");
        // ReadFromDDisk appears as a heatmap column header, but not as a
        // detail-table cell value next to a count (no samples folded).
        UNIT_ASSERT(!html.Contains("<td>ReadFromDDisk</td>"));
    }

    TVChunkStats MakeWriteOk(ui64 ok)
    {
        TVChunkStats stats;
        for (ui64 i = 0; i < ok; ++i) {
            stats.RequestFinished(EVChunkOperation::Write, true);
        }
        return stats;
    }

    Y_UNIT_TEST(VChunkCountersShowsTotalsAndDbgRows)
    {
        TVChunkStatsGatherResult gathered;
        gathered.PerDbg = {
            {.DbgIndex = 0, .Stats = MakeWriteOk(5)},
            {.DbgIndex = 1, .Stats = MakeWriteOk(3)},
        };
        gathered.Total.Accumulate(gathered.PerDbg[0].Stats);
        gathered.Total.Accumulate(gathered.PerDbg[1].Stats);
        gathered.PerVChunk = {
            {.VChunkIndex = 2, .DbgIndex = 1, .Stats = MakeWriteOk(3)},
            {.VChunkIndex = 1, .DbgIndex = 0, .Stats = MakeWriteOk(5)},
        };

        const TMonPageData data{
            .Page = EMonPage::VChunkCounters,
            .TabletInfo = {.TabletId = 42, .DiskId = "vol-1"},
            .VChunkStats = gathered,
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunk counters");
        UNIT_ASSERT_STRING_CONTAINS(html, "Disk totals");
        UNIT_ASSERT_STRING_CONTAINS(html, "Per DBG");
        UNIT_ASSERT_STRING_CONTAINS(html, "Per vchunk");
        UNIT_ASSERT_STRING_CONTAINS(html, ">8<");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=0");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "vcShowVChunks");
        UNIT_ASSERT_STRING_CONTAINS(html, "Show data");
        UNIT_ASSERT_STRING_CONTAINS(html, "vcDbgFilter");
        UNIT_ASSERT(!html.Contains("id='vcCountersForm'"));
        UNIT_ASSERT_STRING_CONTAINS(html, "lat-sortable");
        UNIT_ASSERT_STRING_CONTAINS(html, "lat-hidden");
        UNIT_ASSERT(!html.Contains("page=vchunk&vchunk=1"));
        UNIT_ASSERT(!html.Contains("page=vchunk&vchunk=2"));
    }

    Y_UNIT_TEST(VChunkCountersShowsVChunksWhenRequested)
    {
        TVChunkStatsGatherResult gathered;
        gathered.PerDbg = {{.DbgIndex = 0, .Stats = MakeWriteOk(5)}};
        gathered.PerVChunk = {
            {.VChunkIndex = 1, .DbgIndex = 0, .Stats = MakeWriteOk(5)},
        };
        gathered.Total = gathered.PerDbg[0].Stats;

        const TMonPageData data{
            .Page = EMonPage::VChunkCounters,
            .TabletInfo = {.TabletId = 42},
            .VChunkStats = gathered,
            .SelectedVChunkDbg = 0,
            .ShowVChunks = true,
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunk&vchunk=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "vcVChunksTable");
        UNIT_ASSERT_STRING_CONTAINS(html, "checked");
        UNIT_ASSERT_STRING_CONTAINS(html, "id='vcVChunksBody'>");
        UNIT_ASSERT(!html.Contains("id='vcVChunksBody' class='lat-hidden'"));
    }

    Y_UNIT_TEST(VChunkCountersRespectsRowCap)
    {
        TVChunkStatsGatherResult gathered;
        gathered.PerDbg = {{.DbgIndex = 0, .Stats = MakeWriteOk(6)}};
        gathered.PerVChunk = {
            {.VChunkIndex = 0, .DbgIndex = 0, .Stats = MakeWriteOk(1)},
            {.VChunkIndex = 1, .DbgIndex = 0, .Stats = MakeWriteOk(2)},
            {.VChunkIndex = 2, .DbgIndex = 0, .Stats = {}},
            {.VChunkIndex = 3, .DbgIndex = 0, .Stats = MakeWriteOk(3)},
        };
        for (const auto& row: gathered.PerVChunk) {
            gathered.Total.Accumulate(row.Stats);
        }

        TMonPageData data{
            .Page = EMonPage::VChunkCounters,
            .TabletInfo = {.TabletId = 42},
            .VChunkStats = gathered,
            .VChunkStatsLimit = 1,
            .SelectedVChunkDbg = 0,
            .ShowVChunks = true,
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunk&vchunk=0");
        UNIT_ASSERT(!html.Contains("page=vchunk&vchunk=1"));
        UNIT_ASSERT(!html.Contains("page=vchunk&vchunk=3"));
        UNIT_ASSERT_STRING_CONTAINS(html, "Showing 1 of 3 non-zero vchunks");
        UNIT_ASSERT_STRING_CONTAINS(html, "&all=1");

        data.VChunkStatsLimit = 0;
        const TString all = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(all, "page=vchunk&vchunk=0");
        UNIT_ASSERT_STRING_CONTAINS(all, "page=vchunk&vchunk=1");
        UNIT_ASSERT_STRING_CONTAINS(all, "page=vchunk&vchunk=3");
        UNIT_ASSERT(!all.Contains("Showing 1 of"));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
