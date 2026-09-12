#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

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
<<<<<<< HEAD
            .PBufferUsedSize = 4096,
=======
            .PBuffersUsage{.Count = 1, .Size = 4096},
            .FreshTotalBytes = 8192,
            .RottenTotalBytes = 12288,
>>>>>>> 78a20188184 (Field map set optimization (#52565))
        };
        THostSnapshot sufferer{
            .Index = 1,
            .Health = EHostHealth::Sufferer,
        };
        return {
            .Index = index,
            .VChunkCount = 32,
            .Hosts = {online, sufferer},
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
<<<<<<< HEAD
=======
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunk");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunkcounters");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=latency");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=memory");
>>>>>>> 78a20188184 (Field map set optimization (#52565))
        UNIT_ASSERT_STRING_CONTAINS(html, "DirectBlockGroups");
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunks (total)");
        UNIT_ASSERT_STRING_CONTAINS(html, "LSN counter");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
    }

<<<<<<< HEAD
=======
    Y_UNIT_TEST(MemoryPageShowsPerDbgAndTotalUsage)
    {
        TDbgSnapshot first = MakeDbg(1);
        first.UsedMemorySize = 1024;
        first.AllocatedMemorySize = 4096;
        TDbgSnapshot second = MakeDbg(2);
        second.UsedMemorySize = 2048;
        second.AllocatedMemorySize = 8192;

        TMonPageData data{
            .Page = EMonPage::Memory,
            .TabletInfo = {.TabletId = 42},
            .FastPathServiceInfo =
                TFastPathServiceInfo{
                    .ArenaMemoryUsage =
                        {.Slots =
                             {{.SlotSize = 256,
                               .ReservedSize = 16_KB,
                               .UsedSize = 5_KB},
                              {.SlotSize = 512,
                               .ReservedSize = 32_KB,
                               .UsedSize = 7_KB}}}},
            .Dbgs = {std::move(first), std::move(second)},
        };

        const TString html = RenderMonPage(data);
        UNIT_ASSERT_STRING_CONTAINS(html, "Arena allocator");
        UNIT_ASSERT_STRING_CONTAINS(html, "Memory usage by DBG");
        UNIT_ASSERT_STRING_CONTAINS(html, "256 B");
        UNIT_ASSERT_STRING_CONTAINS(html, "512 B");
        UNIT_ASSERT_STRING_CONTAINS(html, "5.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "16.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "48.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=2");
        UNIT_ASSERT_STRING_CONTAINS(html, "1.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "2.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "3.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "12.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "Total");
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

>>>>>>> 78a20188184 (Field map set optimization (#52565))
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
<<<<<<< HEAD
=======
        UNIT_ASSERT_STRING_CONTAINS(html, "PBuffers usage");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 / 4.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "8.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "12.00 KiB");
>>>>>>> 78a20188184 (Field map set optimization (#52565))
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
<<<<<<< HEAD
=======
        // Host indexes render in the log format ("H0"), not as raw ui8 bytes.
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>H0</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 / 4.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "8.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "12.00 KiB");
>>>>>>> 78a20188184 (Field map set optimization (#52565))
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
