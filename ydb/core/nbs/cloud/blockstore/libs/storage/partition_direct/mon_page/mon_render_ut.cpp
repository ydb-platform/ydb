#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>
#include <util/generic/size_literals.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Supplies touched VChunks and records both access paths used by the renderer.
class TTestTouchedProvider final: public ITouchedProvider
{
public:
    TSet<ui32> Touched;
    mutable size_t GetCallCount = 0;
    mutable size_t GetRegionCallCount = 0;

    // Implemented ITouchedProvider.
    bool Get(ui32 vChunkIndex) const override
    {
        ++GetCallCount;
        return Touched.contains(vChunkIndex);
    }

    TRegionVChunks GetTouchedVChunks(ui32 regionIndex) const override
    {
        ++GetRegionCallCount;
        TRegionVChunks result;
        const ui32 startVChunkIndex = regionIndex * VChunkPerRegionCount;
        for (size_t i = 0; i < VChunkPerRegionCount; ++i) {
            if (Touched.contains(startVChunkIndex + i)) {
                result.Set(i);
            }
        }
        return result;
    }
};

// Counts non-overlapping occurrences of needle in text.
size_t CountOccurrences(TStringBuf text, TStringBuf needle)
{
    size_t result = 0;
    size_t position = 0;
    while ((position = text.find(needle, position)) != TStringBuf::npos) {
        ++result;
        position += needle.size();
    }
    return result;
}

const TVChunkConfigs EmptyVChunkConfigs;
const TTestTouchedProvider EmptyTouchedProvider;

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
                 .BlockSize = 4096,
                 .BlockCount = 16384,
                 .VChunkSize = 1_MB,
                 .VolumeDirectBlockGroupCount = 32,
                 .TouchedVChunkCount = 17,
                 .DiskId = "vol-1",
                 .State = "WORK"},
            .FastPathServiceInfo = TFastPathServiceInfo{.LsnCounter = 100},
        };
    }

    TVector<TDbgSnapshot> MakeOverviewDbgs()
    {
        TVector<TDbgSnapshot> result;
        result.reserve(VChunkPerRegionCount);
        for (size_t i = 0; i < VChunkPerRegionCount; ++i) {
            result.push_back(TDbgSnapshot{.Index = i});
            for (THostIndex host = 0; host < DirectBlockGroupHostCount; ++host)
            {
                result.back().Connections.push_back(TConnectionSnapshot{
                    .HostIndex = host,
                    .DDiskId = {1, 1, host},
                    .PBufferId = {{1, 1, host}},
                });
            }
        }
        return result;
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
            .DirtyMapStats =
                {
                    .PBuffersUsage = {.Count = 1, .Size = 4096},
                    .FreshTotalBytes = 8192,
                    .RottenTotalBytes = 12288,
                },
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
            .PBuffersUsage = {.Count = 1, .Size = 4096},
        };
    }

    Y_UNIT_TEST(OverviewShowsHeaderAndSummary)
    {
        const TString html =
            RenderMonPage(MakeData(), EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "Overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=overview");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=chaos");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=localdb");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunk");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunkcounters");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=latency");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=memory");
        UNIT_ASSERT_STRING_CONTAINS(html, "Volume DirectBlockGroup Count");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Volume DirectBlockGroup Count</td><td>32</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "LSN counter");
        UNIT_ASSERT_STRING_CONTAINS(html, "vol-1");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "VChunk size</td><td>1.00 MiB = 4.00 KiB * 256 (block)");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Region size</td><td>32.00 MiB = 1.00 MiB * "
            "32 (VChunkPerRegion) = 4.00 KiB * 8192 (block)");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "VChunk count</td><td>64 = 2 (region) * "
            "32 (VChunkPerRegion)");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Touched VChunks</td><td>17 / 64</td>");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Disk size</td><td>64.00 MiB = 4.00 KiB * 16384 (block) = "
            "1.00 MiB * 64 (vchunk) = 32.00 MiB * 2 (region)");
        UNIT_ASSERT_STRING_CONTAINS(html, "Regions</td><td>2</td>");
    }

    Y_UNIT_TEST(OverviewRendersTouchedDDisks)
    {
        TMonPageData data = MakeData();
        data.TabletInfo.TouchedVChunkCount = 3;
        data.TabletInfo.TouchedEnabledDDiskCount = 8;
        data.TabletInfo.TouchedDisabledDDiskCount = 1;

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Touched DDisks size</td><td>"
            "9.00 MiB = 1.00 MiB * 8 (Enabled DDisk) + "
            "1.00 MiB * 1 (Disabled DDisk)");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Storage overhead %</td><td>14.0625% = "
            "(9.00 MiB + 0 B) / 64.00 MiB");
    }

    Y_UNIT_TEST(OverviewRendersCustomizedVChunks)
    {
        const TVChunkConfigs configs{
            {0,
             TVChunkConfig::MakeDefault(
                 0,
                 DirectBlockGroupHostCount,
                 DefaultPrimaryCount)},
            {1,
             TVChunkConfig::MakeDefault(
                 1,
                 DirectBlockGroupHostCount,
                 DefaultPrimaryCount)},
        };

        const TString html =
            RenderMonPage(MakeData(), configs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Customized VChunks</td><td>2 / 64</td>");
        UNIT_ASSERT(
            html.find("Touched VChunks") < html.find("Customized VChunks"));
    }

    Y_UNIT_TEST(OverviewRendersUsedPBuffers)
    {
        TMonPageData data = MakeData();
        data.Dbgs = {MakeDbg(0), MakeDbg(1)};

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "Used PBuffers size</td><td>8.00 KiB 2 (count)</td>");
    }

    Y_UNIT_TEST(OverviewRendersDbgTableColumnsAndRows)
    {
        TMonPageData data = MakeData();
        data.Dbgs.resize(33);
        for (size_t i = 0; i < data.Dbgs.size(); ++i) {
            data.Dbgs[i].Index = i;
        }
        data.Dbgs[0].Connections = {TConnectionSnapshot{
            .DDiskId = {20, 100, 1},
            .PBufferId = {{10, 100, 2}},
        }};
        data.Dbgs[32].Connections = {TConnectionSnapshot{
            .DDiskId = {30, 200, 1},
            .PBufferId = {{20, 200, 2}},
        }};

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "Direct Block Group config");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "dbg=0'>DBG #0</a><br><a href='?TabletID=42&page=dbg&dbg=32'>"
            "DBG #32");

        const size_t node10 = html.find("Node 10");
        const size_t node20 = html.find("Node 20");
        const size_t node30 = html.find("Node 30");
        UNIT_ASSERT(node10 != TString::npos);
        UNIT_ASSERT(node20 != TString::npos);
        UNIT_ASSERT(node30 != TString::npos);
        UNIT_ASSERT(node10 < node20);
        UNIT_ASSERT(node20 < node30);
        UNIT_ASSERT_VALUES_EQUAL(35, CountOccurrences(html, "<th>"));
    }

    Y_UNIT_TEST(OverviewReadsTouchedVChunksByRegion)
    {
        TMonPageData data = MakeData();
        data.Dbgs = MakeOverviewDbgs();

        TTestTouchedProvider touchedProvider;
        touchedProvider.Touched = {0, 31, 32, 63};

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, touchedProvider);

        UNIT_ASSERT_VALUES_EQUAL(0, touchedProvider.GetCallCount);
        UNIT_ASSERT_VALUES_EQUAL(2, touchedProvider.GetRegionCallCount);
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            CountOccurrences(html, "DDisk:&#10;Primary:6&#10;PBuffer: 10"));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            CountOccurrences(html, "DDisk:&#10;Primary:12&#10;PBuffer: 20"));
    }

    Y_UNIT_TEST(OverviewAppliesRealVChunkConfig)
    {
        TMonPageData data = MakeData();
        data.Dbgs = MakeOverviewDbgs();

        auto config = TVChunkConfig::MakeDefault(
            0,
            DirectBlockGroupHostCount,
            DefaultPrimaryCount);
        config.PromoteHost(3);
        config.DisableHost(0);
        const TVChunkConfigs configs{{0, std::move(config)}};
        data.Dbgs[0].FreshDDisks[0].Set(3);

        TTestTouchedProvider touchedProvider;
        touchedProvider.Touched = {0};

        const TString html = RenderMonPage(data, configs, touchedProvider);

        UNIT_ASSERT_VALUES_EQUAL(1, touchedProvider.GetCallCount);
        UNIT_ASSERT_VALUES_EQUAL(2, touchedProvider.GetRegionCallCount);
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            CountOccurrences(
                html,
                "DDisk:&#10;Primary:2&#10;Fresh:1&#10;Rotten:1&#10;PBuffer: "
                "4"));
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "class=\"dbg-config-cell dbg-config-both dbg-config-fresh "
            "dbg-config-rotten\"");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "class=\"dbg-config-cell dbg-config-total dbg-config-fresh "
            "dbg-config-rotten\"");
    }

    Y_UNIT_TEST(MemoryPageShowsPerDbgAndTotalUsage)
    {
        TDbgSnapshot first = MakeDbg(1);
        first.MemoryStats.UsedSize = 1024;
        first.MemoryStats.ReservedSize = 4096;
        first.DetailedMemoryStats = {
            {.SlotSize = 256,
             .ArenaSize = 1_MB,
             .ReservedSize = 16_KB,
             .UsedSize = 5_KB,
             .MaxUsedSize = 8_KB}};
        TDbgSnapshot second = MakeDbg(2);
        second.MemoryStats.UsedSize = 2048;
        second.MemoryStats.ReservedSize = 8192;
        second.DetailedMemoryStats = {
            {.SlotSize = 512,
             .ArenaSize = 2_MB,
             .ReservedSize = 32_KB,
             .UsedSize = 7_KB,
             .MaxUsedSize = 12_KB}};

        TMonPageData data{
            .Page = EMonPage::Memory,
            .TabletInfo = {.TabletId = 42},
            .FastPathServiceInfo =
                TFastPathServiceInfo{
                    .ArenaMemoryUsage =
                        {.Slots =
                             {{.SlotSize = 256,
                               .ArenaSize = 1_MB,
                               .ReservedSize = 16_KB,
                               .UsedSize = 5_KB,
                               .MaxUsedSize = 8_KB},
                              {.SlotSize = 512,
                               .ArenaSize = 2_MB,
                               .ReservedSize = 32_KB,
                               .UsedSize = 7_KB,
                               .MaxUsedSize = 12_KB}}}},
            .Dbgs = {std::move(first), std::move(second)},
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "Arena allocator");
        UNIT_ASSERT(!html.Contains("partition_direct tablet"));
        UNIT_ASSERT(!html.Contains("<td>TabletId</td>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "Memory usage by DBG");
        UNIT_ASSERT_STRING_CONTAINS(html, "256 B");
        UNIT_ASSERT_STRING_CONTAINS(html, "512 B");
        UNIT_ASSERT_STRING_CONTAINS(html, "5.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "8.00 KiB");
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

    Y_UNIT_TEST(EscapesHtmlInHeader)
    {
        TMonPageData data = MakeData();
        data.TabletInfo.DiskId = "<script>alert(1)</script>";

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT(!html.Contains("<script>alert(1)</script>"));
        UNIT_ASSERT_STRING_CONTAINS(html, "&lt;script&gt;");
    }

    Y_UNIT_TEST(RuntimeErrorBanner)
    {
        TMonPageData data = MakeData();
        data.FastPathServiceInfo.reset();
        data.RuntimeError = "tablet is initializing";

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "initializing");
    }

    Y_UNIT_TEST(ChaosPageShowsNodeByDbgControls)
    {
        using EChaosMode = TChaosConfig::TChaosNodeConfig::EChaosMode;

        const TMonPageData data{
            .Page = EMonPage::Chaos,
            .TabletInfo = {.TabletId = 42},
            .Dbgs =
                {
                    TDbgSnapshot{
                        .Index = 0,
                        .Connections =
                            {
                                TConnectionSnapshot{
                                    .DDiskId = {10, 100, 1},
                                },
                            },
                    },
                    TDbgSnapshot{
                        .Index = 1,
                        .Connections =
                            {
                                TConnectionSnapshot{
                                    .DDiskId = {10, 101, 1},
                                },
                            },
                    },
                },
            .Chaos =
                TChaosConfig{
                    .NodeConfigs =
                        {
                            {
                                TChaosConfig::TDbgAndNodeId{
                                    .NodeId = 10,
                                    .DbgIndex = 1,
                                },
                                TChaosConfig::TChaosNodeConfig{
                                    .Mode = EChaosMode::Disabled,
                                },
                            },
                        },
                },
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "Chaos");
        UNIT_ASSERT_STRING_CONTAINS(html, "Node 10");
        UNIT_ASSERT_STRING_CONTAINS(html, "DBG #0");
        UNIT_ASSERT_STRING_CONTAINS(html, "DBG #1");
        UNIT_ASSERT_STRING_CONTAINS(html, "action=disable&node=10&dbg=0");
        UNIT_ASSERT_STRING_CONTAINS(html, "action=enable&node=10&dbg=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "chaos-toggle-on");
        UNIT_ASSERT_STRING_CONTAINS(html, "chaos-toggle-off");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "title='Disable node 10 in all DBGs'");
        UNIT_ASSERT(!html.Contains("All DBGs</th>"));
        UNIT_ASSERT(!html.Contains(">Disable</button>"));
        UNIT_ASSERT(!html.Contains(">Enable</button>"));
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "<input type='hidden' name='dbg' value='all'/>");
    }

    Y_UNIT_TEST(ChaosPageHandlesEmptyDbgList)
    {
        const TMonPageData data{
            .Page = EMonPage::Chaos,
            .TabletInfo = {.TabletId = 42},
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "No Direct Block Groups.");
    }

    Y_UNIT_TEST(DbgListShowsRollupAndDrilldownLinks)
    {
        const TMonPageData data{
            .Page = EMonPage::Dbg,
            .TabletInfo = {.TabletId = 42},
            .Dbgs = {MakeDbg(0), MakeDbg(1)},
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "Direct Block Groups");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=0");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 Online");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 Sufferer");
        UNIT_ASSERT_STRING_CONTAINS(html, "Consecutive<br>success");
        UNIT_ASSERT_STRING_CONTAINS(html, "PBuffers<br>usage");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 / 4.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "8.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "12.00 KiB");
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

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "DBG #1");
        UNIT_ASSERT_STRING_CONTAINS(
            html,
            "WriteToPBuffer");   // operation column
        UNIT_ASSERT_STRING_CONTAINS(html, "back to DBGs");
        // Host indexes render in the log format ("H0"), not as raw ui8 bytes.
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>H0</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "1 / 4.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "8.00 KiB");
        UNIT_ASSERT_STRING_CONTAINS(html, "12.00 KiB");
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
        UNIT_ASSERT_STRING_CONTAINS(html, "connected");
    }

    Y_UNIT_TEST(DbgDetailNotFound)
    {
        const TMonPageData data{
            .Page = EMonPage::Dbg,
            .Dbgs = {MakeDbg(0)},
            .SelectedDbg = 9,
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "not found");
    }

    Y_UNIT_TEST(VChunkPageShowsInputForm)
    {
        const TMonPageData data{
            .Page = EMonPage::VChunk,
            .TabletInfo = {.TabletId = 42},
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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
        const TMonPageData data{
            .Page = EMonPage::VChunk,
            .TabletInfo = {.TabletId = 42},
            .SelectedVChunk = 5,
            .VChunk =
                TVChunkSnapshot{
                    .VChunkConfig = config,
                    .SafeBarrier = TPBufferKey{.Generation = 1, .Lsn = 100},
                    .DirtyMapDump = "DDiskStates: dump-text",
                },
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "VChunk #5");
        UNIT_ASSERT_STRING_CONTAINS(html, "page=dbg&dbg=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "Safe barrier");
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>H0</td>");
        UNIT_ASSERT_STRING_CONTAINS(html, "Primary");
        UNIT_ASSERT_STRING_CONTAINS(html, "HandOff");
        UNIT_ASSERT_STRING_CONTAINS(html, "DDiskStates: dump-text");
    }

    Y_UNIT_TEST(VChunkPageNotFound)
    {
        const TMonPageData data{
            .Page = EMonPage::VChunk,
            .TabletInfo = {.TabletId = 42},
            .SelectedVChunk = 999,
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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
                },
        };
        const TVChunkConfigs configs{
            {3, TVChunkConfig::MakeDefault(3, 5, 3)},
        };

        const TString html = RenderMonPage(data, configs, EmptyTouchedProvider);
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
        UNIT_ASSERT_STRING_CONTAINS(html, "<td>3</td>");
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

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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
            .SelectedDbg = 0,
            .VChunkStats = gathered,
            .ShowVChunks = true,
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
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
            .SelectedDbg = 0,
            .VChunkStats = gathered,
            .VChunkStatsLimit = 1,
            .ShowVChunks = true,
        };

        const TString html =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(html, "page=vchunk&vchunk=0");
        UNIT_ASSERT(!html.Contains("page=vchunk&vchunk=1"));
        UNIT_ASSERT(!html.Contains("page=vchunk&vchunk=3"));
        UNIT_ASSERT_STRING_CONTAINS(html, "Showing 1 of 3 non-zero vchunks");
        UNIT_ASSERT_STRING_CONTAINS(html, "&all=1");

        data.VChunkStatsLimit = 0;
        const TString all =
            RenderMonPage(data, EmptyVChunkConfigs, EmptyTouchedProvider);
        UNIT_ASSERT_STRING_CONTAINS(all, "page=vchunk&vchunk=0");
        UNIT_ASSERT_STRING_CONTAINS(all, "page=vchunk&vchunk=1");
        UNIT_ASSERT_STRING_CONTAINS(all, "page=vchunk&vchunk=3");
        UNIT_ASSERT(!all.Contains("Showing 1 of"));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
