#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardStats) {

    NKikimrTxDataShard::TEvPeriodicTableStats WaitTableStats(TTestActorRuntime& runtime, size_t minPartCount = 0) {
        NKikimrTxDataShard::TEvPeriodicTableStats stats;
        bool captured = false;

        auto observerFunc = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvPeriodicTableStats::EventType: {
                    stats = ev->Get<TEvDataShard::TEvPeriodicTableStats>()->Record;
                    if (stats.GetTableStats().GetPartCount() >= minPartCount) {
                        captured = true;
                    }
                    break;
                }
                default: {
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(observerFunc);

        for (int i = 0; i < 5 && !captured; ++i) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return captured;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(5));
        }

        runtime.SetObserverFunc(prevObserverFunc);
        UNIT_ASSERT(captured);

        return stats;
    }

    NKikimrTxDataShard::TEvCompactTableResult CompactTable(TTestActorRuntime& runtime, ui64 tabletId, const TPathId& pathId) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvDataShard::TEvCompactTable>(pathId.OwnerId, pathId.LocalPathId);
        runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

        auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
        return ev->Get()->Record;
    }

    NKikimrTableStats::TTableStats GetTableStats(TTestActorRuntime& runtime, ui64 tabletId, ui64 tableId) {
        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvDataShard::TEvGetTableStats>(tableId);
        runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

        auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetTableStatsResult>(sender);
        return ev->Get()->Record.GetTableStats();
    }

    TVector<std::pair<ui64, ui64>> ReadHistogram(NKikimrTableStats::THistogram histogram) {
        TVector<std::pair<ui64, ui64>> result;
        for (auto b : histogram.GetBuckets()) {
            TSerializedCellVec key(b.GetKey());
            auto keyValue = key.GetCells()[0].AsValue<ui32>();
            result.push_back({keyValue, b.GetValue()});
        }
        return result;
    }

    Y_UNIT_TEST(OneChannelStatsCorrect) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3)");

        TPathId pathId;
        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetDataSize(), 0u);
            pathId = TPathId(stats.GetTableOwnerId(), stats.GetTableLocalId());
        }

        CompactTable(runtime, shards.at(0), pathId);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, /* minPartCount */ 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetDataSize(), 0u);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetDataSize(), 
                stats.GetTableStats().GetDataSize() - stats.GetTableStats().GetInMemSize());
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetIndexSize(), 
                stats.GetTableStats().GetIndexSize());
        }
    }

    Y_UNIT_TEST(MultipleChannelsStatsCorrect) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .AddStoragePool("ssd")
            .AddStoragePool("hdd");

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Shards(1)
            .Columns({{"key", "Uint32", true, false}, {"value", "Uint32", false, false}, {"value2", "Uint32", false, false, "hdd"}})
            .Families({{.Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd"}, {.Name = "hdd", .DataPoolKind = "hdd"}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3)");

        TPathId pathId;
        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetDataSize(), 0u);
            pathId = TPathId(stats.GetTableOwnerId(), stats.GetTableLocalId());
        }

        CompactTable(runtime, shards.at(0), pathId);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, /* minPartCount */ 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetDataSize(), 0u);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetChannel(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetChannel(), 2u);
            
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetDataSize() + stats.GetTableStats().GetChannels()[1].GetDataSize(), 
                stats.GetTableStats().GetDataSize() - stats.GetTableStats().GetInMemSize());
            UNIT_ASSERT_GT(stats.GetTableStats().GetChannels()[0].GetDataSize(), 0u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetChannels()[1].GetDataSize(), 0u);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetIndexSize(), stats.GetTableStats().GetIndexSize());
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetIndexSize(), 0);
        }
    }

    Y_UNIT_TEST(HistogramStatsCorrect) {
        const auto gDbStatsDataSizeResolutionBefore = NDataShard::gDbStatsDataSizeResolution;
        const auto gDbStatsRowCountResolutionBefore = NDataShard::gDbStatsRowCountResolution;
        NDataShard::gDbStatsDataSizeResolution = 1; // by page stats
        NDataShard::gDbStatsRowCountResolution = 1;

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        const int count = 2000;
        TString query = "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
        for (auto times = 0; times < count; times++) {
            if (times != 0)
                query += ", ";
            query += "(" + ToString(times) + ", " + ToString(times) + ") ";
        }
        ExecSQL(server, sender, query);

        TPathId pathId;
        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), count);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_GT(stats.GetTableStats().GetDataSize(), 0u);
            pathId = TPathId(stats.GetTableOwnerId(), stats.GetTableLocalId());
        }

        CompactTable(runtime, shards.at(0), pathId);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, /* minPartCount */ 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shards.at(0));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), count);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 30100);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetDataSize(), 
                stats.GetTableStats().GetDataSize() - stats.GetTableStats().GetInMemSize());
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetIndexSize(), 
                stats.GetTableStats().GetIndexSize());
        }

        {
            auto stats = GetTableStats(runtime, shards.at(0), pathId.LocalPathId);

            auto dataSizeHistogram = ReadHistogram(stats.GetDataSizeHistogram());
            TVector<std::pair<ui64, ui64>> expectedDataSizeHistogram = {{475, 7145}, {950, 14290}, {1425, 21435}, {1900, 28580}};
            UNIT_ASSERT_VALUES_EQUAL(expectedDataSizeHistogram, dataSizeHistogram);

            auto rowCountHistogram = ReadHistogram(stats.GetRowCountHistogram());
            TVector<std::pair<ui64, ui64>> expectedRowCountHistogram = {{475, 475}, {950, 950}, {1425, 1425}, {1900, 1900}};
            UNIT_ASSERT_VALUES_EQUAL(expectedRowCountHistogram, rowCountHistogram);
        }

        NDataShard::gDbStatsDataSizeResolution = gDbStatsDataSizeResolutionBefore;
        NDataShard::gDbStatsRowCountResolution = gDbStatsRowCountResolutionBefore;
    }

} // Y_UNIT_TEST_SUITE(DataShardStats)

} // namespace NKikimr
