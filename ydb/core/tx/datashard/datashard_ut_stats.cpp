#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"
#include "ydb/core/tablet_flat/shared_sausagecache.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardStats) {

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
        runtime.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto [shards, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ui64 shard1 = shards.at(0);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3)");

        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 704u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetImmediateTxCompleted(), 1u);
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 65u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 54u);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetChannel(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetDataSize(), 65u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetIndexSize(), 54u);
        }

        Write(runtime, sender, shard1, tableId1, TShardedTableOptions().Columns_, 1, 100, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

        {
            Cerr << "... waiting for stats after write" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 4u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetImmediateTxCompleted(), 2u);
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
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3)");

        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 752u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 0u);
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 115u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 82u);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetChannel(), 1u); // ssd
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetDataSize(), 65u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetIndexSize(), 82u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetChannel(), 2u); // hdd
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetDataSize(), 50u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetIndexSize(), 0u);
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
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        const int count = 2000;
        TString query = "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
        for (auto times = 0; times < count; times++) {
            if (times != 0)
                query += ", ";
            query += "(" + ToString(times) + ", " + ToString(times) + ") ";
        }
        ExecSQL(server, sender, query);

        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), count);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 196096u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 0u);
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), count);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 30100u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 138u);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetChannel(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetDataSize(), 30100u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetIndexSize(), 138u);
        }

        {
            auto stats = GetTableStats(runtime, shard1, tableId1.PathId.LocalPathId);

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

    Y_UNIT_TEST(BlobsStatsCorrect) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .AddStoragePool("ssd")
            .AddStoragePool("hdd")
            .AddStoragePool("ext");

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Shards(1)
            .Columns({
                {"key", "Uint32", true, false}, 
                {"value", "String", false, false}, 
                {"value2", "String", false, false, "hdd"}})
            .Families({
                {.Name = "default", .LogPoolKind = "ssd", .SysLogPoolKind = "ssd", .DataPoolKind = "ssd", 
                    .ExternalPoolKind = "ext", .DataThreshold = 100u, .ExternalThreshold = 200u}, 
                {.Name = "hdd", .DataPoolKind = "hdd"}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        TString smallValue(150, 'S');
        TString largeValue(1500, 'L');
        ExecSQL(server, sender, (TString)"UPSERT INTO `/Root/table-1` (key, value, value2) VALUES " + 
            "(1, \"AAA\", \"AAA\"), " + 
            "(2, \"" + smallValue + "\", \"BBB\"), " + 
            "(3, \"CCC\", \"" + smallValue + "\"), " + 
            "(4, \"" + largeValue + "\", \"BBB\"), " + 
            "(5, \"CCC\", \"" + largeValue + "\")");

        {
            Cerr << "... waiting for stats after upsert" << Endl;
            auto stats = WaitTableStats(runtime, shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 4232u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 0u);
        }

        CompactTable(runtime, shard1, tableId1, false);

        {
            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), 5u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetDataSize(), 3555u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetIndexSize(), 82u);

            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetChannel(), 1u); // ssd
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetDataSize(), 440u); // two small values
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[0].GetIndexSize(), 82u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[2].GetChannel(), 3u); // hdd
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[2].GetDataSize(), 99u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[2].GetIndexSize(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetChannel(), 2u); // ext
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetDataSize(), 3016u); // two large values
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetChannels()[1].GetIndexSize(), 0u);
        }
    }

    Y_UNIT_TEST(SharedCacheGarbage) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Shards(1)
            .Columns({
                {"key", "Uint32", true, false}, 
                {"value", "String", true, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        const int batches = 10;
        const int batchItems = 10;
        for (auto batch : xrange(batches)) {
            TString query = "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
            for (auto item = 0; item < batchItems; item++) {
                if (item != 0)
                    query += ", ";
                query += "(0, \"" + TString(7000, 'x') + ToString(batch * batchItems + item) + "\") ";
            }
            Cerr << query << Endl << Endl;
            ExecSQL(server, sender, query);
            CompactTable(runtime, shard1, tableId1, false);

            Cerr << "... waiting for stats after compaction" << Endl;
            auto stats = WaitTableStats(runtime, shard1, 1, (batch + 1) * batchItems);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetDatashardId(), shard1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetRowCount(), (batch + 1) * batchItems);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetTableStats().GetPartCount(), 1);
        }

        // each batch ~70KB, ~700KB in total
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(runtime.GetDynamicCounters());
        Cerr << "ActiveBytes = " << counters->ActiveBytes->Val() << " PassiveBytes = " << counters->PassiveBytes->Val() << Endl;
        UNIT_ASSERT_LE(counters->ActiveBytes->Val(), 800*1024); // one index
    }

} // Y_UNIT_TEST_SUITE(DataShardStats)

} // namespace NKikimr
