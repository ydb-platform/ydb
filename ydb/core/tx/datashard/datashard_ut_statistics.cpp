#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/util/count_min_sketch.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDataShard {

using namespace Tests;

namespace {
    TString FillTableQuery() {
        TStringBuilder sql;
        sql << "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
        for (size_t i = 0; i < 10; ++i) {
            sql << " (" << i << ", " << i * 2 << "),";
        }
        sql << " (10, 20);";
        return sql;
    }
}

Y_UNIT_TEST_SUITE(StatisticsScan) {

    Y_UNIT_TEST(RunScanOnShard) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, FillTableQuery());

        auto navigate = Navigate(runtime, sender, "/Root/table-1",
            NSchemeCache::TSchemeCacheNavigate::EOp::OpPath);
        auto pathId = navigate->ResultSet[0].TableId.PathId;

        ui64 shardId = shards.at(0);
        auto request = std::make_unique<TEvDataShard::TEvStatisticsScanRequest>();
        PathIdFromPathId(pathId, request->Record.MutableTablePathId());
        runtime.SendToPipe(shardId, sender, request.release());

        auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvStatisticsScanResponse>(sender);
        auto& record = response->Get()->Record;
        UNIT_ASSERT(record.GetStatus() == NKikimrTxDataShard::TEvStatisticsScanResponse::SUCCESS);
        UNIT_ASSERT(record.ColumnsSize() == 2);

        for (ui32 i = 0; i < 2; ++i) {
            auto& column = record.GetColumns(i);
            UNIT_ASSERT(column.GetTag() == i + 1);

            UNIT_ASSERT(column.StatisticsSize() == 1);
            auto& stat = column.GetStatistics(0);
            UNIT_ASSERT(stat.GetType() == 2);

            auto* bytes = stat.GetBytes().Data();
            auto* sketch = reinterpret_cast<const TCountMinSketch*>(bytes);

            for (ui32 j = 0; j <= 10; ++j) {
                ui32 value = (i + 1) * j;
                auto probe = sketch->Probe(reinterpret_cast<const char*>(&value), sizeof(value));
                UNIT_ASSERT(probe == 1);
            }
        }
    }
}

} // NKikimr::NDataShard
