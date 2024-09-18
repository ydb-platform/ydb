#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/library/minsketch/count_min_sketch.h>

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

        ui64 shardId = shards.at(0);
        auto request = std::make_unique<NStat::TEvStatistics::TEvStatisticsRequest>();
        auto* reqTableId = request->Record.MutableTable()->MutablePathId();
        reqTableId->SetOwnerId(tableId.PathId.OwnerId);
        reqTableId->SetLocalId(tableId.PathId.LocalPathId);
        runtime.SendToPipe(shardId, sender, request.release());

        auto response = runtime.GrabEdgeEventRethrow<NStat::TEvStatistics::TEvStatisticsResponse>(sender);
        auto& record = response->Get()->Record;
        UNIT_ASSERT(record.GetStatus() == NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);
        UNIT_ASSERT(record.ColumnsSize() == 2);

        for (ui32 i = 0; i < 2; ++i) {
            auto& column = record.GetColumns(i);
            UNIT_ASSERT(column.GetTag() == i + 1);

            UNIT_ASSERT(column.StatisticsSize() == 1);
            auto& stat = column.GetStatistics(0);
            UNIT_ASSERT(stat.GetType() == 2);

            auto* data = stat.GetData().Data();
            auto* sketch = reinterpret_cast<const TCountMinSketch*>(data);

            for (ui32 j = 0; j <= 10; ++j) {
                ui32 value = (i + 1) * j;
                auto probe = sketch->Probe(reinterpret_cast<const char*>(&value), sizeof(value));
                UNIT_ASSERT(probe == 1);
            }
        }
    }
}

} // NKikimr::NDataShard
