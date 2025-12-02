#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <chrono>
#include <thread>

using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::Tests;

Y_UNIT_TEST_SUITE(DataShardTruncate) {

    Y_UNIT_TEST(SimpleTruncateTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        
        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto edgeSender = runtime.AllocateEdgeActor();
        
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_TRACE);
        InitRoot(server, edgeSender);

        auto [shards, tableId] = CreateShardedTable(server, edgeSender, "/Root", "test_table", 1);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, edgeSender, "UPSERT INTO `/Root/test_table` (key, value) VALUES (1, 100), (2, 200), (3, 300);");

        auto beforeResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(beforeResult, 
            "key = 1, value = 100\n"
            "key = 2, value = 200\n" 
            "key = 3, value = 300\n");


        ui64 txId = AsyncTruncateTable(server, edgeSender, "/Root", "test_table");
        WaitTxNotification(server, edgeSender, txId);

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");
    }

    Y_UNIT_TEST(TruncateTableWithSecondaryIndex) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto edgeSender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_TRACE);
        InitRoot(server, edgeSender);

        auto opts = TShardedTableOptions()
            .EnableOutOfOrder(false)
            .Columns({
                {"key", "Uint32", true, false},
                {"skey", "Uint32", false, false},
                {"value", "Uint32", false, false}
            })
            .Indexes({
                {"by_skey", {"skey"}}
            });

        TString indexPath = "/Root/test_table/by_skey/indexImplTable";
        auto [shards, tableId] = CreateShardedTable(server, edgeSender, "/Root", "test_table", opts);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);


        ExecSQL(server, edgeSender, R"(
            UPSERT INTO `/Root/test_table` (key, skey, value) VALUES
                (1, 10, 100),
                (2, 20, 200),
                (3, 30, 300);
        )");

        auto beforeResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(beforeResult,
            "key = 1, skey = 10, value = 100\n"
            "key = 2, skey = 20, value = 200\n"
            "key = 3, skey = 30, value = 300\n");

        auto beforeIndexResult = ReadShardedTable(server, indexPath);
        UNIT_ASSERT_VALUES_EQUAL(beforeIndexResult,
            "skey = 10, key = 1\n"
            "skey = 20, key = 2\n"
            "skey = 30, key = 3\n");

        ui64 txId = AsyncTruncateTable(server, edgeSender, "/Root", "test_table");
        WaitTxNotification(server, edgeSender, txId);

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");

        auto afterIndexResult = ReadShardedTable(server, indexPath);
        UNIT_ASSERT_VALUES_EQUAL(afterIndexResult, "");
    }

    Y_UNIT_TEST(TruncateTableWithVectorIndex) {
    }

    Y_UNIT_TEST(TruncateTableWithManySecondaryIndexes) {
    }

    Y_UNIT_TEST(TruncateTableWithManySecondaryAndVectorIndexes) {
    }
}
