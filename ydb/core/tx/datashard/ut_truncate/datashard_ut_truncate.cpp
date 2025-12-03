#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::Tests;
using namespace NKikimr::NDataShard::NKqpHelpers;

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
                {"value", "Uint32", false, false},
                {"vector_column", "String", false, false}
            })
            .Indexes({
                {"vector_index", {"vector_column"}}
            });

        TString indexPath = "/Root/test_table/vector_index/indexImplTable";
        auto [shards, tableId] = CreateShardedTable(server, edgeSender, "/Root", "test_table", opts);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, edgeSender, R"(
            UPSERT INTO `/Root/test_table` (key, value, vector_column) VALUES
                (1, 100, '[1.0,2.0,3.0]'),
                (2, 200, '[4.0,5.0,6.0]'),
                (3, 300, '[7.0,8.0,9.0]');
        )");

        auto beforeResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(beforeResult,
            "key = 1, value = 100, vector_column = [1.0,2.0,3.0]\n"
            "key = 2, value = 200, vector_column = [4.0,5.0,6.0]\n"
            "key = 3, value = 300, vector_column = [7.0,8.0,9.0]\n");

        auto beforeIndexResult = ReadShardedTable(server, indexPath);
        UNIT_ASSERT_C(!beforeIndexResult.empty(), "Vector index should contain data before TRUNCATE");

        ui64 txId = AsyncTruncateTable(server, edgeSender, "/Root", "test_table");
        WaitTxNotification(server, edgeSender, txId);

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");

        auto afterIndexResult = ReadShardedTable(server, indexPath);
        UNIT_ASSERT_VALUES_EQUAL(afterIndexResult, "");
    }

    Y_UNIT_TEST(TruncateTableWithManySecondaryIndexes) {
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
                {"value", "Uint32", false, false},
                {"name", "String", false, false},
                {"category", "String", false, false}
            })
            .Indexes({
                {"by_value", {"value"}},
                {"by_name", {"name"}},
                {"by_category", {"category"}}
            });

        auto [shards, tableId] = CreateShardedTable(server, edgeSender, "/Root", "test_table", opts);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, edgeSender, R"(
            UPSERT INTO `/Root/test_table` (key, value, name, category) VALUES
                (1, 100, 'item1', 'cat1'),
                (2, 200, 'item2', 'cat2'),
                (3, 300, 'item3', 'cat1');
        )");

        auto beforeResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(beforeResult,
            "key = 1, value = 100, name = item1, category = cat1\n"
            "key = 2, value = 200, name = item2, category = cat2\n"
            "key = 3, value = 300, name = item3, category = cat1\n");

        auto beforeIndexResult1 = ReadShardedTable(server, "/Root/test_table/by_value/indexImplTable");
        auto beforeIndexResult2 = ReadShardedTable(server, "/Root/test_table/by_name/indexImplTable");
        auto beforeIndexResult3 = ReadShardedTable(server, "/Root/test_table/by_category/indexImplTable");
        UNIT_ASSERT_C(!beforeIndexResult1.empty(), "Index by_value should contain data before TRUNCATE");
        UNIT_ASSERT_C(!beforeIndexResult2.empty(), "Index by_name should contain data before TRUNCATE");
        UNIT_ASSERT_C(!beforeIndexResult3.empty(), "Index by_category should contain data before TRUNCATE");

        ui64 txId = AsyncTruncateTable(server, edgeSender, "/Root", "test_table");
        WaitTxNotification(server, edgeSender, txId);

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");

        auto afterIndexResult1 = ReadShardedTable(server, "/Root/test_table/by_value/indexImplTable");
        auto afterIndexResult2 = ReadShardedTable(server, "/Root/test_table/by_name/indexImplTable");
        auto afterIndexResult3 = ReadShardedTable(server, "/Root/test_table/by_category/indexImplTable");
        UNIT_ASSERT_VALUES_EQUAL(afterIndexResult1, "");
        UNIT_ASSERT_VALUES_EQUAL(afterIndexResult2, "");
        UNIT_ASSERT_VALUES_EQUAL(afterIndexResult3, "");
    }

    Y_UNIT_TEST(TruncateTableWithManySecondaryAndVectorIndexes) {
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
                {"value", "Uint32", false, false},
                {"name", "String", false, false},
                {"vector_column1", "String", false, false},
                {"vector_column2", "String", false, false}
            })
            .Indexes({
                {"by_value", {"value"}},
                {"by_name", {"name"}},
                {"vector_index1", {"vector_column1"}},
                {"vector_index2", {"vector_column2"}}
            });

        auto [shards, tableId] = CreateShardedTable(server, edgeSender, "/Root", "test_table", opts);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, edgeSender, R"(
            UPSERT INTO `/Root/test_table` (key, value, name, vector_column1, vector_column2)
            VALUES
                (1, 100, 'item1', '[1.0,2.0,3.0]', '[10.0,11.0,12.0]'),
                (2, 200, 'item2', '[4.0,5.0,6.0]', '[13.0,14.0,15.0]'),
                (3, 300, 'item3', '[7.0,8.0,9.0]', '[16.0,17.0,18.0]');
        )");

        auto beforeResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_C(!beforeResult.empty(), "Main table should contain data before TRUNCATE");

        auto beforeSecondaryIndex1 = ReadShardedTable(server, "/Root/test_table/by_value/indexImplTable");
        auto beforeSecondaryIndex2 = ReadShardedTable(server, "/Root/test_table/by_name/indexImplTable");
        auto beforeVectorIndex1 = ReadShardedTable(server, "/Root/test_table/vector_index1/indexImplTable");
        auto beforeVectorIndex2 = ReadShardedTable(server, "/Root/test_table/vector_index2/indexImplTable");
        
        UNIT_ASSERT_C(!beforeSecondaryIndex1.empty(), "Secondary index by_value should contain data before TRUNCATE");
        UNIT_ASSERT_C(!beforeSecondaryIndex2.empty(), "Secondary index by_name should contain data before TRUNCATE");
        UNIT_ASSERT_C(!beforeVectorIndex1.empty(), "Vector index1 should contain data before TRUNCATE");
        UNIT_ASSERT_C(!beforeVectorIndex2.empty(), "Vector index2 should contain data before TRUNCATE");

        ui64 txId = AsyncTruncateTable(server, edgeSender, "/Root", "test_table");
        WaitTxNotification(server, edgeSender, txId);

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");

        auto afterSecondaryIndex1 = ReadShardedTable(server, "/Root/test_table/by_value/indexImplTable");
        auto afterSecondaryIndex2 = ReadShardedTable(server, "/Root/test_table/by_name/indexImplTable");
        auto afterVectorIndex1 = ReadShardedTable(server, "/Root/test_table/vector_index1/indexImplTable");
        auto afterVectorIndex2 = ReadShardedTable(server, "/Root/test_table/vector_index2/indexImplTable");
        
        UNIT_ASSERT_VALUES_EQUAL(afterSecondaryIndex1, "");
        UNIT_ASSERT_VALUES_EQUAL(afterSecondaryIndex2, "");
        UNIT_ASSERT_VALUES_EQUAL(afterVectorIndex1, "");
        UNIT_ASSERT_VALUES_EQUAL(afterVectorIndex2, "");
    }

    Y_UNIT_TEST(TruncateTableDuringSelect) {
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
                {"value", "Uint32", false, false},
                {"value2", "Uint32", false, false}
            });

        auto [shards, tableId] = CreateShardedTable(server, edgeSender, "/Root", "test_table", opts);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, edgeSender, R"(
            UPSERT INTO `/Root/test_table` (key, value) VALUES (1, 100);
        )");

        TString sessionId, txId;

        KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            UPDATE `/Root/test_table` ON (key, value2) VALUES (1, 200);
        )"));

        auto selectResult = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT key, value FROM `/Root/test_table`;
        )"));
        UNIT_ASSERT_VALUES_EQUAL(selectResult, "{ items { uint32_value: 1 } items { uint32_value: 100 } }");

        ui64 truncateTxId = AsyncTruncateTable(server, edgeSender, "/Root", "test_table");
        WaitTxNotification(server, edgeSender, truncateTxId);

        auto commitResult = KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(SELECT 1;)"));
        UNIT_ASSERT_VALUES_EQUAL(commitResult, "ERROR: ABORTED");

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");
    }
}
