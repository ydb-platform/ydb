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

        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        Cerr << "===================================================================================== BEGIN TRUNCATE" << Endl;
        Cerr << "TableId = " << tableId << Endl;

        ui64 txId = AsyncTruncateTable(server, edgeSender, "/Root", "test_table");
        WaitTxNotification(server, edgeSender, txId);

        std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        Cerr << "===================================================================================== END TRUNCATE" << Endl;

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");
    }
}
