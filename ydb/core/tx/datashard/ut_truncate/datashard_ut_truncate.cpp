#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <chrono>
#include <thread>

using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::Tests;

Y_UNIT_TEST_SUITE(DataShardTruncate) {

    Y_UNIT_TEST(SimpleTruncateTable) {
        // TODO: flown4qqqq

        // TPortManager pm;
        // TServerSettings serverSettings(pm.GetPort(2134));
        // serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        
        // Tests::TServer::TPtr server = new TServer(serverSettings);
        // auto &runtime = *server->GetRuntime();
        // auto edgeSender = runtime.AllocateEdgeActor();
        
        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        // InitRoot(server, edgeSender);
        
        // // Phase 0: Init table
        // auto [shards, tableId] = CreateShardedTable(server, edgeSender, "/Root", "test_table", 1);
        // UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        // ui64 shardId = shards[0];

        // ExecSQL(server, edgeSender, "UPSERT INTO `/Root/test_table` (key, value) VALUES (1, 100), (2, 200), (3, 300);");
        
        // auto beforeResult = ReadTable(server, shards, tableId);
        // UNIT_ASSERT_VALUES_EQUAL(beforeResult, 
        //     "key = 1, value = 100\n"
        //     "key = 2, value = 200\n" 
        //     "key = 3, value = 300\n");

        // ui64 txId = 12345678;
        // ui64 seqNoGeneration = 87654321;

        // NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
        // schemeTx.MutableSeqNo()->SetGeneration(seqNoGeneration);
        // schemeTx.MutableSeqNo()->SetRound(1);

        // auto& truncateOp = *schemeTx.MutableTruncateTable();

        // truncateOp.MutablePathId()->SetOwnerId(tableId.PathId.OwnerId);
        // truncateOp.MutablePathId()->SetLocalId(tableId.PathId.LocalPathId);

        // TString txBody = schemeTx.SerializeAsString();
        // std::cerr << "========================================== BEGIN TRUNCATE TEST" << std::endl;


        // {   // First Send
        //     auto proposal = MakeHolder<TEvDataShard::TEvProposeTransaction>(
        //         NKikimrTxDataShard::TX_KIND_SCHEME,
        //         tableId.PathId.OwnerId,
        //         edgeSender,
        //         txId,
        //         txBody,
        //         NKikimrSubDomains::TProcessingParams());

        //     runtime.Send(new IEventHandle(edgeSender, TActorId(), proposal), 0, true);
        // }
        // // Phase 3: Check table is empty
        // auto afterResult = ReadTable(server, shards, tableId);
        // UNIT_ASSERT_VALUES_EQUAL(afterResult, "");
    }
}
