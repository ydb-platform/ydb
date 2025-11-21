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
        auto sender = runtime.AllocateEdgeActor();
        
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        InitRoot(server, sender);
        
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "test_table", 1);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        ui64 shardId = shards[0];

        ExecSQL(server, sender, "UPSERT INTO `/Root/test_table` (key, value) VALUES (1, 100), (2, 200), (3, 300);");
        
        auto beforeResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(beforeResult, 
            "key = 1, value = 100\n"
            "key = 2, value = 200\n" 
            "key = 3, value = 300\n");

        ui64 txId = 12345678;
        ui64 seqNoGeneration = 87654321;

        NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
        schemeTx.MutableSeqNo()->SetGeneration(seqNoGeneration);
        schemeTx.MutableSeqNo()->SetRound(1);

        auto& truncateOp = *schemeTx.MutableTruncateTable();

        truncateOp.MutablePathId()->SetOwnerId(tableId.PathId.OwnerId);
        truncateOp.MutablePathId()->SetLocalId(tableId.PathId.LocalPathId);

        TString txBody = schemeTx.SerializeAsString();
        std::cerr << "========================================== BEGIN TRUNCATE TEST" << std::endl;

        // Phase 1: Propose Transaction
        auto proposal = MakeHolder<TEvDataShard::TEvProposeTransaction>(
            NKikimrTxDataShard::TX_KIND_SCHEME,
            tableId.PathId.OwnerId,
            sender,
            txId,
            txBody,
            NKikimrSubDomains::TProcessingParams());
        runtime.SendToPipe(shardId, sender, proposal.Release());

        auto proposeResult = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(proposeResult);
        UNIT_ASSERT_VALUES_EQUAL(proposeResult->Get()->GetTxKind(), NKikimrTxDataShard::TX_KIND_SCHEME);
        UNIT_ASSERT_VALUES_EQUAL(proposeResult->Get()->GetTxId(), txId);
        
        std::cerr << "Propose phase completed. Status: " << proposeResult->Get()->Record.GetStatus() << std::endl;
        
        if (proposeResult->Get()->IsPrepared()) {
            std::cerr << "Transaction prepared, proceeding to Plan phase..." << std::endl;
            
            // Phase 2: Plan Transaction (the actual execution)
            ui64 stepId = proposeResult->Get()->Record.GetMinStep();
            auto planStep = MakeHolder<TEvTxProcessing::TEvPlanStep>(stepId, 0, shardId);
            auto plannedTx = planStep->Record.MutableTransactions()->Add();
            plannedTx->SetTxId(txId);
            ActorIdToProto(sender, plannedTx->MutableAckTo());

            std::cerr << "Sending TEvPlanStep with stepId: " << stepId << ", txId: " << txId << std::endl;
            runtime.SendToPipe(shardId, sender, planStep.Release());

            // Wait for final execution result
            // auto finalResult = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(sender);
            // UNIT_ASSERT_VALUES_EQUAL(finalResult->Get()->GetTxKind(), NKikimrTxDataShard::TX_KIND_SCHEME);
            // UNIT_ASSERT_VALUES_EQUAL(finalResult->Get()->GetTxId(), txId);
            // UNIT_ASSERT(finalResult->Get()->IsComplete());
            // std::cerr << "TRUNCATE execution completed!" << std::endl;
        } else if (proposeResult->Get()->IsComplete()) {
            std::cerr << "Transaction completed immediately (no planning needed)" << std::endl;
        } else {
            TString textFormat;
            google::protobuf::TextFormat::PrintToString(proposeResult->Get()->Record, &textFormat);
            std::cerr << "Unexpected propose result:\n" << textFormat << std::endl;
            UNIT_ASSERT_C(false, "Unexpected propose result status");
        }

        std::cerr << "========================================== END TRUNCATE TEST" << std::endl;

        auto afterResult = ReadTable(server, shards, tableId);
        UNIT_ASSERT_VALUES_EQUAL(afterResult, "");
    }
}
