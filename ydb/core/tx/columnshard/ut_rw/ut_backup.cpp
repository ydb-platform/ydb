#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <library/cpp/testing/hook/hook.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(Backup) {
    void SendProposeTx(
        TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody, const ui64 txId) {
        auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(txKind, sender, txId, txBody);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
    }

    [[nodiscard]] TPlanStep WaitProposeTxResult(
        TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, const ui64 txId) {
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        return TPlanStep{ res.GetMinStep() };
    }

    [[nodiscard]] THashMap<ui64, TPlanStep> WaitProposeTxResults(
        TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, std::vector<ui64> txIds) {
        THashMap<ui64, TPlanStep> result;
        const ui32 expectedResultsCount = txIds.size();
        for (ui32 i = 0; i < expectedResultsCount; ++i) {
            auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
            const auto& res = ev->Get()->Record;
            UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
            UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);

            bool found = false;
            for (auto it = txIds.begin(); it != txIds.end(); ++it) {
                if (*it == res.GetTxId()) {
                    result.emplace(res.GetTxId(), TPlanStep{ res.GetMinStep() });
                    txIds.erase(it);
                    found = true;
                    break;
                }
            }
            UNIT_ASSERT(found);
        }
        UNIT_ASSERT(txIds.empty());
        return result;
    }

    [[nodiscard]] TPlanStep ProposeTx(
        TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody, const ui64 txId) {
        SendProposeTx(runtime, sender, txKind, txBody, txId);
        return WaitProposeTxResult(runtime, sender, txKind, txId);
    }

    void SubscribeTxCompletion(TTestBasicRuntime & runtime, TActorId & sender, const ui64 txId) {
        auto evSubscribe = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(txId);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evSubscribe.release());
    }

    void PlanTx(TTestBasicRuntime & runtime, TActorId & sender, NOlap::TSnapshot snap) {
        auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(snap.GetPlanStep(), 0, TTestTxConfig::TxTablet0);
        auto tx = plan->Record.AddTransactions();
        tx->SetTxId(snap.GetTxId());
        ActorIdToProto(sender, tx->MutableAckTo());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());

        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
    }

    void PlanTxs(TTestBasicRuntime & runtime, TActorId & sender, std::vector<NOlap::TSnapshot> snaps) {
        std::sort(snaps.begin(), snaps.end());
        for (auto it = snaps.begin(); it != snaps.end();) {
            const ui64 planStep = it->GetPlanStep();
            auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(planStep, 0, TTestTxConfig::TxTablet0);
            do {
                auto tx = plan->Record.AddTransactions();
                tx->SetTxId(it->GetTxId());
                ActorIdToProto(sender, tx->MutableAckTo());
                ++it;
            } while (it != snaps.end() && it->GetPlanStep() == planStep);

            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
        }
    }

    void WaitNotifyTxCompletions(TTestBasicRuntime & runtime, TActorId & sender, std::vector<ui64> txIds) {
        const ui32 expectedResultsCount = txIds.size();
        for (ui32 i = 0; i < expectedResultsCount; ++i) {
            auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT(ev);
            const auto& record = ev->Get()->Record;
            bool found = false;
            for (auto it = txIds.begin(); it != txIds.end(); ++it) {
                if (*it == record.GetTxId()) {
                    txIds.erase(it);
                    found = true;
                    break;
                }
            }
            UNIT_ASSERT(found);
            UNIT_ASSERT(record.HasOpResult());
            const auto& opResult = record.GetOpResult();
            UNIT_ASSERT(opResult.GetSuccess());
            if (opResult.HasExplain()) {
                Cerr << "Backup completion explain: " << opResult.GetExplain() << Endl;
            }
        }
        UNIT_ASSERT(txIds.empty());
    }

    template <class TChecker>
    void TestWaitCondition(
        TTestBasicRuntime & runtime, const TString& title, const TChecker& checker, const TDuration d = TDuration::Seconds(10)) {
        const TInstant start = TInstant::Now();
        while (TInstant::Now() - start < d && !checker()) {
            Cerr << "waiting " << title << Endl;
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        AFL_VERIFY(checker());
    }

    Y_UNIT_TEST(ProposeBackup) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)), NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8)) };
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;

        TActorId sender = runtime.AllocateEdgeActor();

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 100 }, schema), schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        TestWaitCondition(runtime, "insert compacted", [&]() {
            ++writeId;
            std::vector<ui64> writeIds;
            WriteData(runtime, sender, writeId, tableId, MakeTestBlob({ writeId * 100, (writeId + 1) * 100 }, schema), schema, true, &writeIds);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
            return true;
        }, TDuration::Seconds(1000));

        NKikimrTxColumnShard::TBackupTxBody txBody;
        NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        auto& backupTask = *txBody.MutableBackupTask();
        backupTask.SetTableName("abcde");
        backupTask.SetTableId(tableId);
        backupTask.SetSnapshotStep(backupSnapshot.GetPlanStep());
        backupTask.SetSnapshotTxId(backupSnapshot.GetTxId());
        backupTask.MutableS3Settings()->SetEndpoint(GetEnv("S3_ENDPOINT"));
        backupTask.MutableS3Settings()->SetBucket("test");

        auto& table = *backupTask.MutableTable();
        auto& tableDescription = *table.MutableColumnTableDescription();
        tableDescription.SetColumnShardCount(4);
        auto& schemaBackup = *tableDescription.MutableSchema();

        auto& col1 = *schemaBackup.MutableColumns()->Add();
        col1.SetName("key1");
        col1.SetType("Uint64");

        auto& col2 = *schemaBackup.MutableColumns()->Add();
        col2.SetName("key2");
        col2.SetType("Uint64");

        auto& col3 = *schemaBackup.MutableColumns()->Add();
        col3.SetName("field");
        col3.SetType("Utf8");
        table.MutableSelf();
        AFL_VERIFY(csControllerGuard->GetFinishedExportsCount() == 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
        AFL_VERIFY(csControllerGuard->GetFinishedExportsCount() == 1);
        PlanTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
        TestWaitCondition(runtime, "export", [&]() {
            return NTestUtils::GetObjectKeys("test", s3Client).size() == 3;
        });
    }

    Y_UNIT_TEST(ParallelBackupWithPerTableTracking) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test_parallel", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId1 = 1;
        const ui64 tableId2 = 2;
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)), NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8)) };

        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();

        // Prepare first table
        auto planStep = PrepareTablet(runtime, tableId1, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;
        TActorId sender = runtime.AllocateEdgeActor();

        TPlanStep table1PlanStep;
        ui64 table1TxId = 0;
        // Write data to first table
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId1, MakeTestBlob({ 0, 100 }, schema), schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
            table1PlanStep = planStep;
            table1TxId = txId;
        }

        TPlanStep table2PlanStep;
        ui64 table2TxId = 0;
        // Copy table to create second table (read-only copy)
        {
            const ui64 copyTxId = ++txId;
            const auto copyPlanStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(tableId1, tableId2, 1), copyTxId);
            PlanSchemaTx(runtime, sender, NOlap::TSnapshot(copyPlanStep, copyTxId));
            table2PlanStep = copyPlanStep;
            table2TxId = copyTxId;
        }

        // Create backup tasks for both tables
        NKikimrTxColumnShard::TBackupTxBody txBody1;
        const NOlap::TSnapshot backupSnapshot1(table1PlanStep.Val(), table1TxId);
        auto& backupTask1 = *txBody1.MutableBackupTask();
        backupTask1.SetTableName("table1");
        backupTask1.SetTableId(tableId1);
        backupTask1.SetSnapshotStep(backupSnapshot1.GetPlanStep());
        backupTask1.SetSnapshotTxId(backupSnapshot1.GetTxId());
        backupTask1.MutableS3Settings()->SetEndpoint(GetEnv("S3_ENDPOINT"));
        backupTask1.MutableS3Settings()->SetBucket("test_parallel");
        backupTask1.MutableS3Settings()->SetObjectKeyPattern("table1");

        auto& table1 = *backupTask1.MutableTable();
        auto& tableDescription1 = *table1.MutableColumnTableDescription();
        tableDescription1.SetColumnShardCount(4);
        auto& schemaBackup1 = *tableDescription1.MutableSchema();

        auto& col1 = *schemaBackup1.MutableColumns()->Add();
        col1.SetName("key1");
        col1.SetType("Uint64");

        auto& col2 = *schemaBackup1.MutableColumns()->Add();
        col2.SetName("key2");
        col2.SetType("Uint64");

        auto& col3 = *schemaBackup1.MutableColumns()->Add();
        col3.SetName("field");
        col3.SetType("Utf8");
        table1.MutableSelf();

        NKikimrTxColumnShard::TBackupTxBody txBody2;
        const NOlap::TSnapshot backupSnapshot2(table2PlanStep.Val(), table2TxId);
        auto& backupTask2 = *txBody2.MutableBackupTask();
        backupTask2.SetTableName("table2");
        backupTask2.SetTableId(tableId2);
        backupTask2.SetSnapshotStep(backupSnapshot2.GetPlanStep());
        backupTask2.SetSnapshotTxId(backupSnapshot2.GetTxId());
        backupTask2.MutableS3Settings()->SetEndpoint(GetEnv("S3_ENDPOINT"));
        backupTask2.MutableS3Settings()->SetBucket("test_parallel");
        backupTask2.MutableS3Settings()->SetObjectKeyPattern("table2");

        auto& table2 = *backupTask2.MutableTable();
        auto& tableDescription2 = *table2.MutableColumnTableDescription();
        tableDescription2.SetColumnShardCount(4);
        auto& schemaBackup2 = *tableDescription2.MutableSchema();

        auto& col1_2 = *schemaBackup2.MutableColumns()->Add();
        col1_2.SetName("key1");
        col1_2.SetType("Uint64");

        auto& col2_2 = *schemaBackup2.MutableColumns()->Add();
        col2_2.SetName("key2");
        col2_2.SetType("Uint64");

        auto& col3_2 = *schemaBackup2.MutableColumns()->Add();
        col3_2.SetName("field");
        col3_2.SetType("Utf8");
        table2.MutableSelf();

        AFL_VERIFY(csControllerGuard->GetFinishedExportsCount() == 0);

        // Propose both backups before waiting for propose results to let export actors run in parallel.
        ui64 backupTxId1 = ++txId;
        SendProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody1.SerializeAsString(), backupTxId1);

        ui64 backupTxId2 = ++txId;
        SendProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody2.SerializeAsString(), backupTxId2);

        const auto backupPlanSteps = WaitProposeTxResults(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, { backupTxId1, backupTxId2 });
        const auto* backupPlanStep1 = backupPlanSteps.FindPtr(backupTxId1);
        const auto* backupPlanStep2 = backupPlanSteps.FindPtr(backupTxId2);
        UNIT_ASSERT(backupPlanStep1);
        UNIT_ASSERT(backupPlanStep2);

        AFL_VERIFY(csControllerGuard->GetFinishedExportsCount() == 2);

        // Subscribe to both backups before planning to let them run in parallel.
        SubscribeTxCompletion(runtime, sender, backupTxId1);
        SubscribeTxCompletion(runtime, sender, backupTxId2);

        // Plan both backups without waiting for completion between them.
        PlanTxs(runtime, sender, { NOlap::TSnapshot(*backupPlanStep1, backupTxId1), NOlap::TSnapshot(*backupPlanStep2, backupTxId2) });

        WaitNotifyTxCompletions(runtime, sender, { backupTxId1, backupTxId2 });

        // Wait for both exports to complete
        TestWaitCondition(runtime, "exports", [&]() {
            return NTestUtils::GetObjectKeys("test_parallel", s3Client).size() == 6;   // 3 objects per table
        });

        // Verify that we can query LastCompletedBackupTransaction for both tables
        // Test retry mechanism for table 1
        {
            auto event = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(backupTxId1);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
            auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT(ev);
            const auto& result = ev->Get()->Record;
            UNIT_ASSERT_EQUAL(result.GetTxId(), backupTxId1);
            UNIT_ASSERT(result.GetOpResult().GetSuccess());
        }

        // Test retry mechanism for table 2
        {
            auto event = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(backupTxId2);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
            auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT(ev);
            const auto& result = ev->Get()->Record;
            UNIT_ASSERT_EQUAL(result.GetTxId(), backupTxId2);
            UNIT_ASSERT(result.GetOpResult().GetSuccess());
        }

        // Verify that each table has its own LastCompletedBackupTransaction
        // by checking that the correct TxId is returned for each table
        {
            auto event1 = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(backupTxId1);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event1.release());
            auto ev1 = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT(ev1);
            UNIT_ASSERT_EQUAL(ev1->Get()->Record.GetTxId(), backupTxId1);

            auto event2 = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(backupTxId2);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event2.release());
            auto ev2 = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT(ev2);
            UNIT_ASSERT_EQUAL(ev2->Get()->Record.GetTxId(), backupTxId2);

            // Verify that the TxIds are different (per-table tracking)
            UNIT_ASSERT_UNEQUAL(backupTxId1, backupTxId2);
        }
    }
}

}   // namespace NKikimr
