#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <library/cpp/testing/hook/hook.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(Backup) {

    [[nodiscard]] TPlanStep ProposeTx(TTestBasicRuntime& runtime, TActorId& sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody, const ui64 txId) {
        auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
            txKind, sender, txId, txBody);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
        UNIT_ASSERT_EQUAL(res.GetStatus(),  NKikimrTxColumnShard::PREPARED);
        return TPlanStep{res.GetMinStep()};
    }

    void PlanTx(TTestBasicRuntime& runtime, TActorId& sender, NKikimrTxColumnShard::ETransactionKind txKind, NOlap::TSnapshot snap, bool waitResult = true) {
        auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(snap.GetPlanStep(), 0, TTestTxConfig::TxTablet0);
        auto tx = plan->Record.AddTransactions();
        tx->SetTxId(snap.GetTxId());
        ActorIdToProto(sender, tx->MutableAckTo());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());

        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
        if (waitResult) {
            auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
            const auto& res = ev->Get()->Record;
            UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
            UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
            UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::SUCCESS);
        }
    }

    template <class TChecker>
    void TestWaitCondition(TTestBasicRuntime& runtime, const TString& title, const TChecker& checker, const TDuration d = TDuration::Seconds(10)) {
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
        const std::vector<NArrow::NTest::TTestColumn> schema = {
                                                                    NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
                                                                    NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
                                                                    NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8) )
                                                                };
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;

        TActorId sender = runtime.AllocateEdgeActor();

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({0, 100}, schema), schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        TestWaitCondition(runtime, "insert compacted",
            [&]() {
            ++writeId;
            std::vector<ui64> writeIds;
            WriteData(runtime, sender, writeId, tableId, MakeTestBlob({writeId * 100, (writeId + 1) * 100}, schema), schema, true, &writeIds);
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
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false);
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
    }
}

} // namespace NKikimr
