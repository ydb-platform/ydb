#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>

#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/wrappers/fake_storage.h>


namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(Backup) {

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
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        TColumnShardTestSetup testSetup;

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
                                                                    NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
                                                                    NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
                                                                    NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8) )
                                                                };
        testSetup.CreateTable(tableId, {schema, {schema[0], schema[1]}});
        ui64 txId = 111;
        ui64 planStep = 1000000000; // greater then delays

        ui64 writeId = 1;

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(testSetup.WriteData(writeId++, tableId, MakeTestBlob({0, 100}, schema), schema, true, &writeIds));
            testSetup.ProposeCommit(++txId, writeIds);
            testSetup.PlanCommit(++planStep, {txId});
        }

        const ui32 start = csControllerGuard->GetInsertStartedCounter().Val();
        TestWaitCondition(testSetup.GetRuntime(), "insert compacted",
            [&]() {
            ++writeId;
            std::vector<ui64> writeIds;
            testSetup.WriteData(writeId, tableId, MakeTestBlob({writeId * 100, (writeId + 1) * 100}, schema), schema, true, &writeIds);
            testSetup.ProposeCommit(++txId, writeIds);
            testSetup.PlanCommit(++planStep, {txId});
            return csControllerGuard->GetInsertStartedCounter().Val() > start + 1;
        }, TDuration::Seconds(1000));

        NKikimrTxColumnShard::TBackupTxBody txBody;
        NOlap::TSnapshot backupSnapshot(planStep, txId);
        txBody.MutableBackupTask()->SetTableName("abcde");
        txBody.MutableBackupTask()->SetTableId(tableId);
        txBody.MutableBackupTask()->SetSnapshotStep(backupSnapshot.GetPlanStep());
        txBody.MutableBackupTask()->SetSnapshotTxId(backupSnapshot.GetTxId());
        txBody.MutableBackupTask()->MutableS3Settings()->SetEndpoint("fake");
        txBody.MutableBackupTask()->MutableS3Settings()->SetSecretKey("fakeSecret");
        AFL_VERIFY(csControllerGuard->GetFinishedExportsCount() == 0);
        UNIT_ASSERT_EQUAL(testSetup.ProposeTxWaitProposeResult(NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId),  NKikimrTxColumnShard::PREPARED);
        AFL_VERIFY(csControllerGuard->GetFinishedExportsCount() == 1);
        testSetup.PlanTxWaitAck(txId, ++planStep);
        TestWaitCondition(testSetup.GetRuntime(), "export",
            []() {return Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize(); });
    }
}

} // namespace NKikimr
