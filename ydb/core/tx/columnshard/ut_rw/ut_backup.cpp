#include "columnshard_ut_common.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/tx/columnshard/operations/write_data.h>


namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(Backup) {

    bool ProposeTx(TTestBasicRuntime& runtime, TActorId& sender, const TString& txBody, const ui64 txId) {
        auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
            NKikimrTxColumnShard::TX_KIND_BACKUP, sender, txId, txBody);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_BACKUP);
        return (res.GetStatus() == NKikimrTxColumnShard::PREPARED);
    }

    void PlanTx(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap, bool waitResult = true) {
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
            UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_BACKUP);
            UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::SUCCESS);
        }
    }

    Y_UNIT_TEST(ProposeBackup) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
                                                                    NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
                                                                    NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
                                                                    NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8) )
                                                                };
        PrepareTablet(runtime, tableId, schema, 2);
        const ui64 txId = 111;
        NOlap::TSnapshot backupSnapshot(1,1);

        TActorId sender = runtime.AllocateEdgeActor();

        NKikimrTxColumnShard::TBackupTxBody txBody;
        txBody.MutableBackupTask()->SetTableId(tableId);
        txBody.MutableBackupTask()->SetSnapshotStep(backupSnapshot.GetPlanStep());
        txBody.MutableBackupTask()->SetSnapshotTxId(backupSnapshot.GetTxId());
        ProposeTx(runtime, sender, txBody.SerializeAsString(), txId);
        PlanTx(runtime, sender, NOlap::TSnapshot(11, txId));
    }
}

} // namespace NKikimr
