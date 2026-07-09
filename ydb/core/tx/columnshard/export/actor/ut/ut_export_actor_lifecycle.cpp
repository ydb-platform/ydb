#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/local.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/export/session/session.h>
#include <ydb/core/tx/columnshard/export/session/task.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_processing.h>

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <library/cpp/testing/hook/hook.h>
#include <util/random/random.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(ExportSessionStateMachine) {
    static std::shared_ptr<NOlap::NExport::TSession> MakeExportSession() {
        NKikimrSchemeOp::TBackupTask backupTask;
        backupTask.SetTableName("test_table");
        backupTask.SetTableId(1);
        backupTask.MutableS3Settings()->SetBucket("test");
        backupTask.MutableS3Settings()->SetEndpoint("localhost:9000");

        auto task =
            std::make_shared<NOlap::NExport::TExportTask>(NOlap::NExport::TIdentifier(NColumnShard::TSchemeShardLocalPathId::FromRawValue(1)),
                /*columns=*/std::vector<std::pair<TString, NScheme::TTypeInfo>>{}, backupTask, /*txId=*/std::optional<ui64>{ 42 });

        return std::make_shared<NOlap::NExport::TSession>(task);
    }

    Y_UNIT_TEST(FinishGuardedAfterAbort) {
        auto session = MakeExportSession();
        session->Confirm();
        UNIT_ASSERT(session->IsConfirmed());

        session->Abort("Aborted by user");
        UNIT_ASSERT(!session->IsStarted());
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());

        UNIT_ASSERT(!session->IsStarted());
    }

    Y_UNIT_TEST(DoubleAbortIsNoOp) {
        auto session = MakeExportSession();
        session->Confirm();
        session->Abort("first abort");
        UNIT_ASSERT(session->IsAborted());

        session->Abort("second abort");
        UNIT_ASSERT(session->IsAborted());
    }
}

namespace {

i64 GetExportActorsAliveCounter(TTestBasicRuntime& runtime) {
    const auto subgroup =
        GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
            "ExportActor");
    return subgroup->GetCounter("Value/Export/Actors/Alive", false)->Val();
}

template <class TChecker>
void TestWaitCondition(TTestBasicRuntime& runtime, const TString& title, const TChecker& checker, const TDuration d = TDuration::Seconds(10)) {
    const TInstant start = TInstant::Now();
    while (TInstant::Now() - start < d && !checker()) {
        Cerr << "waiting " << title << Endl;
        runtime.SimulateSleep(TDuration::Seconds(1));
    }
    UNIT_ASSERT(checker());
}

[[nodiscard]] TPlanStep ProposeTx(
    TTestBasicRuntime& runtime, TActorId& sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody, const ui64 txId) {
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(txKind, sender, txId, txBody);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
    UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
    return TPlanStep{ res.GetMinStep() };
}

[[nodiscard]] TPlanStep ProposeTxWithRebootDuringExport(
    TTestBasicRuntime& runtime, TActorId& sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody, const ui64 txId) {
    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvColumnShard::TEvProposeTransaction(txKind, sender, txId, txBody));

    runtime.SimulateSleep(TDuration::MicroSeconds(RandomNumber<ui32>(300)));
    RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

    runtime.SimulateSleep(TDuration::MilliSeconds(100));
    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvColumnShard::TEvProposeTransaction(txKind, sender, txId, txBody));

    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
    UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
    UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
    return TPlanStep{ res.GetMinStep() };
}

void PlanTx(TTestBasicRuntime& runtime, TActorId& sender, NKikimrTxColumnShard::ETransactionKind txKind, NOlap::TSnapshot snap,
    bool waitNotifyResult = false) {
    Y_UNUSED(txKind);
    if (waitNotifyResult) {
        auto evSubscribe = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(snap.GetTxId());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evSubscribe.release());
    }

    auto plan = std::make_unique<TEvTxProcessing::TEvPlanStep>(snap.GetPlanStep(), 0, TTestTxConfig::TxTablet0);
    auto tx = plan->Record.AddTransactions();
    tx->SetTxId(snap.GetTxId());
    ActorIdToProto(sender, tx->MutableAckTo());
    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, plan.release());

    UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(sender));
    if (waitNotifyResult) {
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
        UNIT_ASSERT_EQUAL(ev->Get()->Record.GetTxId(), snap.GetTxId());
        UNIT_ASSERT(ev->Get()->Record.HasOpResult());
        UNIT_ASSERT(ev->Get()->Record.GetOpResult().GetSuccess());
    }
}

NKikimrTxColumnShard::TBackupTxBody MakeBackupTxBody(const ui64 tableId, const NOlap::TSnapshot& backupSnapshot) {
    NKikimrTxColumnShard::TBackupTxBody txBody;
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
    return txBody;
}

void ExportFromTable(TTestBasicRuntime& runtime, TActorId& sender,
    NYDBTest::TControllers::TGuard<NOlap::TWaitCompactionController>& csControllerGuard, ui64& txId, TPlanStep& planStep, const ui64 tableId,
    const NOlap::TSnapshot& backupSnapshot) {
    Y_UNUSED(csControllerGuard);
    const auto txBody = MakeBackupTxBody(tableId, backupSnapshot);
    planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
    UNIT_ASSERT(GetExportActorsAliveCounter(runtime) >= 0);
    PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), true);
}

void RunBackupExport(TTestBasicRuntime& runtime, TActorId& sender,
    NYDBTest::TControllers::TGuard<NOlap::TWaitCompactionController>& csControllerGuard, ui64& txId, TPlanStep& planStep, const ui64 tableId,
    const std::vector<NArrow::NTest::TTestColumn>& schema) {
    std::vector<ui64> writeIds;
    ui64 writeId = 1;
    UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 5 }, schema), schema, true, &writeIds));
    planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);

    const NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
    UNIT_ASSERT_VALUES_EQUAL(csControllerGuard->GetFinishedExportsCount(), 0);
    UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
    ExportFromTable(runtime, sender, csControllerGuard, txId, planStep, tableId, backupSnapshot);
    UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() >= 1);
}

void CopyTableFromMain(
    TTestBasicRuntime& runtime, TActorId& sender, ui64& txId, TPlanStep& planStep, const ui64 srcPathId, const ui64 dstPathId) {
    planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
}

void DropCopyTable(TTestBasicRuntime& runtime, TActorId& sender, ui64& txId, TPlanStep& planStep, const ui64 copyPathId) {
    planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathId, 2), ++txId);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
}

void WriteAndCommitToMain(TTestBasicRuntime& runtime, TActorId& sender, ui64& txId, TPlanStep& planStep, const ui64 mainPathId,
    const std::pair<ui64, ui64>& rowRange, const std::vector<NArrow::NTest::TTestColumn>& schema, ui64& writeId) {
    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(runtime, sender, writeId++, mainPathId, MakeTestBlob(rowRange, schema), schema, true, &writeIds));
    planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);
}

}   // namespace

Y_UNIT_TEST_SUITE(TExportActorLifecycle) {
    Y_UNIT_TEST(AliveCounterReturnsToZeroAfterSuccessfulExport) {
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

        TActorId sender = runtime.AllocateEdgeActor();
        RunBackupExport(runtime, sender, csControllerGuard, txId, planStep, tableId, schema);

        TestWaitCondition(runtime, "export", [&]() {
            return NTestUtils::GetObjectKeys("test", s3Client).size() == 3;
        });

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
    }

    Y_UNIT_TEST(AliveCounterReturnsToZeroAfterExportCancel) {
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

        TActorId sender = runtime.AllocateEdgeActor();

        std::vector<ui64> writeIds;
        ui64 writeId = 1;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 5 }, schema), schema, true, &writeIds));
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        const NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        const auto txBody = MakeBackupTxBody(tableId, backupSnapshot);
        const ui64 backupTxId = txId + 1;

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), backupTxId);
        UNIT_ASSERT(GetExportActorsAliveCounter(runtime) >= 0);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvDataShard::TEvCancelBackup(backupTxId, tableId));

        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, backupTxId), false);

        TestWaitCondition(runtime, "export_cancel", [&]() {
            return csControllerGuard->GetFinishedExportsCount() >= 1;
        });

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
    }

    Y_UNIT_TEST(SaveSessionProgressSurvivesTabletReboot) {
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

        TActorId sender = runtime.AllocateEdgeActor();

        std::vector<ui64> writeIds;
        ui64 writeId = 1;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 1000 }, schema), schema, true, &writeIds));
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        const NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        const auto txBody = MakeBackupTxBody(tableId, backupSnapshot);
        const ui64 backupTxId = txId + 1;

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
        planStep =
            ProposeTxWithRebootDuringExport(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), backupTxId);

        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, backupTxId), true);

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
    }

    Y_UNIT_TEST(RepeatedCopyExportDropCycle) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 mainPathId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)), NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8)) };
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto planStep = PrepareTablet(runtime, mainPathId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;

        TActorId sender = runtime.AllocateEdgeActor();

        WriteAndCommitToMain(runtime, sender, txId, planStep, mainPathId, { 0, 5 }, schema, writeId);

        constexpr ui32 kCycleCount = 3;
        ui64 copyPathId = 2;
        for (ui32 cycle = 0; cycle < kCycleCount; ++cycle) {
            CopyTableFromMain(runtime, sender, txId, planStep, mainPathId, copyPathId);

            const NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
            const ui64 finishedExportsBefore = csControllerGuard->GetFinishedExportsCount();
            ExportFromTable(runtime, sender, csControllerGuard, txId, planStep, copyPathId, backupSnapshot);

            TestWaitCondition(runtime, Sprintf("export_cycle_%u", cycle), [&]() {
                return csControllerGuard->GetFinishedExportsCount() > finishedExportsBefore;
            });
            runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);

            DropCopyTable(runtime, sender, txId, planStep, copyPathId);

            if (cycle + 1 < kCycleCount) {
                const ui64 rowStart = 5 + cycle * 5;
                WriteAndCommitToMain(runtime, sender, txId, planStep, mainPathId, { rowStart, rowStart + 5 }, schema, writeId);
            }

            ++copyPathId;
        }

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
        UNIT_ASSERT_C(csControllerGuard->GetFinishedExportsCount() >= kCycleCount, "expected at least " << kCycleCount << " finished exports");
    }

    Y_UNIT_TEST(CancelDuringSlowS3Operations) {
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

        TActorId sender = runtime.AllocateEdgeActor();

        std::vector<ui64> writeIds;
        ui64 writeId = 1;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 5 }, schema), schema, true, &writeIds));
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        const NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        const auto txBody = MakeBackupTxBody(tableId, backupSnapshot);
        const ui64 backupTxId = txId + 1;

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);

        NActors::TBlockEvents<TEvPrivate::TEvBackupExportRecordBatch> blockExportBatch(runtime);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, txBody.SerializeAsString()));

        runtime.SimulateSleep(TDuration::Seconds(1));

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvDataShard::TEvCancelBackup(backupTxId, tableId));

        TestWaitCondition(runtime, "export_cancel_slow_s3", [&]() {
            return csControllerGuard->GetFinishedExportsCount() >= 1;
        });

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);

        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), backupTxId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_BACKUP);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        planStep = TPlanStep{ res.GetMinStep() };

        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, backupTxId), false);

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);

        blockExportBatch.Stop().Unblock();
    }

    Y_UNIT_TEST(CancelDuringProgressSaveDoesNotCrash) {
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

        TActorId sender = runtime.AllocateEdgeActor();

        std::vector<ui64> writeIds;
        ui64 writeId = 1;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 5 }, schema), schema, true, &writeIds));
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        const NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        const auto txBody = MakeBackupTxBody(tableId, backupSnapshot);
        const ui64 backupTxId = ++txId;

        std::atomic<int> localTxCompletedCount{ 0 };
        NActors::TBlockEvents<NOlap::NBackground::TEvLocalTransactionCompleted> blockLocalTxCompleted(runtime, [&](const auto& /*ev*/) {
            return localTxCompletedCount.fetch_add(1) >= 1;
        });

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, txBody.SerializeAsString()));

        TestWaitCondition(runtime, "blocked_local_tx_cursor_finished", [&]() {
            return !blockLocalTxCompleted.empty();
        }, TDuration::Seconds(30));

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvDataShard::TEvCancelBackup(backupTxId, tableId));

        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        UNIT_ASSERT_GE(blockLocalTxCompleted.size(), 2u);

        blockLocalTxCompleted.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        blockLocalTxCompleted.Stop().Unblock();

        runtime.SimulateSleep(TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
    }

    Y_UNIT_TEST(PlanAfterDuplicateProposeDoesNotCrash) {
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

        TActorId sender = runtime.AllocateEdgeActor();

        RunBackupExport(runtime, sender, csControllerGuard, txId, planStep, tableId, schema);
        TestWaitCondition(runtime, "export", [&]() {
            return NTestUtils::GetObjectKeys("test", s3Client).size() == 3;
        });
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);

        const ui64 backupTxId = txId;

        std::vector<ui64> writeIds;
        ui64 writeId = 100;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 5 }, schema), schema, true, &writeIds));
        auto commitPlanStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, commitPlanStep, txId);

        const NOlap::TSnapshot backupSnapshot(commitPlanStep.Val(), txId);
        const auto txBody = MakeBackupTxBody(tableId, backupSnapshot);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, txBody.SerializeAsString()));
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT_EQUAL(ev->Get()->Record.GetTxId(), backupTxId);
        const TPlanStep dupPlanStep{ ev->Get()->Record.GetMinStep() };

        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(dupPlanStep, backupTxId), false);

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
    }

    Y_UNIT_TEST(CancelAfterDuplicateProposeDoesNotCrash) {
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

        TActorId sender = runtime.AllocateEdgeActor();

        RunBackupExport(runtime, sender, csControllerGuard, txId, planStep, tableId, schema);
        TestWaitCondition(runtime, "export", [&]() {
            return NTestUtils::GetObjectKeys("test", s3Client).size() == 3;
        });
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);

        const ui64 backupTxId = txId;

        std::vector<ui64> writeIds;
        ui64 writeId = 100;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({ 0, 5 }, schema), schema, true, &writeIds));
        auto commitPlanStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, commitPlanStep, txId);

        const NOlap::TSnapshot backupSnapshot(commitPlanStep.Val(), txId);
        const auto txBody = MakeBackupTxBody(tableId, backupSnapshot);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, txBody.SerializeAsString()));
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT_EQUAL(ev->Get()->Record.GetTxId(), backupTxId);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvDataShard::TEvCancelBackup(backupTxId, tableId));

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
    }
}

}   // namespace NKikimr
