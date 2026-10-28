#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/global.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/local.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/actor/actor.h>
#include <ydb/core/tx/columnshard/export/session/control.h>
#include <ydb/core/tx/columnshard/export/session/session.h>
#include <ydb/core/tx/columnshard/export/session/task.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/inflight_test_enums.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/wrappers/abstract.h>

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>
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

NMonitoring::TDynamicCounters::TCounterPtr GetExportActorCounter(TTestBasicRuntime& runtime, const char* name) {
    const auto subgroup =
        GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
            "ExportActor");
    return subgroup->GetCounter(name, false);
}

i64 GetExportActorsAliveCounter(TTestBasicRuntime& runtime) {
    return GetExportActorCounter(runtime, "Value/Export/Actors/Alive")->Val();
}

i64 GetExportReadInflight(TTestBasicRuntime& runtime) {
    return GetExportActorCounter(runtime, "Value/Export/Read/Inflight")->Val();
}

i64 GetExportAckInflight(TTestBasicRuntime& runtime) {
    return GetExportActorCounter(runtime, "Value/Export/Ack/Inflight")->Val();
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
    void TestScanPings(bool allowPings, bool savingCursor, bool failDuringSave = false) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8)),
        };
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;
        auto sender = runtime.AllocateEdgeActor();
        WriteAndCommitToMain(runtime, sender, txId, planStep, tableId, { 0, 5 }, schema, writeId);

        // These tests exercise the export actor's flow control, without waiting
        // for asynchronous HTTP requests in the simulated actor runtime.
        auto storageObserver = runtime.AddObserver<NWrappers::TEvExternalStorage::TEvPutObjectRequest>([&](auto& ev) {
            using TResponse = NWrappers::TEvExternalStorage::TEvPutObjectResponse;
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, new TResponse(TString(ev->Get()->Request.GetKey()),
                                                                         TResponse::TOutcome(Aws::S3::Model::PutObjectResult()))), 0, true);
            ev.Reset();
        });
        TActorId exportActor;
        TActorId scanner;
        auto initObserver = runtime.AddObserver<NKqp::TEvKqpCompute::TEvScanInitActor>([&](auto& ev) {
            exportActor = ev->Recipient;
            scanner = ActorIdFromProto(ev->Get()->Record.GetScanActorId());
            ev->Get()->Record.SetAllowPings(allowPings);
            runtime.EnableScheduleForActor(exportActor);
        });
        ui32 pings = 0;
        ui32 acks = 0;
        auto pingObserver = runtime.AddObserver<NKqp::TEvKqpCompute::TEvScanPing>([&](auto& ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, scanner);
            UNIT_ASSERT_VALUES_EQUAL(ev->Sender, exportActor);
            ++pings;
        });
        auto ackObserver = runtime.AddObserver<NKqp::TEvKqpCompute::TEvScanDataAck>([&](auto&) {
            ++acks;
        });
        NActors::TBlockEvents<TEvPrivate::TEvBackupExportRecordBatch> blockBatch(runtime, [&](const auto&) {
            return !savingCursor;
        });
        const auto saving = GetExportActorCounter(runtime, "Value/Export/SaveCursor/Inflight");
        NActors::TBlockEvents<NOlap::NBackground::TEvLocalTransactionCompleted> blockSave(runtime, [&](const auto& ev) {
            return savingCursor && ev->Recipient == exportActor && saving->Val() == 1;
        });
        const auto body = MakeBackupTxBody(tableId, NOlap::TSnapshot(planStep.Val(), txId));
        const auto backupTxId = ++txId;
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, body.SerializeAsString()));
        TestWaitCondition(runtime, "export waiting for uploader or cursor save", [&]() {
            return savingCursor ? !blockSave.empty() : !blockBatch.empty();
        });
        const auto acksBeforeWait = acks;
        // The first wakeup may have been scheduled before enabling the actor's
        // timers. Trigger it explicitly and verify the following periodic pings.
        runtime.Send(new IEventHandle(exportActor, sender, new TEvents::TEvWakeup()), 0, true);
        runtime.SimulateSleep(TDuration::Seconds(45));
        UNIT_ASSERT_VALUES_EQUAL(acks, acksBeforeWait);
        if (allowPings) {
            UNIT_ASSERT_GE(pings, 3u);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(pings, 0);
        }

        if (failDuringSave) {
            const TString error = "injected uploader scan reset";
            runtime.Send(new IEventHandle(exportActor, sender, new TEvPrivate::TEvBackupExportError(error)), 0, true);
            runtime.SimulateSleep(TDuration::Seconds(1));
            blockSave.Stop().Unblock();
            TestWaitCondition(runtime, "export failed after cursor save", [&]() {
                return controller->GetFinishedExportsCount() >= 1 && GetExportActorsAliveCounter(runtime) == 0;
            });
            auto prepared = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
            planStep = TPlanStep(prepared->Get()->Record.GetMinStep());
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvColumnShard::TEvNotifyTxCompletion(backupTxId));
            PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, backupTxId));
            auto completed = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT(!completed->Get()->Record.GetOpResult().GetSuccess());
            UNIT_ASSERT_STRING_CONTAINS(completed->Get()->Record.GetOpResult().GetExplain(), error);
        } else {
            runtime.Send(new IEventHandle(exportActor, sender, new TEvents::TEvPoisonPill()), 0, true);
            TestWaitCondition(runtime, "export actor stopped", [&]() {
                return GetExportActorsAliveCounter(runtime) == 0;
            });
        }
        const auto pingsAtStop = pings;
        runtime.SimulateSleep(TDuration::Seconds(45));
        UNIT_ASSERT_VALUES_EQUAL(pings, pingsAtStop);
        UNIT_ASSERT_VALUES_EQUAL(GetExportReadInflight(runtime), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetExportAckInflight(runtime), 0);
    }

    Y_UNIT_TEST(PingsWhileWaitingForUploader) {
        TestScanPings(true, false);
    }

    Y_UNIT_TEST(PingsWhileSavingCursor) {
        TestScanPings(true, true);
    }

    Y_UNIT_TEST(NoPingsWhenScannerDisallowsThem) {
        TestScanPings(false, false);
    }

    Y_UNIT_TEST(UploaderErrorDuringCursorSaveIsReported) {
        TestScanPings(true, true, true);
    }

    Y_UNIT_TEST(StopExportClearsInflight, EExportInflightTestCase) {
        const auto operation = Arg<0>();
        NTestUtils::CreateBucket("test");
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8)),
        };
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;
        auto sender = runtime.AllocateEdgeActor();
        WriteAndCommitToMain(runtime, sender, txId, planStep, tableId, { 0, 5 }, schema, writeId);

        const auto reading = GetExportActorCounter(runtime, "Value/Export/Read/Inflight");
        const auto writing = GetExportActorCounter(runtime, "Value/Export/Write/Inflight");
        const auto saving = GetExportActorCounter(runtime, "Value/Export/SaveCursor/Inflight");
        const auto ack = GetExportActorCounter(runtime, "Value/Export/Ack/Inflight");
        const auto counters =
            GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
                "ExportActor");
        const auto histogramSamples = [&](const TString& name) {
            const auto snapshot = counters->FindHistogram(name)->Snapshot();
            ui64 count = 0;
            for (ui32 i = 0; i < snapshot->Count(); ++i) {
                count += snapshot->Value(i);
            }
            return count;
        };
        const auto written = GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")
                                 ->GetSubgroup("subsystem", "columnshard")
                                 ->GetSubgroup("module_id", "ExportActor")
                                 ->GetCounter("Deriviative/Export/Batches/Written", true);
        TActorId exportActorId;
        bool cancelSaveBlocked = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            const auto type = ev->GetTypeRewrite();
            if ((operation == EExportInflightTestCase::Read || operation == EExportInflightTestCase::CancelledRead) &&
                type == NKqp::TEvKqpCompute::TEvScanData::EventType) {
                exportActorId = ev->GetRecipientRewrite();
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (operation == EExportInflightTestCase::Write && type == TEvPrivate::TEvBackupExportRecordBatch::EventType) {
                exportActorId = ev->Sender;
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (type == NOlap::NBackground::TEvExecuteGeneralLocalTransaction::EventType) {
                if (operation == EExportInflightTestCase::SaveCursor && saving->Val() == 1) {
                    exportActorId = ev->Sender;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                if (operation == EExportInflightTestCase::CancelledRead && exportActorId && ev->Sender == exportActorId) {
                    cancelSaveBlocked = true;
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto body = MakeBackupTxBody(tableId, NOlap::TSnapshot(planStep.Val(), txId));
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, ++txId, body.SerializeAsString()));
        TestWaitCondition(runtime, "export operation in flight", [&]() {
            return !!exportActorId;
        });
        const bool readingOperation = operation == EExportInflightTestCase::Read || operation == EExportInflightTestCase::CancelledRead;
        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 1);
        UNIT_ASSERT_VALUES_EQUAL(reading->Val(), readingOperation ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(ack->Val(), readingOperation ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(writing->Val(), operation == EExportInflightTestCase::Write ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(saving->Val(), operation == EExportInflightTestCase::SaveCursor ? 1 : 0);

        const auto writtenBeforeStop = written->Val();
        const auto readSamples = histogramSamples("Histogram/Export/Read/DurationMs/Histogram");
        const auto writeSamples = histogramSamples("Histogram/Export/Write/DurationMs/Histogram");
        const auto saveSamples = histogramSamples("Histogram/Export/SaveCursor/DurationMs/Histogram");
        if (operation == EExportInflightTestCase::CancelledRead) {
            // Keep the cancelled actor alive until a late scan error has been handled.
            const NOlap::NBackground::TSessionControlContainer control(std::make_shared<NOlap::NBackground::TFakeStatusChannel>(),
                std::make_shared<NOlap::NExport::TAbortSessionControl>("CS::EXPORT", "1"));
            runtime.Send(new IEventHandle(exportActorId, sender, new NOlap::NBackground::TEvSessionControl(control)));
            TestWaitCondition(runtime, "cancel state save blocked", [&]() {
                return cancelSaveBlocked;
            });
            runtime.Send(new IEventHandle(exportActorId, sender, new NKqp::TEvKqpCompute::TEvScanError()));
            TestWaitCondition(runtime, "late scan error handled", [&]() {
                return ack->Val() == 0;
            });
        }

        runtime.Send(new IEventHandle(exportActorId, sender, new TEvents::TEvPoisonPill()));
        TestWaitCondition(runtime, "export actor stopped", [&]() {
            return GetExportActorsAliveCounter(runtime) == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL(reading->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(writing->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(saving->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ack->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL_C(written->Val(), writtenBeforeStop, "stopping an export must not count an unfinished write as completed");
        UNIT_ASSERT_VALUES_EQUAL(histogramSamples("Histogram/Export/Read/DurationMs/Histogram"), readSamples);
        UNIT_ASSERT_VALUES_EQUAL(histogramSamples("Histogram/Export/Write/DurationMs/Histogram"), writeSamples);
        UNIT_ASSERT_VALUES_EQUAL(histogramSamples("Histogram/Export/SaveCursor/DurationMs/Histogram"), saveSamples);
    }

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
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
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
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
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
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
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
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
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
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());

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
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
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

    Y_UNIT_TEST(InflightCountersLeakAfterRebootDuringWaitData) {
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
        const ui64 backupTxId = ++txId;

        UNIT_ASSERT_VALUES_EQUAL(GetExportReadInflight(runtime), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetExportAckInflight(runtime), 0);

        NActors::TBlockEvents<NKqp::TEvKqpCompute::TEvScanData> blockScanData(runtime);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, txBody.SerializeAsString()));

        TestWaitCondition(runtime, "export_wait_data_inflight", [&]() {
            return GetExportReadInflight(runtime) >= 1 && GetExportAckInflight(runtime) >= 1;
        }, TDuration::Seconds(30));

        const i64 readInflightBeforeReboot = GetExportReadInflight(runtime);
        const i64 ackInflightBeforeReboot = GetExportAckInflight(runtime);
        Cerr << "Before reboot: Read/Inflight=" << readInflightBeforeReboot << " Ack/Inflight=" << ackInflightBeforeReboot << Endl;

        blockScanData.clear();
        blockScanData.Stop();

        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, txBody.SerializeAsString()));
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), backupTxId);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        planStep = TPlanStep{ res.GetMinStep() };

        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, backupTxId), true);

        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);

        const i64 readInflightAfter = GetExportReadInflight(runtime);
        const i64 ackInflightAfter = GetExportAckInflight(runtime);
        Cerr << "After successful backup: Read/Inflight=" << readInflightAfter << " Ack/Inflight=" << ackInflightAfter << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(readInflightAfter, 0,
            "Value/Export/Read/Inflight leaked after backup completed "
            "(was "
                << readInflightBeforeReboot << " at reboot). Got: " << readInflightAfter);
        UNIT_ASSERT_VALUES_EQUAL_C(ackInflightAfter, 0,
            "Value/Export/Ack/Inflight leaked after backup completed "
            "(was "
                << ackInflightBeforeReboot << " at reboot). Got: " << ackInflightAfter);
    }

    Y_UNIT_TEST(ScanActorNotAbortedOnExportError) {
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

        UNIT_ASSERT_VALUES_EQUAL(NOlap::NReader::TColumnShardScan::GetCounter().Val(), 0);

        std::atomic<ui64> abortExecutionCount{ 0 };
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKqp::TEvKqp::TEvAbortExecution::EventType) {
                abortExecutionCount.fetch_add(1);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NActors::TBlockEvents<TEvPrivate::TEvBackupExportRecordBatch> blockExportBatch(runtime);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_BACKUP, sender, backupTxId, txBody.SerializeAsString()));

        TestWaitCondition(runtime, "export_wait_writing", [&]() {
            return !blockExportBatch.empty() && NOlap::NReader::TColumnShardScan::GetCounter().Val() >= 1;
        }, TDuration::Seconds(30));

        const i64 scansBeforeAbort = NOlap::NReader::TColumnShardScan::GetCounter().Val();
        Cerr << "Scans alive before abort: " << scansBeforeAbort << " AbortExecution events: " << abortExecutionCount.load() << Endl;
        UNIT_ASSERT_GE(scansBeforeAbort, 1);

        {
            auto& batchEv = blockExportBatch.front();
            const TActorId exportActorId = batchEv->GetRecipientRewrite();
            runtime.Send(
                new IEventHandle(exportActorId, sender, new TEvPrivate::TEvBackupExportError("injected export error for scan-abort repro")));
        }

        TestWaitCondition(runtime, "export_finished_after_error", [&]() {
            return csControllerGuard->GetFinishedExportsCount() >= 1 && GetExportActorsAliveCounter(runtime) == 0;
        }, TDuration::Seconds(30));

        runtime.SimulateSleep(TDuration::Seconds(1));

        const i64 scansAfterError = NOlap::NReader::TColumnShardScan::GetCounter().Val();
        const ui64 abortsSent = abortExecutionCount.load();
        Cerr << "Scans alive after export error: " << scansAfterError << " AbortExecution events: " << abortsSent << Endl;

        UNIT_ASSERT_VALUES_EQUAL(GetExportActorsAliveCounter(runtime), 0);
        UNIT_ASSERT_VALUES_EQUAL_C(scansAfterError, 0,
            "TColumnShardScan left alive after export error: Abort was not sent to ScanActorId. "
            "Scans: "
                << scansAfterError << ", TEvAbortExecution sent: " << abortsSent);

        runtime.SetObserverFunc(prevObserver);
        blockExportBatch.Stop().Unblock();
    }
}

}   // namespace NKikimr
