#include <ydb/core/base/counters.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/tx/columnshard/backup/import/session.h>
#include <ydb/core/tx/columnshard/backup/import/task.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/local.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/events.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/inflight_test_enums.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <ydb/library/actors/struct_log/log_stack.h>
#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/testlib/backup_test_enums/backup_test_enums.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <library/cpp/monlib/metrics/histogram_snapshot.h>
#include <library/cpp/testing/hook/hook.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(ImportSessionStateMachine) {
    static std::shared_ptr<NOlap::NImport::TSession> MakeSession() {
        NKikimrSchemeOp::TRestoreTask restoreTask;
        restoreTask.MutableS3Settings()->SetBucket("test");
        restoreTask.MutableS3Settings()->SetEndpoint("localhost:9000");
        restoreTask.SetTableId(1);
        using TNameTypeInfo = std::pair<TString, NScheme::TTypeInfo>;
        THashMap<ui32, TNameTypeInfo> emptyColumns;
        auto task = std::make_shared<NOlap::NImport::TImportTask>(NColumnShard::TSchemeShardLocalPathId::FromRawValue(1),
            /*columns=*/emptyColumns, restoreTask, /*schemaVersion=*/std::optional<ui64>{ 1 }, /*txId=*/std::optional<ui64>{ 42 });
        return std::make_shared<NOlap::NImport::TSession>(task);
    }

    Y_UNIT_TEST(SessionIsNotStartedAfterAbort) {
        auto session = MakeSession();
        session->Confirm();
        UNIT_ASSERT(session->IsConfirmed());
        UNIT_ASSERT(!session->IsStarted());
        session->Abort("simulated error: downloader timed out");
        UNIT_ASSERT(!session->IsStarted());
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());
    }

    Y_UNIT_TEST(AbortOnConfirmedIsLegal) {
        auto session = MakeSession();
        session->Confirm();
        UNIT_ASSERT(session->IsConfirmed());
        session->Abort("abort from confirmed state");
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());
        UNIT_ASSERT(!session->IsStarted());
        UNIT_ASSERT(!session->IsFinished());
    }

    Y_UNIT_TEST(IsStartedReturnsFalseForNonStartedStates) {
        auto session = MakeSession();
        UNIT_ASSERT(!session->IsStarted());
        session->Confirm();
        UNIT_ASSERT(!session->IsStarted());
        session->Abort("abort");
        UNIT_ASSERT(!session->IsStarted());
        UNIT_ASSERT(!session->IsFinished());
    }

    Y_UNIT_TEST(DoubleAbortDoesNotCrash) {
        auto session = MakeSession();
        session->Confirm();
        session->Abort("first abort: timeout");
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());
        session->Abort("second abort: racing error batch");
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());
    }

    Y_UNIT_TEST(AbortOnFinishedSessionDoesNotCrash) {
        auto session = MakeSession();
        session->Confirm();
        session->Abort("abort on confirmed");
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());
        session->Abort("abort again");
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());
    }

    Y_UNIT_TEST(AbortSessionControlOnAlreadyAbortedIsNoOp) {
        auto session = MakeSession();
        session->Confirm();
        session->Abort("first abort: operation timed out");
        UNIT_ASSERT(session->IsReadyForRemoveOnFinished());
        const bool shouldCallAbort = !session->IsFinished() && !session->IsReadyForRemoveOnFinished();
        UNIT_ASSERT_C(!shouldCallAbort, "TAbortSessionControl::DoApply would call Abort() on an already-Aborted "
                                        "session, reproducing the crash at session.cpp:33");
    }
}

namespace {

// Test tablets split portion blobs at 10 KiB, and a chunk of one record cannot be split.
// Values stay under that limit. The batch is over the 48 MiB import limit, so the import actor writes several blobs.
constexpr ui32 kFieldBytes = 1024;
constexpr ui32 kOversizedBatchRows = 56000;

std::shared_ptr<arrow::RecordBatch> MakeOversizedImportBatch() {
    using namespace NArrow::NConstruction;
    auto key1 = std::make_shared<TSimpleArrayConstructor<TIntSeqFiller<arrow::UInt64Type>>>("key1");
    auto key2 = std::make_shared<TSimpleArrayConstructor<TIntSeqFiller<arrow::UInt64Type>>>("key2");
    auto field =
        std::make_shared<TSimpleArrayConstructor<TStringPoolFiller>>("field", TStringPoolFiller(1, kFieldBytes, TString(kFieldBytes, 'a'), 1.));
    return TRecordBatchConstructor({ key1, key2, field }).BuildBatch(kOversizedBatchRows);
}

struct TRestoreColumn {
    const char* Name;
    const char* TypeName;
    TTypeId TypeId;
    bool Key;
};

const std::vector<TRestoreColumn>& RestoreColumns() {
    static const std::vector<TRestoreColumn> columns = {
        { "key1", "Uint64", NTypeIds::Uint64, true },
        { "key2", "Uint64", NTypeIds::Uint64, true },
        { "field", "Utf8", NTypeIds::Utf8, false },
    };
    return columns;
}

std::vector<NArrow::NTest::TTestColumn> MakeRestoreSchema() {
    std::vector<NArrow::NTest::TTestColumn> schema;
    for (const auto& column : RestoreColumns()) {
        schema.emplace_back(column.Name, TTypeInfo(column.TypeId));
    }
    return schema;
}

void FillBackupSchema(NKikimrSchemeOp::TColumnTableSchema& schema) {
    for (const auto& column : RestoreColumns()) {
        auto& proto = *schema.MutableColumns()->Add();
        proto.SetName(column.Name);
        proto.SetType(column.TypeName);
    }
}

void FillRestoreDescription(NKikimrSchemeOp::TTableDescription& description) {
    for (ui32 i = 0; i < RestoreColumns().size(); ++i) {
        const auto& column = RestoreColumns()[i];
        const ui32 id = i + 1;
        auto& proto = *description.MutableColumns()->Add();
        proto.SetName(column.Name);
        proto.SetType(column.TypeName);
        proto.SetId(id);
        proto.SetTypeId(column.TypeId);
        if (column.Key) {
            description.AddKeyColumnNames(column.Name);
            description.AddKeyColumnIds(id);
        }
    }
}

NKikimrTxColumnShard::TBackupTxBody MakeBackupTxBody(const ui64 tableId, const NOlap::TSnapshot& snapshot, const TString& bucket,
    const std::optional<ui64> bytesBatchSize = {},
    const NDataShard::NBackupRestoreTraits::EDataFormat dataFormat = NDataShard::NBackupRestoreTraits::EDataFormat::YdbDump) {
    NKikimrTxColumnShard::TBackupTxBody txBody;
    auto& backupTask = *txBody.MutableBackupTask();
    backupTask.SetTableName("abcde");
    backupTask.SetTableId(tableId);
    backupTask.SetSnapshotStep(snapshot.GetPlanStep());
    backupTask.SetSnapshotTxId(snapshot.GetTxId());
    auto& s3Settings = *backupTask.MutableS3Settings();
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    s3Settings.SetBucket(bucket);
    if (dataFormat == NDataShard::NBackupRestoreTraits::EDataFormat::Parquet) {
        s3Settings.MutableExportDataSettings()->MutableParquet();
    } else {
        s3Settings.MutableExportDataSettings()->MutableYdbDump();
    }
    if (bytesBatchSize) {
        backupTask.MutableScanSettings()->SetBytesBatchSize(*bytesBatchSize);
    }
    auto& table = *backupTask.MutableTable();
    table.MutableColumnTableDescription()->SetColumnShardCount(4);
    FillBackupSchema(*table.MutableColumnTableDescription()->MutableSchema());
    table.MutableSelf();
    return txBody;
}

NKikimrTxColumnShard::TRestoreTxBody MakeRestoreTxBody(const ui64 tableId, const TString& bucket, const std::optional<ui32> readBatchSize = {}) {
    NKikimrTxColumnShard::TRestoreTxBody txBody;
    auto& restoreTask = *txBody.MutableRestoreTask();
    restoreTask.SetTableId(tableId);
    restoreTask.MutableS3Settings()->SetEndpoint(GetEnv("S3_ENDPOINT"));
    restoreTask.MutableS3Settings()->SetBucket(bucket);
    if (readBatchSize) {
        restoreTask.MutableS3Settings()->MutableLimits()->SetReadBatchSize(*readBatchSize);
    }
    FillRestoreDescription(*restoreTask.MutableTableDescription());
    return txBody;
}

}   // namespace

Y_UNIT_TEST_SUITE(Restore) {
    using EDataFormat = NDataShard::NBackupRestoreTraits::EDataFormat;

    [[nodiscard]] TPlanStep ProposeTx(
        TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody, const ui64 txId) {
        auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(txKind, sender, txId, txBody);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        return TPlanStep{ res.GetMinStep() };
    }

    void PlanTx(TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, NOlap::TSnapshot snap,
        bool waitResult = true) {
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
    void TestWaitCondition(
        TTestBasicRuntime & runtime, const TString& title, const TChecker& checker, const TDuration d = TDuration::Seconds(10)) {
        const TInstant start = TInstant::Now();
        while (TInstant::Now() - start < d && !checker()) {
            Cerr << "waiting " << title << Endl;
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        AFL_VERIFY(checker());
    }

    void PlanAndWait(TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, const TPlanStep planStep,
        const ui64 txId) {
        auto evSubscribe = std::make_unique<TEvColumnShard::TEvNotifyTxCompletion>(txId);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evSubscribe.release());
        PlanTx(runtime, sender, txKind, NOlap::TSnapshot(planStep, txId), false);
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get()->Record.GetOpResult().GetSuccess());
    }

    template <class TCounter>
    void RunCountedTx(TTestBasicRuntime & runtime, TActorId & sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody,
        ui64& txId, const TCounter& counter) {
        AFL_VERIFY(counter() == 0);
        const auto planStep = ProposeTx(runtime, sender, txKind, txBody, ++txId);
        AFL_VERIFY(counter() == 1);
        PlanAndWait(runtime, sender, txKind, planStep, txId);
    }

    void CommitBlob(TTestBasicRuntime & runtime, TActorId & sender, const ui64 writeId, ui64& txId, TPlanStep& planStep, const ui64 tableId,
        const TString& data, const std::vector<NArrow::NTest::TTestColumn>& schema) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, data, schema, true, &writeIds));
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);
    }

    void ProposeRestore(EDataFormat dataFormat) {
        UNIT_ASSERT(dataFormat == EDataFormat::YdbDump || dataFormat == EDataFormat::Parquet);

        const TString bucketName = dataFormat == EDataFormat::Parquet ? "test-parquet" : "test-csv";
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket(bucketName, s3Client);
        const auto schema = MakeRestoreSchema();
        const ui64 tableId = 1;

        {
            TTestBasicRuntime runtime;
            TTester::Setup(runtime);
            runtime.GetAppData().FeatureFlags.SetEnableExportInParquet(true);

            auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            auto planStep = PrepareTablet(runtime, tableId, schema, 2);
            ui64 txId = 111;
            TActorId sender = runtime.AllocateEdgeActor();

            ui64 writeId = 1;
            CommitBlob(runtime, sender, writeId++, txId, planStep, tableId, MakeTestBlob({ 0, 5 }, schema), schema);
            TestWaitCondition(runtime, "insert compacted", [&]() {
                ++writeId;
                CommitBlob(runtime, sender, writeId, txId, planStep, tableId, MakeTestBlob({ writeId * 5, (writeId + 1) * 5 }, schema), schema);
                return true;
            }, TDuration::Seconds(1000));

            RunCountedTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP,
                MakeBackupTxBody(tableId, NOlap::TSnapshot(planStep.Val(), txId), bucketName, {}, dataFormat).SerializeAsString(), txId, [&]() {
                    return csControllerGuard->GetFinishedExportsCount();
                });
            TestWaitCondition(runtime, "export", [&]() {
                return NTestUtils::GetObjectKeys(bucketName, s3Client).size() == 3;
            });
            VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
        }

        {
            TTestBasicRuntime runtime;
            TTester::Setup(runtime);
            runtime.GetAppData().FeatureFlags.SetEnableImportInParquet(true);

            auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            Y_UNUSED(PrepareTablet(runtime, tableId, schema, 2));
            ui64 txId = 111;
            TActorId sender = runtime.AllocateEdgeActor();

            RunCountedTx(
                runtime, sender, NKikimrTxColumnShard::TX_KIND_RESTORE, MakeRestoreTxBody(tableId, bucketName).SerializeAsString(), txId, [&]() {
                    return csControllerGuard->GetFinishedImportsCount();
                });
            TestWaitCondition(runtime, "import", [&]() {
                return true;
            });
            VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());

            YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
                {"TESTSTEP", 8});
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(0, 0));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT(reader.IsCorrectlyFinished());
            Y_UNUSED(NArrow::TColumnOperator().VerifyIfAbsent().Extract(rb, TTestSchema::ExtractNames(schema)));
            UNIT_ASSERT((ui32)rb->num_columns() == TTestSchema::ExtractNames(schema).size());
            UNIT_ASSERT(rb->num_rows());
            UNIT_ASSERT_VALUES_EQUAL(rb->ToString(),
                "key1:   [\n    0,\n    1,\n    2,\n    3,\n    4,\n    15,\n    16,\n    17,\n    18,\n    19,\n    20,\n    21,\n    "
                "22,\n    23,\n    24\n  ]\nkey2:   [\n    0,\n    1,\n    2,\n    3,\n    4,\n    15,\n    16,\n    17,\n    18,\n    "
                "19,\n    20,\n    21,\n    22,\n    23,\n    24\n  ]\nfield:   [\n    \"0\",\n    \"1\",\n    \"2\",\n    \"3\",\n    "
                "\"4\",\n    \"15\",\n    \"16\",\n    \"17\",\n    \"18\",\n    \"19\",\n    \"20\",\n    \"21\",\n    \"22\",\n    "
                "\"23\",\n    \"24\"\n  ]\n");
        }
    }

    Y_UNIT_TEST(StopImportClearsInflight, EImportInflightTestCase) {
        const auto operation = Arg<0>();
        const bool writing = operation == EImportInflightTestCase::Write;
        const TString bucket = writing ? "stop-import-writing" : "stop-import-reading";
        NTestUtils::CreateBucketWithObject(bucket, "data_00.csv", "1,2,\"value\"\n");

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        Y_UNUSED(PrepareTablet(runtime, 1, MakeRestoreSchema(), 2));
        const auto sender = runtime.AllocateEdgeActor();
        const auto counters =
            GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
                "ImportActor");
        const auto alive = counters->GetCounter("Value/Import/Actors/Alive", false);
        const auto reading = counters->GetCounter("Value/Import/Read/Inflight", false);
        const auto writingCounter = counters->GetCounter("Value/Import/Write/Inflight", false);
        const auto saving = counters->GetCounter("Value/Import/SaveProgress/Inflight", false);
        const auto downloaderCounters =
            GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
                "ImportDownloader");
        const auto downloaderAlive = downloaderCounters->GetCounter("Value/ImportDownloader/Actors/Alive", false);
        const auto processing = downloaderCounters->GetCounter("Value/ImportDownloader/Process/Inflight", false);
        const auto histogramSamples = [&](const TString& name) {
            const auto snapshot = counters->FindHistogram(name)->Snapshot();
            ui64 count = 0;
            for (ui32 i = 0; i < snapshot->Count(); ++i) {
                count += snapshot->Value(i);
            }
            return count;
        };
        UNIT_ASSERT_VALUES_EQUAL(alive->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(reading->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(writingCounter->Val(), 0);

        TActorId importActorId;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (operation == EImportInflightTestCase::Read && ev->GetTypeRewrite() == TEvPrivate::TEvBackupImportRecordBatch::EventType) {
                const auto* batch = ev->Get<TEvPrivate::TEvBackupImportRecordBatch>();
                UNIT_ASSERT_C(batch->Data && !batch->Error && !batch->IsLast, "expected a data batch before stopping the import");
                importActorId = ev->GetRecipientRewrite();
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (writing && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType) {
                importActorId = ev->Sender;
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (operation == EImportInflightTestCase::SaveProgress && saving->Val() == 1 &&
                ev->GetTypeRewrite() == NOlap::NBackground::TEvExecuteGeneralLocalTransaction::EventType) {
                importActorId = ev->Sender;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto propose = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
            NKikimrTxColumnShard::TX_KIND_RESTORE, sender, 112, MakeRestoreTxBody(1, bucket).SerializeAsString());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, propose.release());
        TestWaitCondition(runtime, "import operation in flight", [&]() {
            return !!importActorId;
        });
        UNIT_ASSERT_VALUES_EQUAL(alive->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(downloaderAlive->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reading->Val(), operation == EImportInflightTestCase::Read ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(writingCounter->Val(), writing ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(saving->Val(), operation == EImportInflightTestCase::SaveProgress ? 1 : 0);
        const auto readSamples = histogramSamples("Histogram/Import/Read/DurationMs/Histogram");
        const auto writeSamples = histogramSamples("Histogram/Import/Write/DurationMs/Histogram");
        const auto saveSamples = histogramSamples("Histogram/Import/SaveProgress/DurationMs/Histogram");
        const auto received = counters->GetCounter("Deriviative/Import/Batches/Received", true)->Val();
        const auto written = counters->GetCounter("Deriviative/Import/Batches/Written", true)->Val();

        // Tablet shutdown stops session actors with PoisonPill. The outstanding reply
        // is dropped above so it cannot finish the operation before the actor stops.
        runtime.Send(new IEventHandle(importActorId, sender, new TEvents::TEvPoisonPill()));
        TestWaitCondition(runtime, "import actor stopped", [&]() {
            return alive->Val() == 0 && downloaderAlive->Val() == 0;
        });
        UNIT_ASSERT_VALUES_EQUAL_C(reading->Val(), 0, "stopped import actor leaked Read/Inflight");
        UNIT_ASSERT_VALUES_EQUAL_C(writingCounter->Val(), 0, "stopped import actor leaked Write/Inflight");
        UNIT_ASSERT_VALUES_EQUAL(saving->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(processing->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(histogramSamples("Histogram/Import/Read/DurationMs/Histogram"), readSamples);
        UNIT_ASSERT_VALUES_EQUAL(histogramSamples("Histogram/Import/Write/DurationMs/Histogram"), writeSamples);
        UNIT_ASSERT_VALUES_EQUAL(histogramSamples("Histogram/Import/SaveProgress/DurationMs/Histogram"), saveSamples);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Deriviative/Import/Batches/Received", true)->Val(), received);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Deriviative/Import/Batches/Written", true)->Val(), written);
    }

    Y_UNIT_TEST(RestoreWithInflightWriteOverload) {
        const TString bucket = "restore-write-overload";
        NTestUtils::CreateBucketWithObject(bucket, "data_00.csv", "1,2,\"value\"\n");

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        // The first write is accepted and exhausts the byte limit. Subsequent writes
        // are rejected until it finishes, regardless of their own payload size.
        runtime.GetAppData().ColumnShardConfig.SetWritingInFlightRequestBytesLimit(1);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        const auto schema = MakeRestoreSchema();
        constexpr ui64 tableId = 1;
        Y_UNUSED(PrepareTablet(runtime, tableId, schema, 2));
        auto sender = runtime.AllocateEdgeActor();

        TAutoPtr<IEventHandle> heldWriteResult;
        ui32 overloadCount = 0;
        TActorId importActorId;
        std::vector<TDuration> retryDelays;
        auto previousScheduledFilter =
            runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) {
                return true;
            });
        runtime.SetScheduledEventFilter(
            [&, previousScheduledFilter](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& ev, TDuration delay, TInstant& deadline) {
                if (ev->GetRecipientRewrite() == importActorId && ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                    retryDelays.push_back(delay);
                }
                return previousScheduledFilter(r, ev, delay, deadline);
            });
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (overloadCount < 2) {
                if (auto* result = TryGetPrivateEvent<NPrivateEvents::NWrite::TEvWritePortionResult>(ev)) {
                    UNIT_ASSERT_VALUES_EQUAL(result->GetWriteStatus(), NKikimrProto::OK);
                    UNIT_ASSERT(!heldWriteResult);
                    heldWriteResult = ev.Release();
                    return true;
                }
            }
            return false;
        });

        // Keep a real foreground write in flight while the import tries to write
        // its first batch. No synthetic TEvWriteResult is injected.
        UNIT_ASSERT(WriteData(runtime, sender, 1, tableId, MakeTestBlob({ 10, 11 }, schema), schema, /*waitResult=*/false, /*writeIds=*/nullptr,
            NEvWrite::EModificationType::Upsert, /*lockId=*/0));
        TestWaitCondition(runtime, "foreground write held in flight", [&]() {
            return !!heldWriteResult;
        });

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType) {
                const auto* result = ev->Get<NEvents::TDataEvents::TEvWriteResult>();
                if (result->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED) {
                    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOrigin(), TTestTxConfig::TxTablet0);
                    UNIT_ASSERT_C(result->GetError().Contains("The limit on the total size of in-flight write requests"), result->GetError());
                    ++overloadCount;
                    UNIT_ASSERT(heldWriteResult);
                    importActorId = ev->GetRecipientRewrite();
                    runtime.EnableScheduleForActor(importActorId);
                    // Reject two attempts before freeing resources. Each response
                    // reaches the import actor unchanged, and the next retry succeeds.
                    if (overloadCount == 2) {
                        runtime.Send(heldWriteResult.Release());
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 txId = 111;
        RunCountedTx(
            runtime, sender, NKikimrTxColumnShard::TX_KIND_RESTORE, MakeRestoreTxBody(tableId, bucket).SerializeAsString(), txId, [&]() {
                return csControllerGuard->GetFinishedImportsCount();
            });
        UNIT_ASSERT_VALUES_EQUAL(overloadCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(retryDelays.size(), 2);
        UNIT_ASSERT_GE(retryDelays[0], TDuration::MilliSeconds(500));
        UNIT_ASSERT_LT(retryDelays[0], TDuration::Seconds(1));
        UNIT_ASSERT_GE(retryDelays[1], TDuration::Seconds(1));
        UNIT_ASSERT_LT(retryDelays[1], TDuration::Seconds(2));
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
        const auto counters =
            GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
                "ImportActor");
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Value/Import/Write/Inflight", false)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Deriviative/Import/Batches/Written", true)->Val(), 1);

        TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(0, 0));
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(schema));
        const auto batch = reader.ReadAll();
        UNIT_ASSERT(batch);
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 2);
        const auto field = std::static_pointer_cast<arrow::StringArray>(batch->GetColumnByName("field"));
        UNIT_ASSERT(field);
        UNIT_ASSERT_VALUES_EQUAL(field->GetString(0), "value");
    }

    Y_UNIT_TEST(RestoreWriteError) {
        const TString bucket = "restore-write-error";
        NTestUtils::CreateBucketWithObject(bucket, "data_00.csv", "1,2,\"value\"\n");

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        runtime.GetAppData().ColumnShardConfig.SetWritingEnabled(false);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        Y_UNUSED(PrepareTablet(runtime, 1, MakeRestoreSchema(), 2));
        auto sender = runtime.AllocateEdgeActor();
        constexpr ui64 txId = 112;
        const auto planStep =
            ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_RESTORE, MakeRestoreTxBody(1, bucket).SerializeAsString(), txId);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvColumnShard::TEvNotifyTxCompletion(txId));
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_RESTORE, NOlap::TSnapshot(planStep, txId), false);
        const auto result = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
        UNIT_ASSERT(!result->Get()->Record.GetOpResult().GetSuccess());
        UNIT_ASSERT_C(result->Get()->Record.GetOpResult().GetExplain().Contains("writing disabled"), result->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(csControllerGuard->GetFinishedImportsCount(), 1);
        VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());

        const auto counters =
            GetServiceCounters(runtime.GetDynamicCounters(0), "tablets")->GetSubgroup("subsystem", "columnshard")->GetSubgroup("module_id",
                "ImportActor");
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Value/Import/Write/Inflight", false)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Deriviative/Import/Batches/Written", true)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Deriviative/Import/Errors/Count", true)->Val(), 1);
    }

    Y_UNIT_TEST(RestoreOversizedBatch) {
        auto batch = MakeOversizedImportBatch();
        constexpr ui64 kImportBlobLimit = 48 * 1024 * 1024;
        auto split = NArrow::SplitByBlobSize(batch, NArrow::TBatchSplittingContext(kImportBlobLimit));
        UNIT_ASSERT(split.IsSuccess());
        UNIT_ASSERT_C(split.GetResult().size() > 1, "batch must exceed the import blob limit");

        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        const TString bucket = "oversized-backup";
        NTestUtils::CreateBucket(bucket, s3Client);
        const auto schema = MakeRestoreSchema();
        const ui64 tableId = 1;
        // One S3 object and one restore read, so the downloader emits a single batch over the import limit.
        constexpr ui32 kWholeFileBytes = 128 * 1024 * 1024;

        {
            TTestBasicRuntime runtime;
            TTester::Setup(runtime);
            auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            auto planStep = PrepareTablet(runtime, tableId, schema, 2);
            ui64 txId = 111;
            TActorId sender = runtime.AllocateEdgeActor();

            ui64 writeId = 1;
            CommitBlob(runtime, sender, writeId, txId, planStep, tableId, NArrow::SerializeBatchNoCompression(batch), schema);
            RunCountedTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP,
                MakeBackupTxBody(tableId, NOlap::TSnapshot(planStep.Val(), txId), bucket, kWholeFileBytes).SerializeAsString(), txId, [&]() {
                    return csControllerGuard->GetFinishedExportsCount();
                });
            TestWaitCondition(runtime, "export", [&]() {
                return NTestUtils::GetObjectKeys(bucket, s3Client).size() == 3;
            });
            VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());
        }

        {
            TTestBasicRuntime runtime;
            TTester::Setup(runtime);
            auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
            Y_UNUSED(PrepareTablet(runtime, tableId, schema, 2));
            ui64 txId = 111;
            TActorId sender = runtime.AllocateEdgeActor();

            RunCountedTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_RESTORE,
                MakeRestoreTxBody(tableId, bucket, kWholeFileBytes).SerializeAsString(), txId, [&]() {
                    return csControllerGuard->GetFinishedImportsCount();
                });
            VerifyNoBackupOrRestoreArtifacts(runtime, csControllerGuard.operator->());

            TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(0, 0));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT(reader.IsCorrectlyFinished());
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), kOversizedBatchRows);
            auto field = std::static_pointer_cast<arrow::StringArray>(rb->GetColumnByName("field"));
            UNIT_ASSERT(field);
            const TString expectedField(kFieldBytes, 'a');
            const auto gotFirst = field->GetView(0);
            const auto gotLast = field->GetView(kOversizedBatchRows - 1);
            UNIT_ASSERT_VALUES_EQUAL(TString(gotFirst.data(), gotFirst.size()), expectedField);
            UNIT_ASSERT_VALUES_EQUAL(TString(gotLast.data(), gotLast.size()), expectedField);
            auto key1 = std::static_pointer_cast<arrow::UInt64Array>(rb->GetColumnByName("key1"));
            auto key2 = std::static_pointer_cast<arrow::UInt64Array>(rb->GetColumnByName("key2"));
            UNIT_ASSERT(key1);
            UNIT_ASSERT(key2);
            UNIT_ASSERT_VALUES_EQUAL(key1->Value(0), 0);
            UNIT_ASSERT_VALUES_EQUAL(key2->Value(0), 0);
            UNIT_ASSERT_VALUES_EQUAL(key1->Value(kOversizedBatchRows - 1), kOversizedBatchRows - 1);
            UNIT_ASSERT_VALUES_EQUAL(key2->Value(kOversizedBatchRows - 1), kOversizedBatchRows - 1);
        }
    }

    Y_UNIT_TEST(ProposeRestore, EBackupTestDataFormat) {
        ProposeRestore(ToDataFormat(Arg<0>()));
    }
}

}   // namespace NKikimr
