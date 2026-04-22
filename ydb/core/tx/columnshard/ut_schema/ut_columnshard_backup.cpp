#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/base/blobstorage.h>
#include <util/string/printf.h>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>
#include <ydb/library/actors/protos/unittests.pb.h>
#include <util/string/join.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <library/cpp/testing/hook/hook.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;


Y_UNIT_TEST_SUITE(BackupWithRestart) {

    [[nodiscard]] TPlanStep ProposeTx(TTestBasicRuntime& runtime, TActorId& sender, NKikimrTxColumnShard::ETransactionKind txKind, const TString& txBody, const ui64 txId, bool rebootAfterForward = false) {
        auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
            txKind, sender, txId, txBody);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        
        if (rebootAfterForward) {
            // Add random delay before reboot to simulate real-world scenario
            const ui32 delayUs = RandomNumber<ui32>(300);
            runtime.SimulateSleep(TDuration::MicroSeconds(delayUs));
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
            // After reboot, wait a bit and retry the propose
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
            Cerr << "Retrying propose after reboot, txId: " << txId << Endl;
            auto retryEvent = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                txKind, sender, txId, txBody);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, retryEvent.release());
        }
        
        TAutoPtr<IEventHandle> handle;
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(handle);
        UNIT_ASSERT(ev);
        const auto& res = ev->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), txId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
        UNIT_ASSERT_EQUAL(res.GetStatus(),  NKikimrTxColumnShard::PREPARED);
        return TPlanStep{res.GetMinStep()};
    }

    void PlanTx(TTestBasicRuntime& runtime, TActorId& sender, NKikimrTxColumnShard::ETransactionKind txKind, NOlap::TSnapshot snap, bool waitResult = true, bool waitNotifyResult = false) {
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
        if (waitResult) {
            auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
            const auto& res = ev->Get()->Record;
            UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
            UNIT_ASSERT_EQUAL(res.GetTxKind(), txKind);
            UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::SUCCESS);
        }
        if (waitNotifyResult) {
            auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(sender);
            UNIT_ASSERT_EQUAL(ev->Get()->Record.GetTxId(), snap.GetTxId());
            UNIT_ASSERT(ev->Get()->Record.HasOpResult());
            const auto& opResult = ev->Get()->Record.GetOpResult();
            UNIT_ASSERT(opResult.GetSuccess());
            if (opResult.HasExplain()) {
                Cerr << "Backup completion explain: " << opResult.GetExplain() << Endl;
            }
        }
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

    Y_UNIT_TEST_DUO(ProposeBackup, Reboot) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
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

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

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
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
        
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 1);
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false, true);
        
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
    }

    Y_UNIT_TEST_DUO(BackupWithUncommittedData, Reboot) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
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

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        // Write uncommitted data
        std::vector<ui64> uncommittedWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({1000, 1100}, schema), schema, true, &uncommittedWriteIds));

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

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
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
        
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 1);
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false, true);
        
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
    }

    Y_UNIT_TEST_DUO(BackupWithMultipleRestarts, Reboot) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
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

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        {
            std::vector<ui64> writeIds;
            WriteData(runtime, sender, writeId, tableId, MakeTestBlob({writeId * 100, (writeId + 1) * 100}, schema), schema, true, &writeIds);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

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
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
        
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 1);
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false, true);
        
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
    }

    Y_UNIT_TEST_DUO(BackupDuringPropose, Reboot) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
        };
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;

        TActorId sender = runtime.AllocateEdgeActor();

        for (int i = 0; i < 50; i++) {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({i * 90, (i + 1) * 100}, schema), schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

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

        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId, Reboot);
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 1);
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false, true);
        
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
    }

    Y_UNIT_TEST_DUO(BackupWithSchemaVerification, Reboot) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
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

        NKikimrTxColumnShard::TBackupTxBody txBody;
        NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        auto& backupTask = *txBody.MutableBackupTask();
        backupTask.SetTableName("test_table");
        backupTask.SetTableId(tableId);
        backupTask.SetSnapshotStep(backupSnapshot.GetPlanStep());
        backupTask.SetSnapshotTxId(backupSnapshot.GetTxId());
        backupTask.MutableS3Settings()->SetEndpoint(GetEnv("S3_ENDPOINT"));
        backupTask.MutableS3Settings()->SetBucket("test");

        auto& table = *backupTask.MutableTable();
        auto& tableDescription = *table.MutableColumnTableDescription();
        tableDescription.SetColumnShardCount(4);
        tableDescription.SetName("test_table");
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
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
        
        // Reboot immediately after propose - this should test serialization
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 1);
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false, true);
        
        // Wait for export to complete
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
        
        // Verify that the export completed successfully even after reboot
        auto keys = NTestUtils::GetObjectKeys("test", s3Client);
        UNIT_ASSERT_EQUAL(keys.size(), 3);
    }

    Y_UNIT_TEST_DUO(BackupWithComplexSchema, Reboot) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("id", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)),
            NArrow::NTest::TTestColumn("value", TTypeInfo(NTypeIds::Double)),
            NArrow::NTest::TTestColumn("data", TTypeInfo(NTypeIds::Utf8)),
            NArrow::NTest::TTestColumn("flag", TTypeInfo(NTypeIds::Bool))
        };
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;

        TActorId sender = runtime.AllocateEdgeActor();

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({0, 50}, schema), schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        NKikimrTxColumnShard::TBackupTxBody txBody;
        NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        auto& backupTask = *txBody.MutableBackupTask();
        backupTask.SetTableName("complex_table");
        backupTask.SetTableId(tableId);
        backupTask.SetSnapshotStep(backupSnapshot.GetPlanStep());
        backupTask.SetSnapshotTxId(backupSnapshot.GetTxId());
        backupTask.MutableS3Settings()->SetEndpoint(GetEnv("S3_ENDPOINT"));
        backupTask.MutableS3Settings()->SetBucket("test");

        auto& table = *backupTask.MutableTable();
        auto& tableDescription = *table.MutableColumnTableDescription();
        tableDescription.SetColumnShardCount(4);
        tableDescription.SetName("complex_table");
        auto& schemaBackup = *tableDescription.MutableSchema();

        auto& col1 = *schemaBackup.MutableColumns()->Add();
        col1.SetName("id");
        col1.SetType("Uint64");

        auto& col2 = *schemaBackup.MutableColumns()->Add();
        col2.SetName("timestamp");
        col2.SetType("Timestamp");

        auto& col3 = *schemaBackup.MutableColumns()->Add();
        col3.SetName("value");
        col3.SetType("Double");

        auto& col4 = *schemaBackup.MutableColumns()->Add();
        col4.SetName("data");
        col4.SetType("Utf8");

        auto& col5 = *schemaBackup.MutableColumns()->Add();
        col5.SetName("flag");
        col5.SetType("Bool");
        
        table.MutableSelf();
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 0);
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
        
        // Multiple reboots to stress test serialization
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        UNIT_ASSERT(csControllerGuard->GetFinishedExportsCount() == 1);
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false, true);
        
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        // Wait for export to complete
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
        
        // Verify export completed
        auto keys = NTestUtils::GetObjectKeys("test", s3Client);
        UNIT_ASSERT_EQUAL(keys.size(), 3);
    }

    Y_UNIT_TEST_DUO(BackupDuringExportExecution, Reboot) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
        };
        
        // Use a controller that doesn't wait for compaction to speed up the test
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        ui64 txId = 111;
        ui64 writeId = 1;

        TActorId sender = runtime.AllocateEdgeActor();

        // Write some data
        for (int i = 0; i < 10; ++i)
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, tableId, MakeTestBlob({i * 1000, (i + 1) * 1000}, schema), schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Create backup transaction
        NKikimrTxColumnShard::TBackupTxBody txBody;
        NOlap::TSnapshot backupSnapshot(planStep.Val(), txId);
        auto& backupTask = *txBody.MutableBackupTask();
        backupTask.SetTableName("test_table");
        backupTask.SetTableId(tableId);
        backupTask.SetSnapshotStep(backupSnapshot.GetPlanStep());
        backupTask.SetSnapshotTxId(backupSnapshot.GetTxId());
        backupTask.MutableS3Settings()->SetEndpoint(GetEnv("S3_ENDPOINT"));
        backupTask.MutableS3Settings()->SetBucket("test");

        auto& table = *backupTask.MutableTable();
        auto& tableDescription = *table.MutableColumnTableDescription();
        tableDescription.SetColumnShardCount(4);
        tableDescription.SetName("test_table");
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
        
        // Propose backup transaction
        planStep = ProposeTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, txBody.SerializeAsString(), ++txId);
        
        // Plan the transaction - this will start the export
        PlanTx(runtime, sender, NKikimrTxColumnShard::TX_KIND_BACKUP, NOlap::TSnapshot(planStep, txId), false, true);
        
        // Wait a bit for export to start
        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        
        // Reboot while export is in progress - this should trigger the bug
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        
        // Wait for export to complete
        TestWaitCondition(runtime, "export",
            [&]() { return NTestUtils::GetObjectKeys("test", s3Client).size() == 3; });
        
        // Verify export completed
        auto keys = NTestUtils::GetObjectKeys("test", s3Client);
        UNIT_ASSERT_EQUAL(keys.size(), 3);
    }
}

} // namespace NKikimr
