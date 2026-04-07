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

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;


Y_UNIT_TEST_SUITE(CopyTable) {
    Y_UNIT_TEST(EmptyTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, {planStep, txId});
    }

    Y_UNIT_TEST(WithUncommittedData) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        const bool ok = WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({0, 100}, testTable.Schema), testTable.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        {
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 200 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
        }
        PlanSchemaTx(runtime, sender, {planStep, txId});
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }

    }

    Y_UNIT_TEST_DUO(WithCommitInProgress, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds1;
        const auto lock1 = 1;
        {
            const bool ok = WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true,
                &writeIds1, NEvWrite::EModificationType::Upsert, lock1);
            UNIT_ASSERT(ok);
        }
        std::vector<ui64> writeIds2;
        const auto lock2 = 2;
        {
            const bool ok = WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 200 }, testTable.Schema), testTable.Schema,
                true, &writeIds2, NEvWrite::EModificationType::Upsert, lock2);
            UNIT_ASSERT(ok);
        }
        const auto commitTxId1 = ++txId;
        planStep = ProposeCommit(runtime, sender, commitTxId1, writeIds1, lock1);
        const auto commitPlanStep = planStep;
        const ui64 dstPathId = 2;
        const auto copyTableTxId = ++txId;
        auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
            NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, copyTableTxId, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), 0, 0);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        PlanCommit(runtime, sender, commitPlanStep, commitTxId1);

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(ev);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), copyTableTxId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        planStep = TPlanStep{ res.GetMaxStep() };
        const auto copyTablePlanStep = planStep;
        UNIT_ASSERT(commitPlanStep.Val() < copyTablePlanStep.Val());

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        PlanSchemaTx(runtime, sender, { copyTablePlanStep, copyTableTxId });

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }


        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot{ copyTablePlanStep, copyTableTxId });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot{ copyTablePlanStep, copyTableTxId });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
    }

    Y_UNIT_TEST_DUO(WithData, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        const bool ok =
            WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        const auto expectedCachedCount = 1;
        UNIT_ASSERT_VALUES_EQUAL(expectedCachedCount, NOlap::TSchemaCachesManager::GetCachedOwnersCount());

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

    }

    Y_UNIT_TEST(CopyAbsentTable_Negative) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        const auto& planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);
        Y_UNUSED(planStep);

        const ui64 absentPathId = 111;
        const ui64 dstPathId = 2;
        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::CopyTableTxBody(absentPathId, dstPathId, 1), ++txId);
    }

    Y_UNIT_TEST(CopyToItself_Negative) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        const auto& planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);
        Y_UNUSED(planStep);
        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, srcPathId, 1), ++txId);
    }
}
}// namespace NKikimr