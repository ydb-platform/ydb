#include <ydb/core/base/blobstorage.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>

#include <ydb/library/actors/protos/unittests.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <util/generic/algorithm.h>
#include <util/string/join.h>
#include <util/string/printf.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

namespace {

class TCopyTableDropTestController: public TDefaultTestsController {
private:
    mutable TMutex ShardMutex;
    const TColumnShard* Shard = nullptr;

public:
    void DoOnTabletInitCompleted(const TColumnShard& shard) override {
        TDefaultTestsController::DoOnTabletInitCompleted(shard);
        TGuard<TMutex> g(ShardMutex);
        Shard = &shard;
    }

    void DoOnTabletStopped(const TColumnShard& shard) override {
        TDefaultTestsController::DoOnTabletStopped(shard);
        TGuard<TMutex> g(ShardMutex);
        if (Shard == &shard) {
            Shard = nullptr;
        }
    }

    const TColumnShard* GetShard() const {
        TGuard<TMutex> g(ShardMutex);
        return Shard;
    }
};

const TColumnShard* WaitForShard(TCopyTableDropTestController& controller, TTestBasicRuntime& runtime) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
    while (controller.GetShardActualsCount() == 0 && TInstant::Now() < deadline) {
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_VALUES_EQUAL(controller.GetShardActualsCount(), 1);
    return controller.GetShard();
}

void AssertReadOnlySnapshotState(const TColumnShard& shard, const NOlap::TSnapshot& snapshot, const bool expectedPresent) {
    const auto snapshots = shard.GetTablesManager().GetReadOnlyTablesSnapshots();
    const bool present = Find(snapshots, snapshot) != snapshots.end();
    UNIT_ASSERT_VALUES_EQUAL(present, expectedPresent);
}

bool IsInPathsToDrop(const TColumnShard& shard, const TInternalPathId& pathId) {
    for (const auto& [_, pathIds] : shard.GetTablesManager().GetPathsToDrop()) {
        if (pathIds.contains(pathId)) {
            return true;
        }
    }
    return false;
}

void AssertPathsToDropState(const TColumnShard& shard, const TInternalPathId& pathId, const bool expectedPresent) {
    UNIT_ASSERT_VALUES_EQUAL(IsInPathsToDrop(shard, pathId), expectedPresent);
}

void AssertCopyPathState(const TColumnShard& shard, const ui64 copyPathId, const std::optional<NOlap::TSnapshot>& expectedCopySnapshot) {
    const auto schemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(copyPathId);
    const auto internalPathId = shard.GetTablesManager().ResolveInternalPathId(schemeShardLocalPathId, false);
    UNIT_ASSERT_VALUES_EQUAL(internalPathId.has_value(), expectedCopySnapshot.has_value());
    if (!expectedCopySnapshot) {
        UNIT_ASSERT(!shard.GetTablesManager().GetCopyVersionOptional(schemeShardLocalPathId));
        return;
    }
    UNIT_ASSERT(internalPathId);
    const auto copySnapshot = shard.GetTablesManager().GetCopyVersionOptional(schemeShardLocalPathId);
    UNIT_ASSERT(copySnapshot);
    UNIT_ASSERT_VALUES_EQUAL(*copySnapshot, *expectedCopySnapshot);
}

bool CheckTableInfoV1RowExists(TTestBasicRuntime& runtime, ui64 tabletId, ui64 internalPathId, ui64 schemeShardLocalPathId) {
    TActorId sender = runtime.AllocateEdgeActor();
    const TString query = Sprintf(R"___(
        (
            (let key '('('PathId (Uint64 '%lu)) '('SchemeShardLocalPathId (Uint64 '%lu))))
            (let select '('PathId))
            (return (AsList (SetResult 'Result (SelectRow 'TableInfoV1 key select))))
        )
    )___", internalPathId, schemeShardLocalPathId);

    auto evTx = new TEvTablet::TEvLocalMKQL;
    evTx->Record.MutableProgram()->MutableProgram()->SetText(query);
    ForwardToTablet(runtime, tabletId, sender, evTx);

    auto event = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(sender);
    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(event->Get()->Record.GetStatus(), NKikimrProto::OK);
    const auto& result = event->Get()->Record.GetExecutionEngineEvaluatedResponse();
    return result.GetValue().GetStruct(0).GetOptional().HasOptional();
}

}   // namespace

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
        PlanSchemaTx(runtime, sender, { planStep, txId });
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
        const bool ok =
            WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        {
            const bool ok = WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 200 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
        }
        PlanSchemaTx(runtime, sender, { planStep, txId });
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

    Y_UNIT_TEST_DUO(ReadOnlyTableSnapshotIsolation, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write first batch [0..100) and commit
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Copy table: src=1 -> dst=2
        const ui64 dstPathId = 2;
        const auto copyTxId = ++txId;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), copyTxId);
        const auto copyPlanStep = planStep;
        PlanSchemaTx(runtime, sender, { copyPlanStep, copyTxId });

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        // Both source and copy see 100 rows at copy snapshot
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(copyPlanStep, copyTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(copyPlanStep, copyTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        // Write second batch [100..200) to source and commit
        {
            std::vector<ui64> writeIds;
            const bool ok = WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 200 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        // Source table now sees 200 rows at the latest snapshot
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 200);
        }

        // Copy table still sees only 100 rows (pinned at copy snapshot)
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        // Write third batch [200..300) to source and commit
        {
            std::vector<ui64> writeIds;
            const bool ok = WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 200, 300 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Source sees 300 rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 300);
        }

        // Copy still sees 100 rows after multiple writes to source
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
    }

    Y_UNIT_TEST(ReadOnlyTableSnapshotIsolationMultipleCopies) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write batch [0..100) and commit
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // First copy at 100 rows
        const ui64 dstPathId1 = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId1, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Write batch [100..200) and commit
        {
            std::vector<ui64> writeIds;
            const bool ok = WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 200 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Second copy at 200 rows
        const ui64 dstPathId2 = 3;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId2, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Write batch [200..300) and commit
        {
            std::vector<ui64> writeIds;
            const bool ok = WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 200, 300 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Source sees 300 rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 300);
        }

        // First copy pinned at 100 rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId1, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        // Second copy pinned at 200 rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId2, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 200);
        }
    }

    // After DropTable on a read-only copy, its TableInfoV1 row and pinned snapshot must disappear.
    Y_UNIT_TEST(ReadOnlyTableDropRemovesTableInfoV1AndSnapshot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TCopyTableDropTestController>();
        auto& csController = *csControllerGuard.operator->();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const ui64 copyPathId1 = 2;
        const auto copyTxId1 = ++txId;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathId1, 1), copyTxId1);
        const auto copySnapshot1 = NOlap::TSnapshot(planStep, copyTxId1);
        PlanSchemaTx(runtime, sender, { planStep, copyTxId1 });

        {
            std::vector<ui64> writeIds;
            const bool ok = WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 200 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const ui64 copyPathId2 = 3;
        const auto copyTxId2 = ++txId;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathId2, 1), copyTxId2);
        const auto copySnapshot2 = NOlap::TSnapshot(planStep, copyTxId2);
        PlanSchemaTx(runtime, sender, { planStep, copyTxId2 });

        const auto* shard = WaitForShard(csController, runtime);
        const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(srcPathId), false);
        UNIT_ASSERT(internalPathId);

        AssertCopyPathState(*shard, copyPathId1, copySnapshot1);
        AssertCopyPathState(*shard, copyPathId2, copySnapshot2);
        AssertReadOnlySnapshotState(*shard, copySnapshot1, true);
        AssertReadOnlySnapshotState(*shard, copySnapshot2, true);
        UNIT_ASSERT(CheckTableInfoV1RowExists(runtime, TTestTxConfig::TxTablet0, internalPathId->GetRawValue(), copyPathId1));
        UNIT_ASSERT(CheckTableInfoV1RowExists(runtime, TTestTxConfig::TxTablet0, internalPathId->GetRawValue(), copyPathId2));

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathId1, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = WaitForShard(csController, runtime);
        AssertCopyPathState(*shard, copyPathId1, std::nullopt);
        AssertCopyPathState(*shard, copyPathId2, copySnapshot2);
        AssertReadOnlySnapshotState(*shard, copySnapshot1, false);
        AssertReadOnlySnapshotState(*shard, copySnapshot2, true);
        UNIT_ASSERT(!shard->GetTablesManager().GetCopyVersionOptional(TSchemeShardLocalPathId::FromRawValue(copyPathId1)));
        UNIT_ASSERT(!CheckTableInfoV1RowExists(runtime, TTestTxConfig::TxTablet0, internalPathId->GetRawValue(), copyPathId1));
        UNIT_ASSERT(CheckTableInfoV1RowExists(runtime, TTestTxConfig::TxTablet0, internalPathId->GetRawValue(), copyPathId2));
        AssertPathsToDropState(*shard, *internalPathId, false);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 200);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathId2, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 200);
        }
    }

    // Drop source first while read-only copies still exist, then drop copies one by one.
    // The internal path must enter PathsToDrop only after the last copy is removed, then cleanup erases it.
    Y_UNIT_TEST(DropSourceThenReadOnlyCopiesAddedToPathsToDropAndCleanedUp) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TCopyTableDropTestController>();
        auto& csController = *csControllerGuard.operator->();
        csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        csControllerGuard->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const ui64 copyPathId1 = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathId1, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        {
            std::vector<ui64> writeIds;
            const bool ok = WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 200 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const ui64 copyPathId2 = 3;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathId2, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        const auto* shard = WaitForShard(csController, runtime);
        const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(srcPathId), false);
        UNIT_ASSERT(internalPathId);
        AssertPathsToDropState(*shard, *internalPathId, false);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard->GetTablesManager().HasTable(*internalPathId));
        AssertPathsToDropState(*shard, *internalPathId, false);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathId1, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard->GetTablesManager().HasTable(*internalPathId));
        AssertPathsToDropState(*shard, *internalPathId, false);
        UNIT_ASSERT(!shard->GetTablesManager().GetCopyVersionOptional(TSchemeShardLocalPathId::FromRawValue(copyPathId1)));

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathId2, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard->GetTablesManager().HasTable(*internalPathId, /*withDeleted=*/true));
        AssertPathsToDropState(*shard, *internalPathId, true);
        UNIT_ASSERT(!shard->GetTablesManager().GetCopyVersionOptional(TSchemeShardLocalPathId::FromRawValue(copyPathId2)));

        csControllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        for (ui32 i = 0; i < 60; ++i) {
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            runtime.SimulateSleep(TDuration::Seconds(1));
            shard = WaitForShard(csController, runtime);
            if (shard->GetTablesManager().GetPathsToDrop().empty()) {
                break;
            }
        }

        shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard->GetTablesManager().GetPathsToDrop().empty());
        UNIT_ASSERT(!shard->GetTablesManager().HasTable(*internalPathId));
        UNIT_ASSERT(!shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(srcPathId), false));
    }

    // Verifies that CopyTable and DropTable use independent per-path seq_no tracking.
    Y_UNIT_TEST(CopyAndDropIndependentSeqNo) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data to srcPathId so that copy has something to work with
        std::vector<ui64> writeIds;
        {
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
        }
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        // CopyTable: src=1 -> dst=2, round=5 (high round for path 2)
        const ui64 copyDstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyDstPathId, 5), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // DropTable: path=2, round=1 (lower round than the CopyTable's round=5 for same path — should fail)
        ProposeSchemaTxFail(runtime, sender, TTestSchema::DropTableTxBody(copyDstPathId, 1), ++txId);

        // CopyTable: src=1 -> dst=3, round=1 (low round, but for a NEW path 3 — should succeed
        // because path 3 has no prior seq_no, despite path 2 having had round=5)
        const ui64 copyDstPathId2 = 3;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyDstPathId2, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Verify data is readable from both copies
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyDstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyDstPathId2, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        // DropTable on path 2 with round=6 should succeed (higher round than CopyTable's round=5)
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyDstPathId, 6), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // DropTable on path 3 with round=2 should still succeed (independent from path 2's round=6)
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyDstPathId2, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Verify source table (path 1) is still readable after all copies and drops
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
    }
}
}   // namespace NKikimr
