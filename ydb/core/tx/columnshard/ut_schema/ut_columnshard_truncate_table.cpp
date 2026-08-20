#include <ydb/core/base/blobstorage.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
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

// Captures a pointer to the live TColumnShard so tests can inspect TablesManager state directly.
class TShardCapturingController: public TDefaultTestsController {
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

const TColumnShard* WaitForShard(TShardCapturingController& controller, TTestBasicRuntime& runtime) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
    while (!controller.GetShard() && TInstant::Now() < deadline) {
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
    }
    const auto* shard = controller.GetShard();
    UNIT_ASSERT(shard);
    return shard;
}

}   // namespace

Y_UNIT_TEST_SUITE(TruncateTable) {
    Y_UNIT_TEST(EmptyTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // After truncation, reading should return no data
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    Y_UNIT_TEST(WithData) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data
        std::vector<ui64> writeIds;
        const bool ok =
            WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Verify data is readable before truncation
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        // Truncate the table
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Reading at a pre-truncate snapshot must still observe the pre-truncate data: TRUNCATE gives
        // the same time-travel MVCC guarantee as DROP.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // After truncation, reading at truncate snapshot should return no data
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateAndInsert) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit initial data (100 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        // Truncate the table
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Write and commit new data (50 rows) after truncation
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // After truncation + insert, reading at the latest snapshot should see only the new 50 rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 50);
        }

        // The two generations coexist: a time-travel read at the pre-truncate snapshot still observes
        // the original 100 rows even though the live table now holds only the 50 post-truncate rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
    }

    Y_UNIT_TEST(TruncateAbsentTable) {
        // Truncation of an absent table is a no-op at the column shard level.
        // The proposal succeeds (PREPARED), and at plan time the truncation is silently skipped.
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        const ui64 absentPathId = 111;
        ui64 txId = 10;
        // Truncation of absent table succeeds at propose time (PREPARED) but is a no-op at plan time.
        // ProposeSchemaTx already asserts the result is non-null, which implies PREPARED status.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(absentPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Original table should still be readable (empty, since no data was written)
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    Y_UNIT_TEST(MultipleTruncates) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data (100 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // First truncation
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Write new data (30 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 230 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Verify 30 rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 30);
        }

        // Second truncation
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // After second truncation, should be empty
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }

        // Write data again (20 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 300, 320 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Should see only the 20 new rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 20);
        }
    }

    // TRUNCATE allocates a brand-new InternalPathId for the table. The TTL settings of the
    // truncated generation must be replayed onto that new path id, otherwise the table would silently
    // lose its data-lifecycle configuration (SchemeShard does not resend TTL settings on TRUNCATE).
    // Tables with tiering are rejected on SchemeShard, so this test covers pure TTL (delete action).
    Y_UNIT_TEST(TruncatePreservesTtl) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TShardCapturingController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        testTable.InStore = false;

        // Create a standalone table WITH TTL enabled on the default ttl column.
        auto specials = TTestSchema::TTableSpecials().SetTtl(TDuration::Seconds(3600));
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        const auto initBody = TTestSchema::CreateStandaloneTableTxBody(pathId, testTable.Schema, testTable.Pk, specials);
        auto planStep = PrepareTablet(runtime, initBody);
        Y_UNUSED(planStep);

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = WaitForShard(csController, runtime);

        // Sanity: TTL is present for the original generation.
        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            UNIT_ASSERT(shard->GetTablesManager().GetTableTtl(*internalPathId).has_value());
        }

        ui64 txId = 100;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = WaitForShard(csController, runtime);

        // After TRUNCATE the freshly generated InternalPathId must still carry the TTL settings.
        {
            const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(newInternalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*newInternalPathId);
            UNIT_ASSERT_C(ttl.has_value(), "TTL settings were lost after TRUNCATE");
        }
    }

    // Pins the MVCC boundary semantics of TRUNCATE: a read exactly at the truncate snapshot sees the
    // post-truncate (empty) generation, while a read strictly before it still sees the old data. This
    // guards ResolveInternalPathIdForSnapshot's `dropVersion <= readSnapshot` boundary condition.
    Y_UNIT_TEST(TruncateSnapshotBoundary) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit 100 rows.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        // Truncate the table.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        // Read strictly before the truncate snapshot: the old generation is still visible (100 rows).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // Read exactly AT the truncate snapshot: the drop version equals the read snapshot, so the old
        // generation is no longer visible and the (empty) new generation is selected.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateAndDrop) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Truncate the table
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Drop the table after truncation
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Reading from a dropped table should return no data
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    // TRUNCATE of a read-only table (created via CopyTable) must be rejected at propose time.
    // Implementation check: table.IsReadOnly(schemeShardLocalPathId) in schema.cpp.
    // The RO flag is set per SchemeShardLocalPathId when CopyTable registers the destination
    // path pointing to the source's InternalPathId.
    Y_UNIT_TEST(TruncateReadOnlyTableFails) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data to the source table.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Copy the table: the destination (dstPathId) becomes a read-only alias of the source.
        // Both paths share the same InternalPathId, but dstPathId has IsReadOnly=true.
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // TRUNCATE of the read-only copy must be rejected at propose time.
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(dstPathId, 1), ++txId);

        // The read-only copy must remain intact and readable.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
    }

    // TRUNCATE of the source table (from which a read-only copy was made) succeeds with retention:
    // the old generation is kept alive for the copy, and a new generation is allocated for the
    // source. The copy continues to read the old data, while the source gets a fresh (empty) table.
    Y_UNIT_TEST(TruncateCopySourceRetention) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema, 1, true);

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

        // Copy creates a read-only alias (dstPathId) sharing the source's InternalPathId.
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // TRUNCATE of the source succeeds with retention: old generation kept for copy.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Source table is now empty (new generation). Empty tables return nullptr from ReadAll.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        // Copy still reads the old data (retained generation).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
    }

    // After dropping the read-only copy, the source table reverts to a single-path table and
    // becomes truncatable again. This verifies that the GetPathIds().size() > 1 check is
    // dynamically evaluated, not cached.
    Y_UNIT_TEST(TruncateSourceAfterDropCopySucceeds) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data to the source table.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Copy the table.
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Drop the read-only copy.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(dstPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // After the copy is dropped, the source has only one path ID again and can be truncated.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 3), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Source table is now empty after truncate.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }

        // The dropped copy is no longer readable.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    Y_UNIT_TEST(TruncateSeqNoCheck) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;

        // Truncate with round=5
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 5), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Truncate with round=3 (lower) should fail
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 3), ++txId);

        // Drop on the same path with a lower per-path SeqNo must also fail (Truncate is path-scoped).
        ProposeSchemaTxFail(runtime, sender, TTestSchema::DropTableTxBody(pathId, 4), ++txId);

        // Truncate with round=6 (higher) should succeed
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 6), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
    }

    // Verifies that TRUNCATE waits for all in-flight write transactions to complete before
    // becoming PREPARED (analogous to MoveTable::WithCommitInProgress). Combined with
    // TruncateTablePropose (path fence), this prevents a concurrent write from committing into
    // the old InternalPathId after the generation swap.
    Y_UNIT_TEST_DUO(TruncateWithCommitInProgress, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Write 100 rows and propose a commit but do NOT plan it yet — this creates an in-flight tx.
        std::vector<ui64> writeIds;
        {
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
        }
        const auto commitTxId = ++txId;
        planStep = ProposeCommit(runtime, sender, commitTxId, writeIds);
        const auto commitPlanStep = planStep;

        // Send TRUNCATE propose asynchronously (without waiting for PREPARED).
        // Because an in-flight write tx exists, TWaitTxs must defer the PREPARED reply until
        // that write tx completes.
        const auto truncateTxId = ++txId;
        {
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, truncateTxId, TTestSchema::TruncateTableTxBody(pathId, 1), 0, 0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        }

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        // Complete the write commit — this should unblock the TRUNCATE propose.
        PlanCommit(runtime, sender, commitPlanStep, commitTxId);

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        // Now the TRUNCATE propose should have completed with PREPARED.
        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(ev);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), truncateTxId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        planStep = TPlanStep{ res.GetMinStep() };
        const auto truncatePlanStep = planStep;
        // TRUNCATE must be planned after the commit that preceded it.
        UNIT_ASSERT(commitPlanStep.Val() < truncatePlanStep.Val());

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        // Apply TRUNCATE on plan.
        PlanSchemaTx(runtime, sender, { truncatePlanStep, truncateTxId });

        // After TRUNCATE the table must be empty — the 100 committed rows are gone.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot{ truncatePlanStep, truncateTxId });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // Write and commit new data after TRUNCATE; only these rows must be visible.
        {
            std::vector<ui64> newWriteIds;
            const bool ok = WriteData(
                runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &newWriteIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, newWriteIds);
            PlanCommit(runtime, sender, planStep, txId);

            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot{ planStep, txId });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 50);
        }
    }

    // Path fence on TRUNCATE propose (TruncateTablePropose): new writes and CommitWriteLock for
    // locks that still hold the old generation must fail with "unknown table", same as Move.
    Y_UNIT_TEST(TruncateFencesWritesOnPropose) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 10;

        // Lock-held write that resolved the path before TRUNCATE propose.
        std::vector<ui64> writeIdsBefore;
        const auto lockBefore = 1;
        {
            const bool ok = WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 50 }, testTable.Schema), testTable.Schema, true,
                &writeIdsBefore, NEvWrite::EModificationType::Upsert, lockBefore);
            UNIT_ASSERT(ok);
        }

        // Start TRUNCATE propose asynchronously — TruncateTablePropose fences the path immediately.
        const auto truncateTxId = ++txId;
        {
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, truncateTxId, TTestSchema::TruncateTableTxBody(pathId, 1), 0, 0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        // New write after fence must fail.
        {
            std::vector<ui64> writeIdsAfter;
            const bool ok = WriteData(
                runtime, sender, writeId++, pathId, MakeTestBlob({ 50, 100 }, testTable.Schema), testTable.Schema, true, &writeIdsAfter);
            UNIT_ASSERT(!ok);
        }

        // Commit of the pre-fence lock must also fail (CommitWriteLock checks ResolveInternalPathId).
        { ProposeCommitFail(runtime, sender, TTestTxConfig::TxTablet0, ++txId, writeIdsBefore, lockBefore); }

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(ev);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), truncateTxId);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        planStep = TPlanStep{ res.GetMinStep() };
        PlanSchemaTx(runtime, sender, { planStep, truncateTxId });

        // After TRUNCATE the table is empty.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot{ planStep, truncateTxId });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    // TRUNCATE is only supported for standalone column tables. A table that belongs to a column
    // store must be rejected at propose time on the column shard side (the SchemeShard operation
    // enforces the same restriction). PrepareTablet creates an in-store table (schema preset id=1).
    Y_UNIT_TEST(TruncateInStoreTableFails) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, false);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data to the in-store table.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // TRUNCATE of an in-store column table must be rejected at propose time.
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);

        // The in-store table must remain intact and readable with all 100 rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }
    }

    // After restart mid-truncate (between propose and plan), the schema tx re-fences the path
    // via DoOnTabletInit and eventually plans the truncate. The table ends up empty.
    Y_UNIT_TEST(TruncateSurvivesRestart) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, true);

        ui64 txId = 10;
        int writeId = 1;

        // Write some data.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 50 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            auto commitPlan = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, commitPlan, txId);
        }

        // Propose TRUNCATE but don't plan yet. ProposeSchemaTx grabs the PREPARED reply and
        // returns the plan step. The PREPARED state is persisted to the database, so it survives
        // a tablet restart.
        const auto truncateTxId = ++txId;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), truncateTxId);

        // Restart the tablet (simulates crash between propose and plan).
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        // After restart, plan the truncate. The PREPARED schema tx is replayed via DoOnTabletInit,
        // so the plan should succeed and the table should end up empty.
        PlanSchemaTx(runtime, sender, { planStep, truncateTxId });

        // Table should be empty after truncate completes.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot{ planStep, truncateTxId });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }
}
}   // namespace NKikimr
