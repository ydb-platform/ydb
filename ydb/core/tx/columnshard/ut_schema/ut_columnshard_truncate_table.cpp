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

// Update a column in a RecordBatch to a constant value (seconds since epoch).
// Copied from ut_columnshard_schema.cpp.
std::shared_ptr<arrow::RecordBatch> UpdateColumn(std::shared_ptr<arrow::RecordBatch> batch, TString columnName, i64 seconds) {
    std::string name(columnName.c_str(), columnName.size());
    auto schema = batch->schema();
    int pos = schema->GetFieldIndex(name);
    UNIT_ASSERT(pos >= 0);
    auto colType = batch->GetColumnByName(name)->type_id();
    std::shared_ptr<arrow::Array> array;
    if (colType == arrow::Type::TIMESTAMP) {
        auto scalar = arrow::TimestampScalar(seconds * 1000 * 1000, arrow::timestamp(arrow::TimeUnit::MICRO));
        UNIT_ASSERT_VALUES_EQUAL(scalar.value, seconds * 1000 * 1000);
        auto res = arrow::MakeArrayFromScalar(scalar, batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT16) {
        TInstant date(TInstant::Seconds(seconds));
        auto res = arrow::MakeArrayFromScalar(arrow::UInt16Scalar(date.Days()), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT32) {
        auto res = arrow::MakeArrayFromScalar(arrow::UInt32Scalar(seconds), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT64) {
        auto res = arrow::MakeArrayFromScalar(arrow::UInt64Scalar(seconds), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    }
    UNIT_ASSERT(array);
    auto columns = batch->columns();
    columns[pos] = array;
    return arrow::RecordBatch::Make(schema, batch->num_rows(), columns);
}

// Controller that captures a pointer to the live TColumnShard so tests can inspect
// internal TablesManager state (PathsToDrop, AllPathIds, LivePathIds) after operations.
// Cleanup background is disabled by default so GC only runs when the test explicitly
// drives it via WaitForPathsToDropEmpty; this mirrors the copy-table cleanup tests.
class TTruncateDropTestController: public TDefaultTestsController {
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

constexpr auto TruncateTestMaxReadStaleness = TDuration::Seconds(1);

void SetupTruncateTestRuntime(TTestBasicRuntime& runtime) {
    TTester::Setup(runtime);
    // Use local scan snapshot guard so SetOverrideMaxReadStaleness controls the cleanup floor.
    runtime.GetAppData().FeatureFlags.SetEnableSnapshotsLocking(false);
}

template <typename TController>
auto RegisterTruncateTestController() {
    auto guard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TController>();
    guard->SetOverrideMaxReadStaleness(TruncateTestMaxReadStaleness);
    guard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
    return guard;
}

const TColumnShard* WaitForShard(TTruncateDropTestController& controller, TTestBasicRuntime& runtime) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
    while (controller.GetShardActualsCount() == 0 && TInstant::Now() < deadline) {
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_VALUES_EQUAL(controller.GetShardActualsCount(), 1);
    return controller.GetShard();
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

void AdvanceShardPlanStep(
    TTestBasicRuntime& runtime, TActorId& sender, ui64& txId, int& writeId, const ui64 pathId, const TestTableDescription& testTable) {
    std::vector<ui64> writeIds;
    const bool ok = WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 1 }, testTable.Schema), testTable.Schema, true, &writeIds);
    if (!ok) {
        return;
    }
    const auto planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);
}

// Drives GC (cleanup) until PathsToDrop becomes empty. Cleanup is gated by the read staleness
// floor, so we advance the plan step between wakeups to push the safe boundary past the drop
// version of the finalized generation.
bool WaitForPathsToDropEmpty(TTruncateDropTestController& controller, TTestBasicRuntime& runtime, const TActorId& sender,
    const std::function<void()>& advancePlanStep = {}, const TDuration deadline = TDuration::Seconds(60)) {
    const TInstant end = TInstant::Now() + deadline;
    while (TInstant::Now() < end) {
        Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
        if (advancePlanStep) {
            advancePlanStep();
        }
        runtime.SimulateSleep(TDuration::Seconds(1));
        Y_UNUSED(controller.WaitCleaning(TDuration::Seconds(1), &runtime));
        if (const auto* shard = controller.GetShard()) {
            if (shard->GetTablesManager().GetPathsToDrop().empty()) {
                return true;
            }
        }
    }
    return false;
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

Y_UNIT_TEST_SUITE(TruncateTable) {
    Y_UNIT_TEST(EmptyTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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
        // Truncation of an absent table is rejected at propose time with SCHEMA_ERROR.
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        const ui64 absentPathId = 111;
        ui64 txId = 10;
        // Truncation of absent table is rejected at propose time.
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(absentPathId, 1), ++txId);
    }

    Y_UNIT_TEST(MultipleTruncates) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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

    // Review gap #3: "Несколько truncate подряд — Latest snapshot да; time-travel по
    // промежуточным generation нет". The existing MultipleTruncates test only checks the
    // latest snapshot after each truncate. This test extends that scenario with time-travel
    // reads into every intermediate generation to verify that each one is still reachable
    // via a historical snapshot.
    //
    // Sequence: write g0 (100 rows) → truncate → write g1 (30 rows) → truncate →
    // write g2 (20 rows). Then read at snapshots pointing into g0, g1, and g2 and verify
    // the correct row count for each generation. Before the fix to
    // ResolveInternalPathIdForSnapshot a historical read could non-deterministically
    // resolve to the wrong (empty) generation.
    Y_UNIT_TEST(MultipleTruncatesTimeTravel) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        auto writeAndCommit = [&](ui64 from, ui64 to) -> NOlap::TSnapshot {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ from, to }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
            return NOlap::TSnapshot(planStep, txId);
        };
        auto truncate = [&]() -> NOlap::TSnapshot {
            planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
            return NOlap::TSnapshot(planStep, txId);
        };

        // Generation g0: write 100 rows.
        const auto g0Snapshot = writeAndCommit(0, 100);
        // First truncation → g0 dropped at t1.
        const auto t1 = truncate();
        // Generation g1: write 30 rows.
        const auto g1Snapshot = writeAndCommit(200, 230);
        // Second truncation → g1 dropped at t2.
        const auto t2 = truncate();
        // Generation g2: write 20 rows.
        const auto g2Snapshot = writeAndCommit(300, 320);

        // Time-travel into g0's window: g0Snapshot < t1 → must see g0's 100 rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g0Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // Time-travel into g1's window: t1 <= g1Snapshot < t2 → must see g1's 30 rows,
        // NOT g0 (dropped at t1) and NOT g2 (appeared at t2 > g1Snapshot).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g1Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 30);
            UNIT_ASSERT(!reader.IsError());
        }

        // Latest snapshot: g2 is live → must see g2's 20 rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g2Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 20);
            UNIT_ASSERT(!reader.IsError());
        }

        // Boundary: read exactly at t1 (g0's drop version). g0 is no longer visible
        // (dropVersion <= readSnapshot), g1 has not appeared yet → empty.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t1);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // Boundary: read exactly at t2 (g1's drop version). g1 is no longer visible,
        // g2 has not appeared yet → empty.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // TRUNCATE allocates a brand-new InternalPathId for the table. The TTL settings of the
    // truncated generation must be replayed onto that new path id, otherwise the table would silently
    // lose its data-lifecycle configuration (SchemeShard does not resend TTL settings on TRUNCATE).
    // Tables with tiering are rejected on SchemeShard, so this test covers pure TTL (delete action).
    //
    // This test verifies:
    //   (a) the TTL column name is preserved on the new generation;
    //   (b) the TTL duration is preserved on the new generation;
    //   (c) TTL actually expires rows on the new generation (end-to-end).
    Y_UNIT_TEST(TruncatePreservesTtl) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        csControllerGuard->SetOverrideTasksActualizationLag(TDuration::Zero());
        csControllerGuard->SetOverrideCompactionActualizationLag(TDuration::Zero());
        csControllerGuard->SetOverrideOptimizerFreshnessCheckDuration(TDuration::Zero());
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};

        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        const auto ttlDuration = TDuration::Seconds(3600);
        auto specials = TTestSchema::TTableSpecials().SetTtl(ttlDuration);
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        const auto alterBody =
            TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/1, testTable.Schema, testTable.Pk, specials);
        ui64 txId = 10;
        auto planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = csController.GetTheOnlyShard();

        // Sanity: TTL is present for the original generation with correct column and duration.
        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT_C(ttl.has_value(), "TTL settings missing on original generation");
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            const auto& tiers = ttl->GetOrderedTiers();
            UNIT_ASSERT_EQUAL(tiers.size(), 1);
            const auto& tier = *tiers.begin();
            UNIT_ASSERT_VALUES_EQUAL(tier->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT_VALUES_EQUAL(tier->GetEvictDuration(), ttlDuration);
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = csController.GetTheOnlyShard();

        // (a) + (b) After TRUNCATE the freshly generated InternalPathId must carry the same
        //           TTL column name and duration.
        {
            const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(newInternalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*newInternalPathId);
            UNIT_ASSERT_C(ttl.has_value(), "TTL settings were lost after TRUNCATE");
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            const auto& tiers = ttl->GetOrderedTiers();
            UNIT_ASSERT_EQUAL(tiers.size(), 1);
            const auto& tier = *tiers.begin();
            UNIT_ASSERT_VALUES_EQUAL(tier->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT_VALUES_EQUAL(tier->GetEvictDuration(), ttlDuration);
        }

        // (c) Write data with a TTL column value that is already stale (older than the TTL
        //     duration), then verify that TTL compaction actually deletes the rows on the
        //     new generation. This proves the TTL is not just metadata but is functional.
        {
            const auto now = TAppData::TimeProvider->Now().Seconds();
            const auto staleTs = now - 7200;   // 2 hours ago, TTL is 1 hour → stale
            const auto freshTs = now - 1800;   // 30 minutes ago, TTL is 1 hour → fresh

            // Write one stale row and one fresh row (different PKs).
            {
                std::vector<ui64> writeIds;
                const auto arrowSchema = NArrow::MakeArrowSchema(testTable.Schema);
                auto writeWithTtlTs = [&](const ui64 writeId, const std::pair<ui64, ui64> range, const i64 ts) {
                    const TString blob = MakeTestBlob(range, testTable.Schema);
                    auto batch = NArrow::DeserializeBatch(blob, arrowSchema);
                    UNIT_ASSERT(batch);
                    batch = UpdateColumn(batch, TTestSchema::DefaultTtlColumn, ts);
                    const TString data = NArrow::SerializeBatchNoCompression(batch);
                    UNIT_ASSERT(WriteData(runtime, sender, writeId, pathId, data, testTable.Schema, true, &writeIds));
                };
                writeWithTtlTs(100, { 0, 1 }, staleTs);
                writeWithTtlTs(101, { 1, 2 }, freshTs);
                planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
                PlanCommit(runtime, sender, planStep, txId);
            }

            // Before TTL compaction: both rows are visible.
            {
                TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
                reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
                auto rb = reader.ReadAll();
                UNIT_ASSERT(rb);
                UNIT_ASSERT_EQUAL(rb->num_rows(), 2);
                UNIT_ASSERT(!reader.IsError());
            }

            // Trigger TTL compaction: the stale row must be deleted, the fresh row must survive.
            csController.WaitTtl(TDuration::Seconds(30));

            {
                TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
                reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
                auto rb = reader.ReadAll();
                UNIT_ASSERT(rb);
                UNIT_ASSERT_EQUAL(rb->num_rows(), 1);
                UNIT_ASSERT(!reader.IsError());
            }
        }
    }

    // Review gap #13: "Move/alter после truncate — Нет". There was no test verifying that an
    // ALTER TABLE after TRUNCATE applies to the new generation (the freshly allocated
    // InternalPathId) and does not corrupt the old generation's time-travel visibility.
    //
    // TRUNCATE swaps the path to a brand-new InternalPathId. A subsequent ALTER must resolve
    // to that new generation and update its schema/TTL, while the old (dropped) generation
    // must remain untouched and still serve historical reads. This test:
    //   (a) writes data, truncates, then ALTERs the table to add a TTL;
    //   (b) verifies the TTL landed on the new generation, not the old one;
    //   (c) writes and reads new data after the alter to confirm the table is functional;
    //   (d) verifies a pre-truncate time-travel read still sees the old data (alter did not
    //       break MVCC on the old generation).
    Y_UNIT_TEST(TruncateThenAlter) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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

        // TRUNCATE the table → new InternalPathId generation.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = csController.GetTheOnlyShard();

        // Capture the new generation's InternalPathId (the live one after TRUNCATE).
        const auto newInternalPathId =
            shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(newInternalPathId);

        // Before the alter, the new generation has no TTL.
        UNIT_ASSERT(!shard->GetTablesManager().GetTableTtl(*newInternalPathId).has_value());

        // ALTER the table after TRUNCATE: add a TTL. This must apply to the new generation.
        auto specials = TTestSchema::TTableSpecials().SetTtl(TDuration::Seconds(3600));
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        const auto alterBody =
            TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/2, testTable.Schema, testTable.Pk, specials);
        planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = csController.GetTheOnlyShard();

        // (b) The TTL must now be present on the new generation.
        {
            const auto resolved =
                shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(resolved);
            UNIT_ASSERT_VALUES_EQUAL(*resolved, *newInternalPathId);
            UNIT_ASSERT_C(
                shard->GetTablesManager().GetTableTtl(*resolved).has_value(), "TTL was not applied to the new generation after TRUNCATE+ALTER");
        }

        // (c) Write and read new data after the alter to confirm the table is functional.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }

        // (d) Pre-truncate time-travel read still sees the old 100 rows — the alter did not
        //     touch the old generation.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // The truncate snapshot itself is still empty (new generation, pre-alter).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Review gap: "truncate + move". There was no test verifying that a MOVE (rename) after
    // TRUNCATE works correctly. TRUNCATE swaps the path to a new InternalPathId generation;
    // a subsequent MOVE must rename the new generation to the destination path, the old source
    // path must become unreadable, and a pre-truncate time-travel read on the old path must
    // still see the old data (MVCC is preserved across the rename).
    Y_UNIT_TEST(TruncateThenMove) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 dstPathId = 2;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit 100 rows.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        // TRUNCATE the table → new InternalPathId generation.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        // MOVE the table after TRUNCATE: rename srcPathId → dstPathId.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto moveSnapshot = NOlap::TSnapshot(planStep, txId);

        // The old source path is now unreadable (table was moved away).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, moveSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // The new destination path is readable (empty, since truncate emptied the table).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, moveSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // Write and read new data on the destination path to confirm the table is functional.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, dstPathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }

        // Pre-truncate time-travel read on the DESTINATION path sees the old 100 rows —
        // the move renamed the SS path on all generations (including the old dropped one),
        // so time-travel via the new path reaches the old generation.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // The old source path has no generations at all (AllPathIds[src] was moved to dst),
        // so a time-travel read on src returns empty.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

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

    // Review gap #5: "Retention: source пустой, copy жива — Latest snapshot да; MVCC source нет".
    //
    // After TRUNCATE of a source table that has a live read-only copy, the source's latest
    // snapshot is correctly empty (new generation), but a time-travel read on the SOURCE at a
    // pre-truncate snapshot must still see the old data (MVCC, same guarantee as DROP). Before
    // the fix to TruncateTableProgress retention mode, the source path was Removed from the old
    // generation immediately, so the resolver could not reach it and the MVCC read returned
    // empty — the source appeared to have no history at all.
    //
    // This test isolates that exact symptom: it does not check V1 persistence or recovery
    // (covered by TruncateCopySourceRetentionMvccAndRecovery), only the MVCC read on the source.
    Y_UNIT_TEST(TruncateCopySourceRetentionMvcc) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit 100 rows to the source.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        // Copy creates a read-only alias (dstPathId) sharing the source's InternalPathId.
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // TRUNCATE the source (retention mode: copy is alive).
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        // Latest snapshot on the source: empty (new generation). "Latest snapshot да".
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // MVCC on the source: a pre-truncate time-travel read must still see the old 100 rows.
        // Before the fix this returned empty — "MVCC source нет".
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // The copy still reads the old data at the latest snapshot (retention works).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // MVCC on the copy at the pre-truncate snapshot also sees the old data.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
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
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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

    // TRUNCATE is not supported for tables in a column store (in-store tables).
    Y_UNIT_TEST(TruncateInStoreTableFails) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        // PrepareTablet with standalone=false creates an InStore table.
        // Must pass keySize=1 explicitly since the signature is:
        // PrepareTablet(runtime, tableId, schema, keySize=1, standalone=true)
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema, 1, false);
        Y_UNUSED(planStep);
        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
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
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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

    // Review gap: "restart *after* plan". The existing TruncateSurvivesRestart test reboots
    // between propose and plan. This test reboots *after* the TRUNCATE plan completes, verifying
    // that the new generation (created by TRUNCATE) is correctly persisted to V1 and reloaded
    // after a restart. The table must still be empty (new generation), and a pre-truncate
    // time-travel read must still see the old data (old generation reloaded from V1).
    Y_UNIT_TEST(TruncateRestartAfterPlan) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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

        // TRUNCATE the table (propose + plan).
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        // Verify the table is empty after truncate (before restart).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // Restart the tablet *after* the plan completes.
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        // After restart, the table must still be empty (new generation reloaded from V1).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // Pre-truncate time-travel read must still see the old 100 rows (old generation
        // reloaded from V1).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // Write and read new data after restart to confirm the table is functional.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Regression for review issue 1: crash in TryFinalizeDropPathOnComplete after TRUNCATE.
    //
    // Before the fix, TruncateTableProgress (no copies) called DropTable, which left the old
    // generation in Tables with a drop version on the same SS path, then RegisterTable
    // overwrote LivePathIds[ss] = newInternalPathId. When GC later finalized the old
    // generation, TryFinalizeDropPathOnComplete called ForgetLivePathIdVerified(ss, oldId),
    // which AFL_VERIFY-crashed because LivePathIds[ss] already pointed to the new generation.
    //
    // This test reproduces that path: write data, truncate, then drive cleanup until the old
    // generation is finalized. Without the fix the test crashes with AFL_VERIFY. With the fix
    // the old generation is erased from Tables/AllPathIds and the live table remains.
    //
    // Time-travel semantics:
    //   - BEFORE GC (staleness window): a pre-truncate time-travel read still sees the old data
    //     (the old generation is dropped but not yet finalized).
    //   - AFTER GC: the old generation is finalized and removed; a pre-truncate time-travel
    //     read returns empty (the data is gone).
    Y_UNIT_TEST(TruncateThenCleanupFinalizesOldGeneration) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto csControllerGuard = RegisterTruncateTestController<TTruncateDropTestController>();
        auto& csController = *csControllerGuard.operator->();
        csControllerGuard->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

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

        // Truncate the table (no copies → DropTable path).
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        const auto* shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard);

        // The old generation must be in PathsToDrop (it was dropped by DropTable).
        const auto newInternalPathId =
            shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(newInternalPathId);
        // Find the old (dropped) generation: it is in Tables but not the live one.
        TInternalPathId oldInternalPathId;
        {
            const auto& tables = shard->GetTablesManager().GetTables();
            for (const auto& [internalPathId, table] : tables) {
                if (internalPathId != *newInternalPathId && table.IsDropped()) {
                    oldInternalPathId = internalPathId;
                    break;
                }
            }
            UNIT_ASSERT(oldInternalPathId.IsValid());
        }
        AssertPathsToDropState(*shard, oldInternalPathId, true);

        // BEFORE GC (staleness window): a pre-truncate time-travel read still sees the old data.
        // The old generation is dropped but not yet finalized, so it is still queryable.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // Drive GC: advance the plan step so the read-staleness floor passes the drop version,
        // then run cleanup until the old generation is finalized.
        auto advancePlanStep = [&] {
            AdvanceShardPlanStep(runtime, sender, txId, writeId, pathId, testTable);
        };
        UNIT_ASSERT(WaitForPathsToDropEmpty(csController, runtime, sender, advancePlanStep));

        // After finalization the old generation is gone from Tables; only the live (new) one remains.
        {
            const auto* finalizedShard = csController.GetShard();
            UNIT_ASSERT(finalizedShard);
            const auto& tables = finalizedShard->GetTablesManager().GetTables();
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
            UNIT_ASSERT(tables.contains(*newInternalPathId));
            UNIT_ASSERT(!tables.contains(oldInternalPathId));
        }

        // The live table is still readable (empty at the truncate snapshot).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // AFTER GC: the old generation is finalized and removed. A pre-truncate time-travel
        // read returns empty (the data is gone).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Regression for review issue 2: ResolveInternalPathIdForSnapshot selected a generation
    // non-deterministically and could resolve a historical snapshot to the wrong generation.
    //
    // Before the fix the resolver iterated a THashSet and returned the first generation whose
    // drop version covered the snapshot, treating a live generation as "always valid" without
    // checking it had already appeared at the read snapshot. After several TRUNCATEs a
    // time-travel read could non-deterministically land on the empty newest generation instead
    // of the one holding the historical data.
    //
    // This test performs three TRUNCATEs (g1 drop T1, g2 drop T2, g3 live) and verifies that a
    // snapshot taken in each generation's live window resolves to that exact generation and
    // returns the data written into it. The determinism is checked by reading the distinct
    // row-counts written into each generation.
    Y_UNIT_TEST(TruncateTimeTravelAfterMultipleTruncates) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Helper: write [from, to) and commit, return the commit snapshot.
        auto writeAndCommit = [&](ui64 from, ui64 to) -> NOlap::TSnapshot {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ from, to }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
            return NOlap::TSnapshot(planStep, txId);
        };
        auto truncate = [&]() -> NOlap::TSnapshot {
            planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
            return NOlap::TSnapshot(planStep, txId);
        };

        // Generation g1: write 10 rows.
        const auto g1Snapshot = writeAndCommit(0, 10);
        // Truncate → g1 dropped at T1.
        const auto t1 = truncate();
        // Generation g2: write 20 rows.
        const auto g2Snapshot = writeAndCommit(100, 120);
        // Truncate → g2 dropped at T2.
        const auto t2 = truncate();
        // Generation g3: write 30 rows.
        const auto g3Snapshot = writeAndCommit(200, 230);

        // Time-travel into g1's window: snapshot g1Snapshot < t1 → must see g1's 10 rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g1Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 10);
            UNIT_ASSERT(!reader.IsError());
        }

        // Time-travel into g2's window: t1 <= g2Snapshot < t2 → must see g2's 20 rows, NOT g1
        // (g1 was dropped at t1 <= g2Snapshot) and NOT g3 (g3 appeared at t2 > g2Snapshot).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g2Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 20);
            UNIT_ASSERT(!reader.IsError());
        }

        // Latest snapshot: g3 is live → must see g3's 30 rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g3Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 30);
            UNIT_ASSERT(!reader.IsError());
        }

        // Boundary: read exactly at t1 (g1's drop version). g1 is no longer visible (dropVersion
        // <= readSnapshot), g2 has not appeared yet (appearVersion = t1 > ... actually g2 appears
        // after t1). The resolver must not return g1 here.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t1);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            // At t1, g1 is dropped and g2 has not appeared → empty.
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // Boundary: read exactly at t2 (g2's drop version). g2 is no longer visible,
        // g3 has not appeared yet → empty.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Regression for review issue 3: retention mode (TRUNCATE source while a copy is alive)
    // broke MVCC on the source and left a stale entry in AllPathIds.
    //
    // Before the fix, TruncateTableProgress retention mode did SetDropVersion then immediately
    // Remove + EraseTableInfoV1 on the source path, so the resolver skipped the old generation
    // (no SS path) and ForgetGeneration was never called → stale AllPathIds[source] lived
    // forever, and time-travel on the source after copy+truncate did not work. After a restart
    // the source+old generation was not loaded from V1 at all.
    //
    // This test: copy the source, truncate the source (retention), then verify:
    //   (a) the copy still reads the old data;
    //   (b) time-travel on the SOURCE at a pre-truncate snapshot still sees the old data
    //       (MVCC, same as DROP) — this was broken before the fix;
    //   (c) the old generation's V1 row for the source path still exists (so recovery works);
    //   (d) after a restart, the old generation is reloaded from V1 and the copy still reads.
    Y_UNIT_TEST(TruncateCopySourceRetentionMvccAndRecovery) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto csControllerGuard = RegisterTruncateTestController<TTruncateDropTestController>();
        auto& csController = *csControllerGuard.operator->();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit 100 rows to the source.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        // Copy the source → dstPathId is a read-only alias sharing the source's InternalPathId.
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // TRUNCATE the source (retention mode: copy is alive).
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        const auto* shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard);

        // The old generation's InternalPathId (shared by the copy).
        const auto oldInternalPathId =
            shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(dstPathId), false);
        UNIT_ASSERT(oldInternalPathId);

        // (a) The copy still reads the old data at the latest snapshot.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // (b) MVCC on the SOURCE: a pre-truncate time-travel read must still see the old 100 rows.
        //     Before the fix the source path was Removed from the old generation, so the resolver
        //     could not reach it and the read returned empty (or resolved to the new generation).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // The source at the truncate snapshot is empty (new generation).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // (c) The old generation's V1 row for the source path must still exist (recovery).
        //     Before the fix EraseTableInfoV1 deleted it, so after restart the old generation
        //     was not loaded and the copy lost its data.
        UNIT_ASSERT(CheckTableInfoV1RowExists(runtime, TTestTxConfig::TxTablet0, oldInternalPathId->GetRawValue(), srcPathId));

        // (d) Restart the tablet and verify the old generation is reloaded from V1: the copy
        //     must still read the old 100 rows after recovery.
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        {
            const auto* restartedShard = csController.GetShard();
            UNIT_ASSERT(restartedShard);
            const auto recoveredOld =
                restartedShard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(dstPathId), false);
            UNIT_ASSERT(recoveredOld);
            UNIT_ASSERT_VALUES_EQUAL(*recoveredOld, *oldInternalPathId);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Regression for review issue 1 (second round): v0 TableInfo overwrites path-local drop version
    // during recovery in retention mode.
    //
    // Retention TRUNCATE writes the source path's drop version only to V1 (SaveTableDropVersionV1).
    // The v0 row for the old generation still contains the source SS path WITHOUT a drop version
    // (that's how the table was originally registered). On restart, InitFromDB loads V1 first
    // (source path WITH drop), then v0 (source path WITHOUT drop). TTableInfo::Merge unconditionally
    // overwrote the path info, erasing the drop version. After the fix, Merge preserves the
    // existing DropVersion when the incoming entry lacks one.
    //
    // Without the fix, after restart:
    //   - source path on old gen has no DropVersion → IsDropped() = false
    //   - old gen never enters PathsToDrop → portions and metadata leak forever
    //   - after dropping the copy, the old gen is still not GC'd
    //
    // This test: copy → truncate source (retention) → restart → verify live source empty,
    // copy reads old data, path-local drop preserved → drop copy → old gen in PathsToDrop → GC.
    Y_UNIT_TEST(TruncateRetentionRecoveryDropVersionPreserved) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto csControllerGuard = RegisterTruncateTestController<TTruncateDropTestController>();
        auto& csController = *csControllerGuard.operator->();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit 100 rows to the source.
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        // Copy the source → dstPathId is a read-only alias sharing the source's InternalPathId.
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // TRUNCATE the source (retention mode: copy is alive).
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        const auto* shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard);

        // The old generation's InternalPathId (shared by the copy).
        const auto oldInternalPathId =
            shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(dstPathId), false);
        UNIT_ASSERT(oldInternalPathId);

        // Restart the tablet.
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        const auto* restartedShard = csController.GetShard();
        UNIT_ASSERT(restartedShard);

        // (a) After restart, the live source must be empty (new generation).
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        // (b) After restart, the copy must still read the old 100 rows.
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // (c) After restart, the path-local drop version on the old generation for the source
        //     path must be preserved. Before the fix, v0 overwrote it during Merge, so
        //     GetPathDropVersionOptional returned nullopt and IsDropped() was false.
        {
            const auto& tablesManager = restartedShard->GetTablesManager();
            const auto& table = tablesManager.GetTable(*oldInternalPathId);
            const auto pathDropVersion = table.GetPathDropVersionOptional(TSchemeShardLocalPathId::FromRawValue(srcPathId));
            UNIT_ASSERT(pathDropVersion.has_value())
                << "Path-local drop version lost after restart (v0 overwrote V1 during Merge)";
            UNIT_ASSERT_VALUES_EQUAL(*pathDropVersion, truncateSnapshot);
        }

        // (d) After restart, the old generation must be in PathsToDrop (table-level IsDropped
        //     is true because both source and copy paths have drop versions... wait, the copy
        //     path does NOT have a drop version yet — only the source does. So IsDropped() is
        //     false until the copy is dropped. The old gen enters PathsToDrop only after
        //     dropping the copy.)
        //
        // Drop the copy.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(dstPathId, 3), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // (e) After dropping the copy, the old generation must be in PathsToDrop.
        {
            const auto& tablesManager = restartedShard->GetTablesManager();
            const auto& table = tablesManager.GetTable(*oldInternalPathId);
            UNIT_ASSERT(table.IsDropped())
                << "Old generation not fully dropped after copy drop (path-local drop was lost)";
        }

        // (f) Drive GC: the old generation must be finalized and removed.
        UNIT_ASSERT(WaitForPathsToDropEmpty(csController, runtime, sender));

        // (g) After GC, the old generation is gone from Tables.
        {
            const auto& tablesManager = restartedShard->GetTablesManager();
            UNIT_ASSERT(!tablesManager.HasTable(*oldInternalPathId))
                << "Old generation not finalized by GC after drop copy";
        }
    }
}
}   // namespace NKikimr
