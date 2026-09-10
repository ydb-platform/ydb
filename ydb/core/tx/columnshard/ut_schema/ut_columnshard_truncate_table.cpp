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

constexpr auto TruncateTestMaxReadStaleness = TDuration::Seconds(1);

void SetupTruncateTestRuntime(TTestBasicRuntime& runtime) {
    TTester::Setup(runtime);
    // Use local scan snapshot guard so SetOverrideMaxReadStaleness controls the cleanup floor.
    runtime.GetAppData().FeatureFlags.SetEnableSnapshotsLocking(false);
}

auto RegisterTruncateTestController() {
    auto guard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    guard->SetOverrideMaxReadStaleness(TruncateTestMaxReadStaleness);
    guard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
    return guard;
}

const TColumnShard* WaitForShard(TDefaultTestsController& controller, TTestBasicRuntime& runtime) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
    while (controller.GetShardActualsCount() == 0 && TInstant::Now() < deadline) {
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_VALUES_EQUAL(controller.GetShardActualsCount(), 1);
    return controller.GetTheOnlyShard();
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
    UNIT_ASSERT(WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 1 }, testTable.Schema), testTable.Schema, true, &writeIds));
    const auto planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);
}

// Drives GC (cleanup) until PathsToDrop becomes empty. Cleanup is gated by the read staleness
// floor, so we advance the plan step between wakeups to push the safe boundary past the drop
// version of the finalized generation.
bool WaitForPathsToDropEmpty(TDefaultTestsController& controller, TTestBasicRuntime& runtime, const TActorId& sender,
    const std::function<void()>& advancePlanStep = {}, const TDuration deadline = TDuration::Seconds(60)) {
    const TInstant end = TInstant::Now() + deadline;
    while (TInstant::Now() < end) {
        Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
        // Re-evaluate snapshot usage so the read-staleness floor can advance past the drop version
        // of the finalized generation; otherwise recently-used read snapshots keep blocking GC.
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
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

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Write, truncate, then check both sides of the drop-version boundary:
    // a read strictly before the truncate snapshot still sees the old data, a read exactly at
    // the truncate snapshot sees the new (empty) generation.
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateAbsentTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(111, 1), ++txId);
    }

    // Three generations with distinct row counts. Time-travel into each live window must resolve
    // to that generation; a read exactly at a drop version is empty (old gen gone, new not yet appeared).
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
        ui32 schemaRound = 0;

        auto writeAndCommit = [&](ui64 from, ui64 to) -> NOlap::TSnapshot {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ from, to }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
            return NOlap::TSnapshot(planStep, txId);
        };
        auto truncate = [&]() -> NOlap::TSnapshot {
            planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, ++schemaRound), ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
            return NOlap::TSnapshot(planStep, txId);
        };

        const auto g0Snapshot = writeAndCommit(0, 100);
        const auto t1 = truncate();
        const auto g1Snapshot = writeAndCommit(200, 230);
        const auto t2 = truncate();
        const auto g2Snapshot = writeAndCommit(300, 320);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g0Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g1Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 30);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g2Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 20);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t1);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // TRUNCATE allocates a new InternalPathId. TTL settings must be copied onto that path
    // (SchemeShard does not resend TTL on TRUNCATE) and must actually expire rows.
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

        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT(ttl.has_value());
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            const auto& tiers = ttl->GetOrderedTiers();
            UNIT_ASSERT_VALUES_EQUAL(tiers.size(), 1);
            const auto& tier = *tiers.begin();
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictDuration(), ttlDuration);
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        shard = csController.GetTheOnlyShard();

        {
            const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(newInternalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*newInternalPathId);
            UNIT_ASSERT(ttl.has_value());
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            const auto& tiers = ttl->GetOrderedTiers();
            UNIT_ASSERT_VALUES_EQUAL(tiers.size(), 1);
            const auto& tier = *tiers.begin();
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictDuration(), ttlDuration);
        }

        const auto now = TAppData::TimeProvider->Now().Seconds();
        const auto staleTs = now - 7200;
        const auto freshTs = now - 1800;
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
        const auto dataSnapshot = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, dataSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 2);
            UNIT_ASSERT(!reader.IsError());
        }

        // TTL eviction commits at a fresh plan step, so it is invisible at dataSnapshot (MVCC).
        // Advance the plan step with an empty commit and read the latest state at that step
        // (TxId = Max<ui64>()) to observe eviction of the stale row.
        auto readLatestRowCount = [&]() -> ui64 {
            planStep = planStep + 1;
            PlanCommit(runtime, sender, planStep, TSet<ui64>{});
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, Max<ui64>()));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(reader.IsCorrectlyFinished());
            return rb ? rb->num_rows() : 0;
        };
        ui64 evictedRowCount = 0;
        csController.WaitCondition(TDuration::Seconds(30), [&] {
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
            evictedRowCount = readLatestRowCount();
            return evictedRowCount == 1;
        });
        UNIT_ASSERT_VALUES_EQUAL(evictedRowCount, 1);
    }

    // ALTER after TRUNCATE must apply to the new generation. Pre-truncate time-travel on the old
    // generation stays intact.
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = csController.GetTheOnlyShard();
        const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(newInternalPathId);
        UNIT_ASSERT(!shard->GetTablesManager().GetTableTtl(*newInternalPathId).has_value());

        auto specials = TTestSchema::TTableSpecials().SetTtl(TDuration::Seconds(3600));
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        const auto alterBody =
            TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/2, testTable.Schema, testTable.Pk, specials);
        planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = csController.GetTheOnlyShard();
        {
            const auto resolved = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(resolved);
            UNIT_ASSERT_VALUES_EQUAL(*resolved, *newInternalPathId);
            UNIT_ASSERT(shard->GetTablesManager().GetTableTtl(*resolved).has_value());
        }

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // MOVE after TRUNCATE renames the SS path on every generation, including the dropped one.
    // Time-travel therefore works on dst, not on src. Reboot must preserve that mapping.
    Y_UNIT_TEST_DUO(TruncateThenMove, Reboot) {
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto moveSnapshot = NOlap::TSnapshot(planStep, txId);

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, moveSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, moveSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, writeId++, dstPathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
    }

    // COPY after TRUNCATE aliases the new (empty) generation. The copy is pinned at its CopyVersion,
    // so later writes to the source are NOT visible on the copy.
    Y_UNIT_TEST(TruncateThenCopy) {
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto copySnapshot = NOlap::TSnapshot(planStep, txId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, copySnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, copySnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            // The copy is pinned at CopyVersion (taken right after the truncate, when the new
            // generation was still empty), so the later source write is invisible on dst.
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(dstPathId, 1), ++txId);
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(dstPathId, 1), ++txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Retention: two copies keep the old generation alive. After truncate, source latest is empty,
    // source time-travel still sees old data, copies keep old data. After reboot those reads (including
    // source MVCC) and the path-local drop version must survive; GC waits until every copy is dropped.
    Y_UNIT_TEST(TruncateCopySourceRetention) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto csControllerGuard = RegisterTruncateTestController();
        auto& csController = *csControllerGuard.operator->();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 copyPathIdA = 2;
        const ui64 copyPathIdB = 3;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathIdA, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathIdB, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        const auto* shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard);
        const auto oldInternalPathId =
            shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(copyPathIdA), false);
        UNIT_ASSERT(oldInternalPathId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdA, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdB, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdA, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        UNIT_ASSERT(CheckTableInfoV1RowExists(runtime, TTestTxConfig::TxTablet0, oldInternalPathId->GetRawValue(), srcPathId));

        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        const auto* restartedShard = WaitForShard(csController, runtime);
        UNIT_ASSERT(restartedShard);
        {
            const auto recoveredOld =
                restartedShard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(copyPathIdA), false);
            UNIT_ASSERT(recoveredOld);
            UNIT_ASSERT_VALUES_EQUAL(*recoveredOld, *oldInternalPathId);
            const auto pathDropVersion = restartedShard->GetTablesManager()
                                             .GetTable(*oldInternalPathId)
                                             .GetPathDropVersionOptional(TSchemeShardLocalPathId::FromRawValue(srcPathId));
            UNIT_ASSERT(pathDropVersion.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*pathDropVersion, truncateSnapshot);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdA, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdB, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathIdA, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdB, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        UNIT_ASSERT(restartedShard->GetTablesManager().HasTable(*oldInternalPathId, true));

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathIdB, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        UNIT_ASSERT(restartedShard->GetTablesManager().GetTable(*oldInternalPathId, true).IsDropped());

        // The old generation is pinned by read snapshots taken above; expire them immediately and
        // enable the cleanup background so GC can finalize the drop within the wait deadline.
        csControllerGuard->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        csControllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        auto advancePlanStep = [&] {
            AdvanceShardPlanStep(runtime, sender, txId, writeId, srcPathId, testTable);
        };
        UNIT_ASSERT(WaitForPathsToDropEmpty(csController, runtime, sender, advancePlanStep));

        {
            const auto* finalizedShard = csController.GetShard();
            UNIT_ASSERT(finalizedShard);
            UNIT_ASSERT(!finalizedShard->GetTablesManager().HasTable(*oldInternalPathId));
        }
        {
            // After GC finalizes the old generation, the read-staleness floor has advanced past
            // snapshotBeforeTruncate, so the time-travel read is rejected ("Snapshot too old").
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
    }

    // Second TRUNCATE of the source while a copy of the first generation is still alive.
    Y_UNIT_TEST(TruncateSourceTwiceWithLiveCopy) {
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto g0Snapshot = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto t1 = NOlap::TSnapshot(planStep, txId);

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 200, 220 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto g1Snapshot = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto t2 = NOlap::TSnapshot(planStep, txId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, g0Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, g1Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 20);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, t1);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateSourceAfterDropCopySucceeds) {
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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(dstPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 3), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
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
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 5), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 3), ++txId);
        ProposeSchemaTxFail(runtime, sender, TTestSchema::DropTableTxBody(pathId, 4), ++txId);
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 6), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
    }

    Y_UNIT_TEST_DUO(TruncateWithCommitInProgress, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        ui64 txId = 10;
        int writeId = 10;

        std::vector<ui64> writeIds;
        UNIT_ASSERT(
            WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
        const auto commitTxId = ++txId;
        const auto commitPlanStep = ProposeCommit(runtime, sender, commitTxId, writeIds);

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

        PlanCommit(runtime, sender, commitPlanStep, commitTxId);

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(ev);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(res.GetTxId(), truncateTxId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        const auto truncatePlanStep = TPlanStep{ res.GetMinStep() };
        UNIT_ASSERT(commitPlanStep.Val() < truncatePlanStep.Val());

        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        PlanSchemaTx(runtime, sender, { truncatePlanStep, truncateTxId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(truncatePlanStep, truncateTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            auto planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Completing an unrelated schema tx on another path must not unblock TRUNCATE's TWaitTxs.
    Y_UNIT_TEST(TruncateWaitTxsIgnoresUnrelatedTxCompleted) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema, 1, testTable.Standalone));

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        const auto lockId = 1;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds,
            NEvWrite::EModificationType::Upsert, lockId));
        const auto commitTxId = ++txId;
        const auto commitPlanStep = ProposeCommit(runtime, sender, commitTxId, writeIds, lockId);

        const auto truncateTxId = ++txId;
        {
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, truncateTxId, TTestSchema::TruncateTableTxBody(pathId, 1), 0, 0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        TPlanStep lastPlanStep = commitPlanStep;
        {
            constexpr ui64 auxPathId = 99;
            NKikimrTxColumnShard::TSchemaTxBody auxTx;
            Y_ABORT_UNLESS(
                auxTx.ParseFromString(TTestSchema::CreateTableTxBody(auxPathId, testTable.Standalone, testTable.Schema, testTable.Pk)));
            auxTx.MutableSeqNo()->SetRound(2);
            TString auxTxBody;
            Y_PROTOBUF_SUPPRESS_NODISCARD auxTx.SerializeToString(&auxTxBody);
            const auto auxPlan = ProposeSchemaTx(runtime, sender, auxTxBody, ++txId);
            PlanSchemaTx(runtime, sender, { auxPlan, txId });
            lastPlanStep = auxPlan;
        }

        PlanCommit(runtime, sender, TPlanStep{ lastPlanStep.Val() + 1 }, commitTxId);

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(ev);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(res.GetTxId(), truncateTxId);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        // Plan at MaxStep, not MinStep: MinStep is frozen at propose-start time, but while TRUNCATE
        // waits in TWaitTxs the test advances the plan step (the aux CreateTable tx and the in-flight
        // commit are planned at auxPlan/auxPlan+1), pushing LastPlannedStep past MinStep. A plan at
        // MinStep would be silently dropped by TTxPlanStep ("Ignore old txIds") and the test would hang.
        const auto truncatePlanStep = TPlanStep{ res.GetMaxStep() };
        PlanSchemaTx(runtime, sender, { truncatePlanStep, truncateTxId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(truncatePlanStep, truncateTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Path fence on TRUNCATE propose: uncommitted writes, new writes, and CommitWriteLock for a
    // lock that still holds the old generation must fail; after plan the table is empty.
    Y_UNIT_TEST(TruncateFencesWritesOnPropose) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        ui64 txId = 10;
        int writeId = 10;

        std::vector<ui64> uncommittedWriteIds;
        UNIT_ASSERT(WriteData(
            runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 50 }, testTable.Schema), testTable.Schema, true, &uncommittedWriteIds));

        std::vector<ui64> writeIdsBefore;
        const auto lockBefore = 1;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 50, 100 }, testTable.Schema), testTable.Schema, true,
            &writeIdsBefore, NEvWrite::EModificationType::Upsert, lockBefore));

        const auto truncateTxId = ++txId;
        {
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, truncateTxId, TTestSchema::TruncateTableTxBody(pathId, 1), 0, 0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        {
            std::vector<ui64> writeIdsAfter;
            UNIT_ASSERT(!WriteData(
                runtime, sender, writeId++, pathId, MakeTestBlob({ 100, 150 }, testTable.Schema), testTable.Schema, true, &writeIdsAfter));
        }
        ProposeCommitFail(runtime, sender, TTestTxConfig::TxTablet0, ++txId, writeIdsBefore, lockBefore);

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(ev);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(res.GetTxId(), truncateTxId);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        const auto planStep = TPlanStep{ res.GetMinStep() };
        PlanSchemaTx(runtime, sender, { planStep, truncateTxId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, truncateTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateInStoreTableFails) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema, 1, false));
        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
    }

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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 50 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const auto truncateTxId = ++txId;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), truncateTxId);
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        PlanSchemaTx(runtime, sender, { planStep, truncateTxId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, truncateTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

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
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // After TRUNCATE without copies, cleanup must finalize the old generation without crashing
    // ForgetLivePathIdVerified. Time-travel works until GC, then the old data is gone.
    Y_UNIT_TEST(TruncateThenCleanupFinalizesOldGeneration) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto csControllerGuard = RegisterTruncateTestController();
        auto& csController = *csControllerGuard.operator->();
        csControllerGuard->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        const auto* shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard);

        const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(newInternalPathId);
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
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // The old generation is pinned by the time-travel read above; expire used snapshots
        // immediately and enable the cleanup background so GC can finalize the drop.
        csControllerGuard->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        csControllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        auto advancePlanStep = [&] {
            AdvanceShardPlanStep(runtime, sender, txId, writeId, pathId, testTable);
        };
        UNIT_ASSERT(WaitForPathsToDropEmpty(csController, runtime, sender, advancePlanStep));

        {
            const auto* finalizedShard = csController.GetShard();
            UNIT_ASSERT(finalizedShard);
            const auto& tables = finalizedShard->GetTablesManager().GetTables();
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
            UNIT_ASSERT(tables.contains(*newInternalPathId));
            UNIT_ASSERT(!tables.contains(oldInternalPathId));
        }
        {
            // GC advanced the read-staleness floor past the truncate snapshot, so this read is rejected.
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
        {
            // Likewise, snapshotBeforeTruncate is below the floor after GC finalized the old generation.
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
    }
}
}   // namespace NKikimr
