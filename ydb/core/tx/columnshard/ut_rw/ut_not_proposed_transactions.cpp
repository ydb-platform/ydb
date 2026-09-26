#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

namespace NKikimr::NColumnShard {

using namespace Tests;
using namespace NTxUT;

namespace {

constexpr ui64 TableId = 1;
constexpr ui64 LockId = 222;
constexpr ui64 TxId = 111;
constexpr ui64 DropTxId = 200;

class TShardFixture {
public:
    NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> Controller =
        NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;
    TShardWriter Writer;

    TShardFixture()
        : Sender(Setup(Runtime))
        , Writer(Runtime, TTestTxConfig::TxTablet0, TableId, LockId)
    {
    }

    const TColumnShard& Shard() {
        return *Controller->GetTheOnlyShard();
    }

    NKikimrDataEvents::TLock WriteUnderLock() {
        const auto batch = MakeTestBatch<arrow::UInt64Type>({ "key" }, std::vector<ui64>{ 1, 2, 3 });
        const auto result = Writer.WriteWithResult(batch, { 1 }, TxId);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(result.TxLocksSize(), 1);
        return result.GetTxLocks(0);
    }

    ui64 CountOperations() {
        return CountLocalDbTableRows(Runtime, TTestTxConfig::TxTablet0, "Operations", "'('('WriteId (Null) (Void)))", "'('WriteId)");
    }

    ui64 CountInsertedPortions() {
        const auto& tables = Shard().GetIndexAs<NOlap::TColumnEngineForLogs>().GetTables();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        return tables.begin()->second->GetInsertedPortions().size();
    }

    void RebootAndWaitTransactionsAborted() {
        RebootTablet(Runtime, TTestTxConfig::TxTablet0, Sender);
        for (ui32 attempt = 0; CountOperations() && attempt < 100; ++attempt) {
            Runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(CountOperations(), 0);
    }

    TInternalPathId DropTable() {
        Y_UNUSED(SetupSchema(Runtime, Sender, TTestSchema::DropTableTxBody(TableId, 2), DropTxId));
        return *Shard().GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(TableId), false);
    }

    bool IsPendingDrop(const TInternalPathId pathId) {
        for (const auto& [_, pathIds] : Shard().GetTablesManager().GetPathsToDrop()) {
            if (pathIds.contains(pathId)) {
                return true;
            }
        }
        return false;
    }

    bool HasTable(const TInternalPathId pathId) {
        return Shard().GetTablesManager().HasTable(pathId, true);
    }

    void PassReadWindow() {
        const auto& longTx = Runtime.GetAppData(0).LongTxServiceConfig;
        const auto registryLag =
            TDuration::Seconds(longTx.GetLocalSnapshotPromotionTimeSeconds()) + TDuration::MilliSeconds(longTx.GetMaxClockSkewMs());
        Runtime.SimulateSleep(registryLag + TDuration::Seconds(1));
        PlanCommit(Runtime, Sender, TPlanStep{ Runtime.GetCurrentTime().MilliSeconds() }, TSet<ui64>{});
    }

    void TryCleanupTables(const TInternalPathId pathId, const ui32 attempts) {
        for (ui32 attempt = 0; attempt < attempts && IsPendingDrop(pathId); ++attempt) {
            PublishEmptySnapshotRegistry();
            Wakeup(Runtime, Sender, TTestTxConfig::TxTablet0);
            Runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }
    }

private:
    static TActorId Setup(TTestBasicRuntime& runtime) {
        TTester::Setup(runtime);
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn(
            "key", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)) };
        Y_UNUSED(PrepareTablet(runtime, TableId, schema));
        return runtime.AllocateEdgeActor();
    }

    void PublishEmptySnapshotRegistry() {
        auto registryBuilder = CreateImmutableSnapshotRegistryBuilder();
        registryBuilder->SetOldestCollectionTime(Runtime.GetCurrentTime());
        Runtime.GetAppData(0).SnapshotRegistryHolder->Set(std::move(*registryBuilder).Build());
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardNotProposedTransactions) {
    Y_UNIT_TEST(AbortedOnRestart) {
        TShardFixture shard;
        Y_UNUSED(shard.WriteUnderLock());
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperations(), 1);

        shard.RebootAndWaitTransactionsAborted();

        UNIT_ASSERT_VALUES_EQUAL(shard.CountInsertedPortions(), 0);
    }

    Y_UNIT_TEST(RollbackAfterRestartIsAccepted) {
        TShardFixture shard;
        Y_UNUSED(shard.WriteUnderLock());
        shard.RebootAndWaitTransactionsAborted();

        UNIT_ASSERT_VALUES_EQUAL(shard.Writer.Abort(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(CommitAfterRestartIsRefused) {
        TShardFixture shard;
        const auto lock = shard.WriteUnderLock();
        shard.RebootAndWaitTransactionsAborted();

        UNIT_ASSERT_VALUES_EQUAL(shard.Writer.StartCommitWithLock(TxId, lock), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(WriteAfterRestartCannotCommitWithOldLock) {
        TShardFixture shard;
        const auto lockBeforeRestart = shard.WriteUnderLock();
        shard.RebootAndWaitTransactionsAborted();

        const auto lockAfterRestart = shard.WriteUnderLock();
        UNIT_ASSERT_VALUES_UNEQUAL(lockAfterRestart.GetGeneration(), lockBeforeRestart.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(
            shard.Writer.StartCommitWithLock(TxId, lockBeforeRestart), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(DroppedTableKeptUntilAbortOnRestart) {
        TShardFixture shard;
        Y_UNUSED(shard.WriteUnderLock());
        const auto pathId = shard.DropTable();
        UNIT_ASSERT(shard.IsPendingDrop(pathId));

        shard.PassReadWindow();
        shard.TryCleanupTables(pathId, 10);
        UNIT_ASSERT(shard.IsPendingDrop(pathId));
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperations(), 1);

        shard.RebootAndWaitTransactionsAborted();

        shard.PassReadWindow();
        shard.TryCleanupTables(pathId, 60);
        UNIT_ASSERT(!shard.IsPendingDrop(pathId));
        UNIT_ASSERT(!shard.HasTable(pathId));
    }
}

}   // namespace NKikimr::NColumnShard
