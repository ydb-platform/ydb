#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>

namespace NKikimr::NColumnShard {

using namespace Tests;
using namespace NTxUT;

namespace {

constexpr ui64 TableId = 1;
constexpr ui64 LockId = 222;
constexpr ui64 TxId = 111;

void PrepareShard(TTestBasicRuntime& runtime) {
    TTester::Setup(runtime);
    const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)) };
    Y_UNUSED(PrepareTablet(runtime, TableId, schema));
}

NKikimrDataEvents::TLock WriteUnderLock(TShardWriter& writer) {
    const auto batch = MakeTestBatch<arrow::UInt64Type>({ "key" }, std::vector<ui64>{ 1, 2, 3 });
    const auto result = writer.WriteWithResult(batch, { 1 }, TxId);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    UNIT_ASSERT_VALUES_EQUAL(result.TxLocksSize(), 1);
    return result.GetTxLocks(0);
}

ui64 CountOperations(TTestBasicRuntime& runtime) {
    return CountLocalDbTableRows(runtime, TTestTxConfig::TxTablet0, "Operations", "'('('WriteId (Null) (Void)))", "'('WriteId)");
}

void RebootAndWaitTransactionsAborted(TTestBasicRuntime& runtime, const TActorId& sender) {
    RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    for (ui32 attempt = 0; CountOperations(runtime) && attempt < 100; ++attempt) {
        runtime.SimulateSleep(TDuration::MilliSeconds(10));
    }
    UNIT_ASSERT_VALUES_EQUAL(CountOperations(runtime), 0);
}

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardNotProposedTransactions) {
    Y_UNIT_TEST(AbortedOnRestart) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        PrepareShard(runtime);
        TShardWriter writer(runtime, TTestTxConfig::TxTablet0, TableId, LockId);
        Y_UNUSED(WriteUnderLock(writer));
        UNIT_ASSERT_VALUES_EQUAL(CountOperations(runtime), 1);

        RebootAndWaitTransactionsAborted(runtime, writer.GetSender());

        const auto& tables = controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>().GetTables();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tables.begin()->second->GetInsertedPortions().size(), 0);
    }

    Y_UNIT_TEST(RollbackAfterRestartIsAccepted) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        PrepareShard(runtime);
        TShardWriter writer(runtime, TTestTxConfig::TxTablet0, TableId, LockId);
        Y_UNUSED(WriteUnderLock(writer));
        RebootAndWaitTransactionsAborted(runtime, writer.GetSender());

        UNIT_ASSERT_VALUES_EQUAL(writer.Abort(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(CommitAfterRestartIsRefused) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        PrepareShard(runtime);
        TShardWriter writer(runtime, TTestTxConfig::TxTablet0, TableId, LockId);
        const auto lock = WriteUnderLock(writer);
        RebootAndWaitTransactionsAborted(runtime, writer.GetSender());

        UNIT_ASSERT_VALUES_EQUAL(writer.StartCommitWithLock(TxId, lock), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(WriteAfterRestartCannotCommitWithOldLock) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        PrepareShard(runtime);
        TShardWriter writer(runtime, TTestTxConfig::TxTablet0, TableId, LockId);
        const auto lockBeforeRestart = WriteUnderLock(writer);
        RebootAndWaitTransactionsAborted(runtime, writer.GetSender());

        const auto lockAfterRestart = WriteUnderLock(writer);
        UNIT_ASSERT_VALUES_UNEQUAL(lockAfterRestart.GetGeneration(), lockBeforeRestart.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(writer.StartCommitWithLock(TxId, lockBeforeRestart), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
    }
}

}   // namespace NKikimr::NColumnShard
