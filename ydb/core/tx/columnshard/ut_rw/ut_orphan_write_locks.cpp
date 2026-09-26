#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>
#include <ydb/core/tx/long_tx_service/long_tx_service.h>

namespace NKikimr::NColumnShard {

Y_UNIT_TEST_SUITE(TColumnShardOrphanWriteLocks) {
    Y_UNIT_TEST(RestoredSubscriptionAbortsAfterOwnerDies) {
        using namespace Tests;
        using namespace NTxUT;
        using TLongTxEvents = NLongTxService::TEvLongTxService;

        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        constexpr ui64 tableId = 1;
        constexpr ui64 lockId = 222;
        constexpr ui64 txId = 111;
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn(
            "key", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)) };
        Y_UNUSED(PrepareTablet(runtime, tableId, schema));
        TShardWriter writer(runtime, TTestTxConfig::TxTablet0, tableId, lockId);
        writer.SetLockNodeId(runtime.GetNodeId());

        const auto service = NLongTxService::MakeLongTxServiceID(runtime.GetNodeId());
        runtime.RegisterService(service, runtime.Register(NLongTxService::CreateLongTxService()));
        runtime.Send(
            new IEventHandle(service, writer.GetSender(), new TLongTxEvents::TEvRegisterLock(lockId, runtime.GetTimeProvider()->Now())));
        runtime.Send(new IEventHandle(service, writer.GetSender(), new TLongTxEvents::TEvSubscribeLock(lockId, runtime.GetNodeId())));
        auto status = runtime.GrabEdgeEvent<TLongTxEvents::TEvLockStatus>(writer.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED);

        const auto batch = MakeTestBatch<arrow::UInt64Type>({ "key" }, std::vector<ui64>{ 1, 2, 3 });
        UNIT_ASSERT_VALUES_EQUAL(writer.Write(batch, { 1 }, txId), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

        const auto countOperations = [&] {
            return CountLocalDbTableRows(runtime, TTestTxConfig::TxTablet0, "Operations", "'('('WriteId (Null) (Void)))", "'('WriteId)");
        };
        RebootTablet(runtime, TTestTxConfig::TxTablet0, writer.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(countOperations(), 1);

        runtime.Send(new IEventHandle(service, writer.GetSender(), new TLongTxEvents::TEvUnregisterLock(lockId)));
        status = runtime.GrabEdgeEvent<TLongTxEvents::TEvLockStatus>(writer.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetStatus(), NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND);
        for (ui32 attempt = 0; countOperations() && attempt < 100; ++attempt) {
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_VALUES_EQUAL(countOperations(), 0);
        const auto& tables = controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>().GetTables();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tables.begin()->second->GetInsertedPortions().size(), 0);
    }
}

}   // namespace NKikimr::NColumnShard
