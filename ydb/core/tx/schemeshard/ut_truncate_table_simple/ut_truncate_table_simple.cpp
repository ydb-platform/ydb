#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TruncateTable) {
    Y_UNIT_TEST(TruncateTableWithConcurrentDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        TVector<TCell> cells = {
            TCell::Make((ui64)1), TCell(TStringBuf("row one")), TCell(TStringBuf("data one")),
            TCell::Make((ui64)2), TCell(TStringBuf("row two")), TCell(TStringBuf("data two")),
            TCell::Make((ui64)3), TCell(TStringBuf("row three")), TCell(TStringBuf("data three")),
            TCell::Make((ui64)4), TCell(TStringBuf("row four")), TCell(TStringBuf("data four")),
            TCell::Make((ui64)5), TCell(TStringBuf("row five")), TCell(TStringBuf("data five")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(cells, 5, 3), true);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 5);
        }

        bool firstProposeTransactionResultHandled = false;
        const ui64 truncateTxId = ++txId;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            if (ev->GetTypeRewrite() == TEvDataShard::EvProposeTransactionResult) {
                if (!firstProposeTransactionResultHandled) {
                    firstProposeTransactionResultHandled = true;
                    
                    TestDropTable(runtime, ++txId, "/MyRoot", "TestTable",
                                                {NKikimrScheme::StatusMultipleModifications});
                    env.TestWaitNotification(runtime, txId);
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        AsyncTruncateTable(runtime, truncateTxId, "/MyRoot", "TestTable", TTestTxConfig::SchemeShard);

        env.TestWaitNotification(runtime, truncateTxId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
        
        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 0);
        }
    }

    Y_UNIT_TEST(TruncateTableWithConcurrentTruncate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        TVector<TCell> cells = {
            TCell::Make((ui64)1), TCell(TStringBuf("row one")), TCell(TStringBuf("data one")),
            TCell::Make((ui64)2), TCell(TStringBuf("row two")), TCell(TStringBuf("data two")),
            TCell::Make((ui64)3), TCell(TStringBuf("row three")), TCell(TStringBuf("data three")),
            TCell::Make((ui64)4), TCell(TStringBuf("row four")), TCell(TStringBuf("data four")),
            TCell::Make((ui64)5), TCell(TStringBuf("row five")), TCell(TStringBuf("data five")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(cells, 5, 3), true);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 5);
        }

        bool firstProposeTransactionResultHandled = false;
        const ui64 truncateTxId = ++txId;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            if (ev->GetTypeRewrite() == TEvDataShard::EvProposeTransactionResult) {
                if (!firstProposeTransactionResultHandled) {
                    firstProposeTransactionResultHandled = true;
                    
                    TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable",
                                     {NKikimrScheme::StatusMultipleModifications});
                    env.TestWaitNotification(runtime, txId);
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        AsyncTruncateTable(runtime, truncateTxId, "/MyRoot", "TestTable", TTestTxConfig::SchemeShard);

        env.TestWaitNotification(runtime, truncateTxId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
        
        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 0);
        }
    }

    Y_UNIT_TEST(TruncateTableSequentialOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        TVector<TCell> cells1 = {
            TCell::Make((ui64)1), TCell(TStringBuf("row one")), TCell(TStringBuf("data one")),
            TCell::Make((ui64)2), TCell(TStringBuf("row two")), TCell(TStringBuf("data two")),
            TCell::Make((ui64)3), TCell(TStringBuf("row three")), TCell(TStringBuf("data three")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(cells1, 3, 3), true);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 3);
        }

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        env.TestWaitNotification(runtime, txId);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 0);
        }

        TVector<TCell> cells2 = {
            TCell::Make((ui64)10), TCell(TStringBuf("row ten")), TCell(TStringBuf("data ten")),
            TCell::Make((ui64)20), TCell(TStringBuf("row twenty")), TCell(TStringBuf("data twenty")),
            TCell::Make((ui64)30), TCell(TStringBuf("row thirty")), TCell(TStringBuf("data thirty")),
            TCell::Make((ui64)40), TCell(TStringBuf("row forty")), TCell(TStringBuf("data forty")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(cells2, 4, 3), true);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 4);
        }

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        env.TestWaitNotification(runtime, txId);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 0);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
    }

    Y_UNIT_TEST(TruncateNonExistentTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);

       TestTruncateTable(runtime, ++txId, "/MyRoot", "NonExistentTable",
                        {NKikimrScheme::StatusPathDoesNotExist});
       env.TestWaitNotification(runtime, txId);
   }

   Y_UNIT_TEST(TruncateTableWithCdcStream) {
       TTestBasicRuntime runtime;
       TTestEnv env(runtime);
       ui64 txId = 100;

       runtime.GetAppData().FeatureFlags.SetEnableTruncateTable(true);

       TestCreateTable(runtime, ++txId, "/MyRoot", R"(
           Name: "TestTable"
           Columns { Name: "id" Type: "Uint64" }
           Columns { Name: "text" Type: "String" }
           Columns { Name: "data" Type: "String" }
           KeyColumnNames: [ "id" ]
       )");
       env.TestWaitNotification(runtime, txId);

       TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
           TableName: "TestTable"
           StreamDescription {
               Name: "Stream"
               Mode: ECdcStreamModeKeysOnly
               Format: ECdcStreamFormatProto
           }
       )");
       env.TestWaitNotification(runtime, txId);

       // Cannot truncate table with CDC stream
       TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable",
                        {NKikimrScheme::StatusPreconditionFailed});
       env.TestWaitNotification(runtime, txId);
   }
}
