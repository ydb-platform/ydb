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

    Y_UNIT_TEST(TruncateTableWithIndexAndCdcStream) {
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

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/TestTable",
            TBuildIndexConfig{"TestIndex", NKikimrSchemeOp::EIndexTypeGlobal, {"text"}, {}, {}});
        env.TestWaitNotification(runtime, txId);

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "TestTable"
            StreamDescription {
                Name: "TestStream"
                Mode: ECdcStreamModeKeysOnly
                Format: ECdcStreamFormatProto
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TestIndex"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TestIndex/indexImplTable"),
            {NLs::PathExist});

        TVector<TCell> cells = {
            TCell::Make((ui64)1), TCell(TStringBuf("row one")), TCell(TStringBuf("data one")),
            TCell::Make((ui64)2), TCell(TStringBuf("row two")), TCell(TStringBuf("data two")),
            TCell::Make((ui64)3), TCell(TStringBuf("row three")), TCell(TStringBuf("data three")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(cells, 3, 3), true);

        TVector<TCell> indexCells = {
            TCell(TStringBuf("row one")), TCell::Make((ui64)1),
            TCell(TStringBuf("row two")), TCell::Make((ui64)2),
            TCell(TStringBuf("row three")), TCell::Make((ui64)3),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable/TestIndex/indexImplTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(indexCells, 3, 2), true);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 3);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TestIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 3);
        }

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable",
                         {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 3);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TestIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 3);
        }

        TestDropCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "TestTable"
            StreamName: "TestStream"
        )");
        env.TestWaitNotification(runtime, txId);

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        env.TestWaitNotification(runtime, txId);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 0);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TestIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 0);
        }

        TVector<TCell> newCells = {
            TCell::Make((ui64)10), TCell(TStringBuf("new row")), TCell(TStringBuf("new data")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(newCells, 1, 3), true);

        TVector<TCell> newIndexCells = {
            TCell(TStringBuf("new row")), TCell::Make((ui64)10),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable/TestIndex/indexImplTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(newIndexCells, 1, 2), true);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 1);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TestIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 1);
        }
    }

    void TruncateTableWithIndex(NKikimrSchemeOp::EIndexType indexType) {
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

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/TestTable",
            TBuildIndexConfig{"TextIndex", indexType, {"text"}, {}, {}});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex/indexImplTable"),
            {NLs::PathExist});

        TVector<TCell> mainTableCells = {
            TCell::Make((ui64)1), TCell(TStringBuf("hello")), TCell(TStringBuf("data one")),
            TCell::Make((ui64)2), TCell(TStringBuf("world")), TCell(TStringBuf("data two")),
            TCell::Make((ui64)3), TCell(TStringBuf("test")), TCell(TStringBuf("data three")),
            TCell::Make((ui64)4), TCell(TStringBuf("index")), TCell(TStringBuf("data four")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(mainTableCells, 4, 3), true);

        TVector<TCell> indexTableCells = {
            TCell(TStringBuf("hello")), TCell::Make((ui64)1),
            TCell(TStringBuf("world")), TCell::Make((ui64)2),
            TCell(TStringBuf("test")), TCell::Make((ui64)3),
            TCell(TStringBuf("index")), TCell::Make((ui64)4),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable/TextIndex/indexImplTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(indexTableCells, 4, 2), true);

        {
            auto mainRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(mainRows, 4);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 4);
        }

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        env.TestWaitNotification(runtime, txId);

        {
            auto mainRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(mainRows, 0);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 0);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex/indexImplTable"),
            {NLs::PathExist});

        TVector<TCell> newMainTableCells = {
            TCell::Make((ui64)10), TCell(TStringBuf("new")), TCell(TStringBuf("new data one")),
            TCell::Make((ui64)20), TCell(TStringBuf("fresh")), TCell(TStringBuf("new data two")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(newMainTableCells, 2, 3), true);

        TVector<TCell> newIndexTableCells = {
            TCell(TStringBuf("new")), TCell::Make((ui64)10),
            TCell(TStringBuf("fresh")), TCell::Make((ui64)20),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable/TextIndex/indexImplTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(newIndexTableCells, 2, 2), true);

        {
            auto mainRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(mainRows, 2);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 2);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex/indexImplTable"),
            {NLs::PathExist});
    }

    Y_UNIT_TEST(TruncateTableWithSecondaryIndex) {
        TruncateTableWithIndex(NKikimrSchemeOp::EIndexTypeGlobal);   
    }

    Y_UNIT_TEST(TruncateTableWithUniqueIndex) {
        TruncateTableWithIndex(NKikimrSchemeOp::EIndexTypeGlobalUnique);   
    }
}

