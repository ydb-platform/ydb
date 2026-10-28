#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/olap_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TruncateTable) {

    Y_UNIT_TEST(TruncateNonTableFails) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        TestMkDir(runtime, ++txId, "/MyRoot", "NotATable");
        env.TestWaitNotification(runtime, txId);

        TestTruncateTable(runtime, ++txId, "/MyRoot", "NotATable",
            {{NKikimrScheme::StatusPreconditionFailed,
                "TRUNCATE TABLE is only supported for tables and column tables"}});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST_FLAG(TruncateColumnTableFeatureFlag, Enable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(Enable);
        CreateTestTable(runtime, ++txId, "/MyRoot", true);
        env.TestWaitNotification(runtime, txId);

        if constexpr (Enable) {
            TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        } else {
            TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable",
                {{NKikimrScheme::StatusPreconditionFailed, "TRUNCATE TABLE is not supported for column tables"}});
        }
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST_FLAG(TruncateTableWithConcurrentDrop, IsColumnTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        CreateTestTable(runtime, ++txId, "/MyRoot", IsColumnTable);
        env.TestWaitNotification(runtime, txId);

        WriteTableData(runtime, txId, "/MyRoot/TestTable", IsColumnTable);
        UNIT_ASSERT_VALUES_EQUAL(CountTableRows(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep()), 5);

        bool firstProposeTransactionResultHandled = false;
        const ui64 truncateTxId = ++txId;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            if (ev->GetTypeRewrite() == (IsColumnTable ? TEvColumnShard::TEvProposeTransactionResult::EventType : TEvDataShard::TEvProposeTransactionResult::EventType)) {
                if (!firstProposeTransactionResultHandled) {
                    firstProposeTransactionResultHandled = true;

                    DropTestTable(runtime, txId, "/MyRoot", "TestTable", IsColumnTable,
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

        VerifyTableEmpty(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep());
    }

    Y_UNIT_TEST_FLAG(TruncateTableWithConcurrentTruncate, IsColumnTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        CreateTestTable(runtime, ++txId, "/MyRoot", IsColumnTable);
        env.TestWaitNotification(runtime, txId);

        WriteTableData(runtime, txId, "/MyRoot/TestTable", IsColumnTable);
        UNIT_ASSERT_VALUES_EQUAL(CountTableRows(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep()), 5);

        bool firstProposeTransactionResultHandled = false;
        const ui64 truncateTxId = ++txId;

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            if (ev->GetTypeRewrite() == (IsColumnTable ? TEvColumnShard::TEvProposeTransactionResult::EventType : TEvDataShard::TEvProposeTransactionResult::EventType)) {
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

        VerifyTableEmpty(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep());
    }

    Y_UNIT_TEST_FLAG(TruncateTableSequentialOperations, IsColumnTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        if constexpr (!IsColumnTable) {
            runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        }

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        CreateTestTable(runtime, ++txId, "/MyRoot", IsColumnTable);
        env.TestWaitNotification(runtime, txId);

        WriteTableData(runtime, txId, "/MyRoot/TestTable", IsColumnTable);
        UNIT_ASSERT_VALUES_EQUAL(CountTableRows(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep()), 5);

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        env.TestWaitNotification(runtime, txId);

        VerifyTableEmpty(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep());

        WriteTableData(runtime, txId, "/MyRoot/TestTable", IsColumnTable);
        UNIT_ASSERT_VALUES_EQUAL(CountTableRows(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep()), 5);

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        env.TestWaitNotification(runtime, txId);

        VerifyTableEmpty(runtime, "/MyRoot/TestTable", IsColumnTable, env.GetCoordinatorStep());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
    }

    Y_UNIT_TEST_FLAG(TruncateNonExistentTable, IsColumnTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        TestTruncateTable(runtime, ++txId, "/MyRoot",
            IsColumnTable ? "NonExistentColumnTable" : "NonExistentTable",
            {NKikimrScheme::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(TruncateTableWithCdcStream) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

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

        WriteTableData(runtime, txId, "/MyRoot/TestTable", false);

        TVector<TCell> indexCells = {
            TCell(TStringBuf("row one")), TCell::Make((ui64)1),
            TCell(TStringBuf("row two")), TCell::Make((ui64)2),
            TCell(TStringBuf("row three")), TCell::Make((ui64)3),
            TCell(TStringBuf("row four")), TCell::Make((ui64)4),
            TCell(TStringBuf("row five")), TCell::Make((ui64)5),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable/TestIndex/indexImplTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(indexCells, 5, 2), true);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 5);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TestIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 5);
        }

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable",
                         {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        {
            auto rows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(rows, 5);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TestIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 5);
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
        runtime.GetAppData().FeatureFlags.SetEnableCompactFulltextIndex(
            indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact ||
            indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "data" Type: "String" }
            KeyColumnNames: [ "id" ]
        )");
        env.TestWaitNotification(runtime, txId);

        TVector<TCell> mainTableCells = {
            TCell::Make((ui64)1), TCell(TStringBuf("hello")), TCell(TStringBuf("data one")),
            TCell::Make((ui64)2), TCell(TStringBuf("world")), TCell(TStringBuf("data two")),
            TCell::Make((ui64)3), TCell(TStringBuf("test")), TCell(TStringBuf("data three")),
            TCell::Make((ui64)4), TCell(TStringBuf("index")), TCell(TStringBuf("data four")),
        };
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/TestTable",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2, 3}, TSerializedCellMatrix(mainTableCells, 4, 3), true);

        Ydb::Table::TableIndex index;
        index.set_name("TextIndex");
        index.add_index_columns("text");
        if (indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance ||
            indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance) {
            auto& fulltext = *index.mutable_global_fulltext_relevance_index()->mutable_fulltext_settings();
            auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
            fulltext.mutable_columns()->at(0).set_column("text");
            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        } else if (indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain ||
            indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact) {
            auto& fulltext = *index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings();
            auto& analyzers = *fulltext.add_columns()->mutable_analyzers();
            fulltext.mutable_columns()->at(0).set_column("text");
            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        } else if (indexType == NKikimrSchemeOp::EIndexTypeGlobal) {
            index.mutable_global_index();
        } else if (indexType == NKikimrSchemeOp::EIndexTypeGlobalUnique) {
            index.mutable_global_unique_index();
        }

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/TestTable", index);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex"),
            {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTable/TextIndex/indexImplTable"),
            {NLs::PathExist});

        {
            auto mainRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(mainRows, 4);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 4);
        }
        if (indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance ||
            indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance) {
            auto docsRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplDocsTable");
            UNIT_ASSERT_VALUES_EQUAL(docsRows, 4);
            auto statsRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplStatsTable");
            UNIT_ASSERT_VALUES_EQUAL(statsRows, 1);
        }

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable");
        env.TestWaitNotification(runtime, txId);

        {
            auto mainRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable");
            UNIT_ASSERT_VALUES_EQUAL(mainRows, 0);
            auto indexRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(indexRows, 0);
        }
        if (indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance ||
            indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance) {
            auto docsRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplDocsTable");
            UNIT_ASSERT_VALUES_EQUAL(docsRows, 0);
            auto statsRows = CountRows(runtime, TTestTxConfig::SchemeShard, "/MyRoot/TestTable/TextIndex/indexImplStatsTable");
            UNIT_ASSERT_VALUES_EQUAL(statsRows, 0);
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

    Y_UNIT_TEST(TruncateTableWithFulltextIndex) {
        TruncateTableWithIndex(NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain);
    }

    Y_UNIT_TEST(TruncateTableWithFulltextCompactIndex) {
        TruncateTableWithIndex(NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact);
    }

    Y_UNIT_TEST(TruncateTableWithFulltextRelevance) {
        TruncateTableWithIndex(NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance);
    }

    Y_UNIT_TEST(TruncateTableWithFulltextCompactRelevanceIndex) {
        TruncateTableWithIndex(NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance);
    }

    Y_UNIT_TEST(TruncateColumnTableInStoreFails) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                    Columns { Name: "data" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "TestTable"
            ColumnShardCount: 1
        )");
        env.TestWaitNotification(runtime, txId);

        TestTruncateTable(runtime, ++txId, "/MyRoot/OlapStore", "TestTable",
            {{NKikimrScheme::StatusPreconditionFailed,
                "TRUNCATE TABLE is not supported for column tables in a column store"}});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(TruncateColumnTableWithTieringFails) {
        TTestBasicRuntime runtime;
        TTestEnvOptions options;
        options.EnableTieringInColumnShard(true);
        options.RunFakeConfigDispatcher(true);
        TTestEnv env(runtime, options);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
            Name: "Tier1"
            SourceType: "ObjectStorage"
            Location: "http://fake.fake/fake"
            Auth {
                Aws {
                    AwsAccessKeyIdSecretName: "secret"
                    AwsSecretAccessKeySecretName: "secret"
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestCreateColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTable"
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
            }
            Sharding {
                HashSharding { Columns: "timestamp" }
            }
            TtlSettings {
                Enabled {
                    ColumnName: "timestamp"
                    ColumnUnit: UNIT_AUTO
                    Tiers {
                        ApplyAfterSeconds: 360
                        EvictToExternalStorage { Storage: "/MyRoot/Tier1" }
                    }
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestLs(runtime, "/MyRoot/TestTable", false,
            NLs::HasColumnTableTtlSettingsTier("timestamp", TDuration::Seconds(360), "/MyRoot/Tier1"));

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTable",
            {{NKikimrScheme::StatusPreconditionFailed, "Cannot truncate column table with tiering"}});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(TruncateReadOnlyTableFails) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableTruncateColumnTable(true);
        runtime.GetAppData().FeatureFlags.SetEnableColumnTablesBackup(true);
        CreateTestTable(runtime, ++txId, "/MyRoot", true);
        env.TestWaitNotification(runtime, txId);

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TestTableCopy"
            CopyFromTable: "/MyRoot/TestTable"
            IsBackup: true
        )");
        env.TestWaitNotification(runtime, txId);

        TestTruncateTable(runtime, ++txId, "/MyRoot", "TestTableCopy",
            {{NKikimrScheme::StatusSchemeError, "path is a read-only copy column table"}});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TestTableCopy"), {NLs::PathExist});
    }

}
