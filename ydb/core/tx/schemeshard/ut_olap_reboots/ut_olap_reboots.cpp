#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/schemeshard/ut_helpers/olap_helpers.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

static const TString defaultStoreSchema = R"(
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
)";

static const TString defaultTableSchema = R"(
    Name: "ColumnTable"
    ColumnShardCount: 1
    Schema {
        Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
        Columns { Name: "data" Type: "Utf8" }
        KeyColumnNames: "timestamp"
    }
)";

Y_UNIT_TEST_SUITE(TOlapReboots) {
    Y_UNIT_TEST(CreateStore) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);

            {
                TInactiveZone inactive(activeZone);
                // no inactive finalization
            }
        });
    }

    Y_UNIT_TEST(CreateTable) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                Name: "ColumnTable"
                ColumnShardCount: 1
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST(CreateStandaloneTable) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", defaultTableSchema);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/ColumnTable", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST(CreateDropTable) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                Name: "ColumnTable"
                ColumnShardCount: 1
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", "ColumnTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::PathNotExist);

            {
                TInactiveZone inactive(activeZone);
                // no inactive finalization
            }
        });
    }

    Y_UNIT_TEST(CreateDropStandaloneTable) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", defaultTableSchema);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "ColumnTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/ColumnTable", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(CreateMultipleTables) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateColumnTableRequest(t.TxId += 2, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable1"
                    ColumnShardCount: 1
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->ReliablePropose(runtime,
                CreateColumnTableRequest(t.TxId - 1, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable2"
                    ColumnShardCount: 1
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable1", false, NLs::PathExist);
                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable2", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST(CreateMultipleStandaloneTables) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateColumnTableRequest(t.TxId += 2, "/MyRoot",
                                         SubstGlobalCopy(defaultTableSchema, "ColumnTable", "ColumnTable1")),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->ReliablePropose(runtime,
                CreateColumnTableRequest(t.TxId - 1, "/MyRoot",
                                         SubstGlobalCopy(defaultTableSchema, "ColumnTable", "ColumnTable2")),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/ColumnTable1", false, NLs::PathExist);
                TestLs(runtime, "/MyRoot/ColumnTable2", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST(DropMultipleTables) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable1"
                    ColumnShardCount: 1
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable2"
                    ColumnShardCount: 1
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(t.TxId += 2, "/MyRoot/OlapStore", "ColumnTable1"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(t.TxId - 1, "/MyRoot/OlapStore", "ColumnTable2"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable1", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable2", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(DropMultipleStandaloneTables) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot",
                                      SubstGlobalCopy(defaultTableSchema, "ColumnTable", "ColumnTable1"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot",
                                      SubstGlobalCopy(defaultTableSchema, "ColumnTable", "ColumnTable2"));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(t.TxId += 2, "/MyRoot", "ColumnTable1"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(t.TxId - 1, "/MyRoot", "ColumnTable2"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/ColumnTable1", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/ColumnTable2", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(CreateDropStore) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateOlapStoreRequest(++t.TxId, "/MyRoot", defaultStoreSchema),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->ReliablePropose(runtime,
                DropOlapStoreRequest(++t.TxId, "/MyRoot", "OlapStore"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(DropTableThenStore) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable"
                    ColumnShardCount: 1
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot/OlapStore", "ColumnTable"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->ReliablePropose(runtime,
                DropOlapStoreRequest(++t.TxId, "/MyRoot", "OlapStore"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(AlterTtlSettings) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", defaultStoreSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable"
                    ColumnShardCount: 1
                    SchemaPresetName: "default"
                    TtlSettings {
                        Enabled {
                            ColumnName: "timestamp"
                            ExpireAfterSeconds: 600
                        }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
                    NLs::HasColumnTableTtlSettingsVersion(1),
                    NLs::HasColumnTableTtlSettingsEnabled("timestamp", TDuration::Seconds(600))));
            }

            t.TestEnv->ReliablePropose(runtime,
                AlterColumnTableRequest(++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable"
                    AlterTtlSettings {
                        Enabled {
                            ColumnName: "timestamp"
                            ExpireAfterSeconds: 300
                        }
                    }
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
                    NLs::HasColumnTableTtlSettingsVersion(2),
                    NLs::HasColumnTableTtlSettingsEnabled("timestamp", TDuration::Seconds(300))));
            }

            t.TestEnv->ReliablePropose(runtime,
                AlterColumnTableRequest(++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "ColumnTable"
                    AlterTtlSettings {
                        Disabled {}
                    }
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
                    NLs::HasColumnTableTtlSettingsVersion(3)));
            }
        });
    }

    Y_UNIT_TEST(CopyWithRebootsAtCommit) {
        TTestWithReboots t(true);
        t.GetTestEnvOptions().EnableRealSystemViewPaths(false);
        t.GetTestEnvOptions().EnableColumnTablesBackup(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "key3" Type: "Uint64" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2", "key3"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime, CopyColumnTableRequest(++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::ChildrenCount(3)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable", true),
                                   {NLs::Finished, NLs::IsColumnTable});
            }
        });
    }

    Y_UNIT_TEST(DropCopyWithRebootsAtCommit) {
        TTestWithReboots t(true);
        t.GetTestEnvOptions().EnableColumnTablesBackup(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "key3" Type: "Uint64" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2", "key3"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime, DropColumnTableRequest(++t.TxId, "/MyRoot", "Table"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});
            t.TestEnv->ReliablePropose(runtime, DropColumnTableRequest(++t.TxId, "/MyRoot", "NewTable"),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(AlterCopyWithReboots) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().EnableColumnTablesBackup(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Alter the table
            TestAlterColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "Table"
                AlterSchema {
                    AddColumns { Name: "add_1" Type: "Uint64" }
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        });
    }

    Y_UNIT_TEST(CopyAlterWithReboots) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().EnableColumnTablesBackup(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            // Copy the table
            TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // Alter on a read-only copy column table must fail
            TestAlterColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "NewTable"
                AlterSchema {
                    AddColumns { Name: "add_2" Type: "Uint64" }
                }
            )", {{NKikimrScheme::StatusSchemeError, "path is a read-only copy column table; only Copy and Drop are allowed"}});

            {
                TInactiveZone inactive(activeZone);
                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "Table");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        });
    }

    Y_UNIT_TEST(CopyTableAndDropWithReboots) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().EnableColumnTablesBackup(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "key3" Type: "Uint64" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2", "key3"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
        });
    }

    Y_UNIT_TEST(CopyTableAndDropWithReboots2) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().EnableColumnTablesBackup(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "key3" Type: "Uint64" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2", "key3"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable", "/MyRoot/Table");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            AsyncDropColumnTable(runtime, ++t.TxId, "/MyRoot", "Table");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});
            AsyncDropColumnTable(runtime, ++t.TxId, "/MyRoot", "NewTable");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId-1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                                   {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(ChainedCopyTableAndDropWithReboots) {
        TTestWithReboots t(false);
        t.GetTestEnvOptions().EnableColumnTablesBackup(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const int maxTableIdx = 4;

            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table1"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "key3" Type: "Uint64" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2", "key3"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                // Make a chain of copy-of-copy
                for (int i = 2; i <= maxTableIdx; ++i) {
                    TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Table%d", i), Sprintf("/MyRoot/Table%d", i-1));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }

                // Drop all intermediate copies
                for (int i = 1; i < maxTableIdx; ++i) {
                    TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Table%d", i));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            // Drop the last table
            TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Table%d", maxTableIdx));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                for (int i = 1; i <= maxTableIdx; ++i) {
                    TestDescribeResult(DescribePath(runtime, Sprintf("/MyRoot/Table%d", i)),
                                       {NLs::PathNotExist});
                }
            }
        });
    }

    // Reboot test: drop a read-only copy (non-owner) of a shared shard.
    // The source table's shard must survive and SharedShards must be cleaned up.
    Y_UNIT_TEST(DropCopySharedShardCleanupWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", defaultTableSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "CopyTable", "/MyRoot/ColumnTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "CopyTable"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/CopyTable", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/ColumnTable", false, NLs::PathExist);

                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);

                const auto tabletIds = GetColumnShardTabletIds(runtime, "/MyRoot/ColumnTable");
                UNIT_ASSERT_VALUES_UNEQUAL(tabletIds.size(), 0u);
                const ui64 localShardIdx = ResolveLocalShardIdxByTabletId(runtime, tabletIds[0]);
                UNIT_ASSERT_VALUES_UNEQUAL(localShardIdx, 0u);
                UNIT_ASSERT_VALUES_UNEQUAL(GetShardOwnerLocalPathId(runtime, localShardIdx), 0u);
            }
        });
    }

    // Reboot test: drop the owner while a copy (sharer) still exists.
    // Ownership must be transferred to the copy.
    Y_UNIT_TEST(DropOwnerWithSharerTransfersOwnershipWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", defaultTableSchema);
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "CopyTable", "/MyRoot/ColumnTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "ColumnTable"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/ColumnTable", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/CopyTable", false, NLs::PathExist);

                const auto tabletIds = GetColumnShardTabletIds(runtime, "/MyRoot/CopyTable");
                UNIT_ASSERT_VALUES_UNEQUAL(tabletIds.size(), 0u);
                const ui64 localShardIdx = ResolveLocalShardIdxByTabletId(runtime, tabletIds[0]);
                UNIT_ASSERT_VALUES_UNEQUAL(localShardIdx, 0u);

                const ui64 copyPathId = GetLocalPathId(runtime, "/MyRoot/CopyTable");
                UNIT_ASSERT_VALUES_EQUAL(GetShardOwnerLocalPathId(runtime, localShardIdx), copyPathId);

                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);

                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "CopyTable");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestLs(runtime, "/MyRoot/CopyTable", false, NLs::PathNotExist);
            }
        });
    }

    // Reboot test: chained copy-of-copy-of-copy, then drop originals leaving
    // only the last copy, then drop the last copy. Tests ownership transfer
    // through a chain. Most drops are in the inactive zone to keep the number
    // of reboot iterations manageable; only the final ownership-transfer drop
    // (T3, the last intermediate) is tested with reboots.
    Y_UNIT_TEST(ChainedCopyDropOriginalsWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const int chainLen = 4;

            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "T1"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (int i = 2; i <= chainLen; ++i) {
                    TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot",
                        Sprintf("T%d", i), Sprintf("/MyRoot/T%d", i - 1));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }

                for (int i = 1; i <= chainLen - 2; ++i) {
                    TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("T%d", i));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", Sprintf("T%d", chainLen - 1)),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                for (int i = 1; i < chainLen; ++i) {
                    TestDescribeResult(DescribePath(runtime, Sprintf("/MyRoot/T%d", i)),
                                       {NLs::PathNotExist});
                }
                TestLs(runtime, Sprintf("/MyRoot/T%d", chainLen), false, NLs::PathExist);

                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);

                const auto tabletIds = GetColumnShardTabletIds(runtime, Sprintf("/MyRoot/T%d", chainLen));
                UNIT_ASSERT_VALUES_UNEQUAL(tabletIds.size(), 0u);
                const ui64 localShardIdx = ResolveLocalShardIdxByTabletId(runtime, tabletIds[0]);
                UNIT_ASSERT_VALUES_UNEQUAL(localShardIdx, 0u);

                const ui64 lastPathId = GetLocalPathId(runtime, Sprintf("/MyRoot/T%d", chainLen));
                UNIT_ASSERT_VALUES_EQUAL(GetShardOwnerLocalPathId(runtime, localShardIdx), lastPathId);

                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("T%d", chainLen));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestDescribeResult(DescribePath(runtime, Sprintf("/MyRoot/T%d", chainLen)),
                                   {NLs::PathNotExist});
                UNIT_ASSERT_VALUES_EQUAL(GetShardOwnerLocalPathId(runtime, localShardIdx), 0u);
            }
        });
    }

    // Reboot test: chained copies where we drop the last copy first, then
    // work backwards to the original. Shared shard entries must be cleaned
    // up correctly at each step.
    Y_UNIT_TEST(ChainedCopyDropInReverseOrderWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const int chainLen = 4;

            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "T1"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (int i = 2; i <= chainLen; ++i) {
                    TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot",
                        Sprintf("T%d", i), Sprintf("/MyRoot/T%d", i - 1));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            {
                TInactiveZone inactive(activeZone);
                for (int i = chainLen; i >= 2; --i) {
                    TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("T%d", i));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "T1"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                for (int i = 1; i <= chainLen; ++i) {
                    TestDescribeResult(DescribePath(runtime, Sprintf("/MyRoot/T%d", i)),
                                       {NLs::PathNotExist});
                }
                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);
            }
        });
    }

    // Reboot test: interleaved copy and drop operations. Create original,
    // copy it, drop original (ownership transfers), copy the surviving table,
    // drop the intermediate, and so on.
    Y_UNIT_TEST(InterleavedCopyDropOwnershipChainWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "A"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "B", "/MyRoot/A");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "A");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "C", "/MyRoot/B");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "B");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                CopyColumnTableRequest(++t.TxId, "/MyRoot", "D", "/MyRoot/C"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "C"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/A", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/B", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/C", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/D", false, NLs::PathExist);

                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);

                const auto tabletIds = GetColumnShardTabletIds(runtime, "/MyRoot/D");
                UNIT_ASSERT_VALUES_UNEQUAL(tabletIds.size(), 0u);
                const ui64 localShardIdx = ResolveLocalShardIdxByTabletId(runtime, tabletIds[0]);
                UNIT_ASSERT_VALUES_UNEQUAL(localShardIdx, 0u);

                const ui64 dPathId = GetLocalPathId(runtime, "/MyRoot/D");
                UNIT_ASSERT_VALUES_EQUAL(GetShardOwnerLocalPathId(runtime, localShardIdx), dPathId);

                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "D");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestLs(runtime, "/MyRoot/D", false, NLs::PathNotExist);
                UNIT_ASSERT_VALUES_EQUAL(GetShardOwnerLocalPathId(runtime, localShardIdx), 0u);
            }
        });
    }

    // Reboot test: multiple copies from the same source, then drop the source
    // (owner). Ownership must transfer to one of the copies, and the remaining
    // copies must still be in SharedShards.
    Y_UNIT_TEST(MultipleCopiesDropOwnerWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Src"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (int i = 1; i <= 3; ++i) {
                    TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot",
                        Sprintf("Copy%d", i), "/MyRoot/Src");
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "Src"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/Src", false, NLs::PathNotExist);
                for (int i = 1; i <= 3; ++i) {
                    TestLs(runtime, Sprintf("/MyRoot/Copy%d", i), false, NLs::PathExist);
                }

                const auto tabletIds = GetColumnShardTabletIds(runtime, "/MyRoot/Copy1");
                UNIT_ASSERT_VALUES_UNEQUAL(tabletIds.size(), 0u);
                const ui64 localShardIdx = ResolveLocalShardIdxByTabletId(runtime, tabletIds[0]);
                UNIT_ASSERT_VALUES_UNEQUAL(localShardIdx, 0u);

                const ui64 ownerPathId = GetShardOwnerLocalPathId(runtime, localShardIdx);
                UNIT_ASSERT_VALUES_UNEQUAL(ownerPathId, 0u);

                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 2u * tabletIds.size());

                for (int i = 1; i <= 3; ++i) {
                    TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Copy%d", i));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);
                UNIT_ASSERT_VALUES_EQUAL(GetShardOwnerLocalPathId(runtime, localShardIdx), 0u);
            }
        });
    }

    // Reboot test: multiple copies from the same source, drop all copies first,
    // then drop the owner. SharedShards must be cleaned up at each step.
    Y_UNIT_TEST(MultipleCopiesDropAllCopiesThenOwnerWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Src"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (int i = 1; i <= 3; ++i) {
                    TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot",
                        Sprintf("Copy%d", i), "/MyRoot/Src");
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            {
                TInactiveZone inactive(activeZone);
                for (int i = 1; i <= 2; ++i) {
                    TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("Copy%d", i));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "Copy3"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "Src"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/Src", false, NLs::PathNotExist);
                for (int i = 1; i <= 3; ++i) {
                    TestLs(runtime, Sprintf("/MyRoot/Copy%d", i), false, NLs::PathNotExist);
                }
                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);
            }
        });
    }

    // Reboot test: copy-of-copy chain with concurrent async drops of original
    // and copy. Tests that shared shard state is consistent after concurrent
    // drops with reboots.
    Y_UNIT_TEST(ChainedCopyConcurrentDropWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "T1"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "T2", "/MyRoot/T1");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot", "T3", "/MyRoot/T2");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            AsyncDropColumnTable(runtime, ++t.TxId, "/MyRoot", "T1");
            AsyncDropColumnTable(runtime, ++t.TxId, "/MyRoot", "T2");
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);
                TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", "T3");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/T1"), {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/T2"), {NLs::PathNotExist});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/T3"), {NLs::PathNotExist});
                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);
            }
        });
    }

    // Reboot test: deep chain of copies (6 levels), drop from the middle
    // outward. Tests that ownership transfers correctly through a deep chain.
    Y_UNIT_TEST(DeepChainedCopyDropFromMiddleWithReboots) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const int chainLen = 6;

            {
                TInactiveZone inactive(activeZone);
                TestCreateColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "T1"
                    ColumnShardCount: 1
                    Schema {
                        Columns { Name: "key1" Type: "Uint32" NotNull: true }
                        Columns { Name: "key2" Type: "Utf8" NotNull: true }
                        Columns { Name: "Value" Type: "Utf8" }
                        KeyColumnNames: ["key1", "key2"]
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                for (int i = 2; i <= chainLen; ++i) {
                    TestCopyColumnTable(runtime, ++t.TxId, "/MyRoot",
                        Sprintf("T%d", i), Sprintf("/MyRoot/T%d", i - 1));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            {
                TInactiveZone inactive(activeZone);
                const int inactiveDropOrder[] = {3, 4, 2, 5, 1};
                for (int idx : inactiveDropOrder) {
                    TestDropColumnTable(runtime, ++t.TxId, "/MyRoot", Sprintf("T%d", idx));
                    t.TestEnv->TestWaitNotification(runtime, t.TxId);
                }
            }

            t.TestEnv->ReliablePropose(runtime,
                DropColumnTableRequest(++t.TxId, "/MyRoot", "T6"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusPathDoesNotExist, NKikimrScheme::StatusMultipleModifications});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                for (int i = 1; i <= chainLen; ++i) {
                    TestDescribeResult(DescribePath(runtime, Sprintf("/MyRoot/T%d", i)),
                                       {NLs::PathNotExist});
                }
                UNIT_ASSERT_VALUES_EQUAL(CountSharedShardsRows(runtime), 0u);
            }
        });
    }
}
