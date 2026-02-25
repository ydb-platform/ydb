#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

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

            TestAlterColumnTable(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "NewTable"
                AlterSchema {
                    AddColumns { Name: "add_2" Type: "Uint64" }
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

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
}
