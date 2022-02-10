#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr::NSchemeShard; 
using namespace NKikimr;
using namespace NKikimrSchemeOp; 
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TOlapReboots) {
    Y_UNIT_TEST(CreateStore) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TString olapSchema = R"(
                Name: "OlapStore"
                ColumnShardCount: 1
                SchemaPresets {
                    Name: "default"
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" }
                        Columns { Name: "data" Type: "Utf8" }
                        KeyColumnNames: "timestamp"
                        Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                    }
                }
            )";

            TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", olapSchema);
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
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", R"(
                Name: "OlapStore"
                ColumnShardCount: 1
                SchemaPresets {
                    Name: "default"
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" }
                        Columns { Name: "data" Type: "Utf8" }
                        KeyColumnNames: "timestamp"
                        Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                    }
                }
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestCreateOlapTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                Name: "OlapTable"
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestLs(runtime, "/MyRoot/OlapStore/OlapTable", false, NLs::PathExist);

            {
                TInactiveZone inactive(activeZone);
                // no inactive finalization
            }
        });
    }

    Y_UNIT_TEST(CreateDropTable) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "OlapStore"
                    ColumnShardCount: 1
                    SchemaPresets {
                        Name: "default"
                        Schema {
                            Columns { Name: "timestamp" Type: "Timestamp" }
                            Columns { Name: "data" Type: "Utf8" }
                            KeyColumnNames: "timestamp"
                            Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                        }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateOlapTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                Name: "OlapTable"
            )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestDropOlapTable(runtime, ++t.TxId, "/MyRoot/OlapStore", "OlapTable");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestLs(runtime, "/MyRoot/OlapStore/OlapTable", false, NLs::PathNotExist);

            {
                TInactiveZone inactive(activeZone);
                // no inactive finalization
            }
        });
    }

    Y_UNIT_TEST(CreateMultipleTables) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "OlapStore"
                    ColumnShardCount: 1
                    SchemaPresets {
                        Name: "default"
                        Schema {
                            Columns { Name: "timestamp" Type: "Timestamp" }
                            Columns { Name: "data" Type: "Utf8" }
                            KeyColumnNames: "timestamp"
                            Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                        }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                CreateOlapTableRequest(t.TxId += 2, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable1"
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications}); 
            t.TestEnv->ReliablePropose(runtime,
                CreateOlapTableRequest(t.TxId - 1, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable2"
                )"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists, NKikimrScheme::StatusMultipleModifications}); 
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/OlapTable1", false, NLs::PathExist);
                TestLs(runtime, "/MyRoot/OlapStore/OlapTable2", false, NLs::PathExist);
            }
        });
    }

    Y_UNIT_TEST(DropMultipleTables) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "OlapStore"
                    ColumnShardCount: 1
                    SchemaPresets {
                        Name: "default"
                        Schema {
                            Columns { Name: "timestamp" Type: "Timestamp" }
                            Columns { Name: "data" Type: "Utf8" }
                            KeyColumnNames: "timestamp"
                            Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                        }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateOlapTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable1"
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateOlapTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable2"
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropOlapTableRequest(t.TxId += 2, "/MyRoot/OlapStore", "OlapTable1"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications}); 
            t.TestEnv->ReliablePropose(runtime,
                DropOlapTableRequest(t.TxId - 1, "/MyRoot/OlapStore", "OlapTable2"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications}); 
            t.TestEnv->TestWaitNotification(runtime, {t.TxId - 1, t.TxId});

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/OlapTable1", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/OlapStore/OlapTable2", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(CreateDropStore) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);
                // no inactive initialization
            }

            TString olapSchema = R"(
                Name: "OlapStore"
                ColumnShardCount: 1
                SchemaPresets {
                    Name: "default"
                    Schema {
                        Columns { Name: "timestamp" Type: "Timestamp" }
                        Columns { Name: "data" Type: "Utf8" }
                        KeyColumnNames: "timestamp"
                        Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                    }
                }
            )";

            t.TestEnv->ReliablePropose(runtime,
                CreateOlapStoreRequest(++t.TxId, "/MyRoot", olapSchema),
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
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "OlapStore"
                    ColumnShardCount: 1
                    SchemaPresets {
                        Name: "default"
                        Schema {
                            Columns { Name: "timestamp" Type: "Timestamp" }
                            Columns { Name: "data" Type: "Utf8" }
                            KeyColumnNames: "timestamp"
                            Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                        }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateOlapTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable"
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            t.TestEnv->ReliablePropose(runtime,
                DropOlapTableRequest(++t.TxId, "/MyRoot/OlapStore", "OlapTable"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications}); 
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            t.TestEnv->ReliablePropose(runtime,
                DropOlapStoreRequest(++t.TxId, "/MyRoot", "OlapStore"),
                {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications}); 
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/OlapTable", false, NLs::PathNotExist);
                TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathNotExist);
            }
        });
    }

    Y_UNIT_TEST(AlterTtlSettings) {
        TTestWithReboots t(false);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            runtime.GetAppData().FeatureFlags.SetEnableOlapSchemaOperationsForTest(true);

            {
                TInactiveZone inactive(activeZone);

                TestCreateOlapStore(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "OlapStore"
                    ColumnShardCount: 1
                    SchemaPresets {
                        Name: "default"
                        Schema {
                            Columns { Name: "timestamp" Type: "Timestamp" }
                            Columns { Name: "data" Type: "Utf8" }
                            KeyColumnNames: "timestamp"
                            Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                        }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateOlapTable(runtime, ++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable"
                    SchemaPresetName: "default"
                    TtlSettings {
                        Enabled {
                            ColumnName: "timestamp"
                            ExpireAfterSeconds: 600
                        }
                    }
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestLs(runtime, "/MyRoot/OlapStore/OlapTable", false, NLs::All(
                    NLs::HasOlapTableTtlSettingsVersion(1),
                    NLs::HasOlapTableTtlSettingsEnabled("timestamp", TDuration::Seconds(600))));
            }

            t.TestEnv->ReliablePropose(runtime,
                AlterOlapTableRequest(++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable"
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

                TestLs(runtime, "/MyRoot/OlapStore/OlapTable", false, NLs::All(
                    NLs::HasOlapTableTtlSettingsVersion(2),
                    NLs::HasOlapTableTtlSettingsEnabled("timestamp", TDuration::Seconds(300))));
            }

            t.TestEnv->ReliablePropose(runtime,
                AlterOlapTableRequest(++t.TxId, "/MyRoot/OlapStore", R"(
                    Name: "OlapTable"
                    AlterTtlSettings {
                        Disabled {}
                    }
                )"),
                {NKikimrScheme::StatusInvalidParameter, NKikimrScheme::StatusMultipleModifications}); 
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestLs(runtime, "/MyRoot/OlapStore/OlapTable", false, NLs::All(
                    NLs::HasOlapTableTtlSettingsVersion(2),
                    NLs::HasOlapTableTtlSettingsEnabled("timestamp", TDuration::Seconds(300))));
            }
        });
    }
}
