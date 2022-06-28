#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;


Y_UNIT_TEST_SUITE(TOlap) {
    Y_UNIT_TEST(CreateStore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

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

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);

        TString olapSchema1 = R"(
            Name: "OlapStore1"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" }
                    Columns { Name: "data" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                    Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                    StorageTiers { Name: "tier0" }
                    StorageTiers { Name: "tier0" }
                }
            }
        )";

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema1, {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(CreateStoreWithDirs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "DirA/DirB/OlapStore"
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
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/DirA/DirB/OlapStore", false, NLs::PathExist);
    }

    Y_UNIT_TEST(CreateTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

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

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);

        TestMkDir(runtime, ++txId, "/MyRoot/OlapStore", "MyDir");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/MyDir", false, NLs::PathExist);

        TString tableSchema = R"(
            Name: "ColumnTable"
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", tableSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/MyDir/ColumnTable", false, NLs::All(
            NLs::PathExist,
            NLs::HasColumnTableSchemaPreset("default")));

        // Missing column from schema preset
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableMissingDataColumn"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Extra column not in schema preset
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableExtraColumn"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                Columns { Name: "data" Type: "Utf8" }
                Columns { Name: "comment" Type: "Utf8" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Different column order
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableDifferentColumnOrder"
            Schema {
                Columns { Name: "data" Type: "Utf8" }
                Columns { Name: "timestamp" Type: "Timestamp" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Extra key column
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableExtraKeyColumn"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
                KeyColumnNames: "data"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Unknown key column
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableUnknownKeyColumn"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "nottimestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Different data column type
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableDataColumnType"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                Columns { Name: "data" Type: "String" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Repeating preset schema should succeed
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableExplicitSchema"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Creating table with directories should succeed
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "DirA/DirB/NestedTable"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/DirA/DirB/NestedTable", false, NLs::All(
            NLs::PathExist,
            NLs::HasColumnTableSchemaPreset("default")));

        // Additional storage tier in schema
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "TableWithTiers"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                StorageTiers { Name: "tierX" }
            }
        )", {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(CustomDefaultPresets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
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
        env.TestWaitNotification(runtime, txId);

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "ColumnTable"
            SchemaPresetName: "default"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
            NLs::PathExist,
            NLs::HasColumnTableSchemaPreset("default")));
    }

    Y_UNIT_TEST(CreateDropTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

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

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);
        TestLsPathId(runtime, 2, NLs::PathStringEqual("/MyRoot/OlapStore"));

        TestMkDir(runtime, ++txId, "/MyRoot/OlapStore", "MyDir");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/MyDir", false, NLs::PathExist);

        TString tableSchema = R"(
            Name: "ColumnTable"
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", tableSchema);
        env.TestWaitNotification(runtime, txId);
        TestLsPathId(runtime, 4, NLs::PathStringEqual("/MyRoot/OlapStore/MyDir/ColumnTable"));
        TestDropColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", "ColumnTable");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/MyDir/ColumnTable", false, NLs::PathNotExist);
        TestLsPathId(runtime, 4, NLs::PathStringEqual(""));

        TestDropOlapStore(runtime, ++txId, "/MyRoot", "OlapStore", {NKikimrScheme::StatusNameConflict});
        TestRmDir(runtime, ++txId, "/MyRoot/OlapStore", "MyDir");
        env.TestWaitNotification(runtime, txId);

        TestDropOlapStore(runtime, ++txId, "/MyRoot", "OlapStore");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathNotExist);
        TestLsPathId(runtime, 2, NLs::PathStringEqual(""));
    }

    Y_UNIT_TEST(CreateTableTtl) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

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
                    StorageTiers { Name: "tier0" }
                    StorageTiers { Name: "tier1" Compression { CompressionCodec: ColumnCodecZSTD CompressionLevel: 5 } }
                    StorageTiers { Name: "tier2" Compression { CompressionCodec: ColumnCodecZSTD CompressionLevel: 10 } }
                }
            }
        )";

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TString tableSchema1 = R"(
            Name: "Table1"
            TtlSettings {
                Enabled { ColumnName: "timestamp" ExpireAfterSeconds: 300 }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema1);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/Table1", false, NLs::All(
            NLs::HasColumnTableSchemaPreset("default"),
            NLs::HasColumnTableSchemaVersion(1),
            NLs::HasColumnTableTtlSettingsVersion(1),
            NLs::HasColumnTableTtlSettingsEnabled("timestamp", TDuration::Seconds(300))));

        TString tableSchema2 = R"(
            Name: "Table2"
            TtlSettings {
                Disabled {}
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema2);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/Table2", false, NLs::All(
            NLs::HasColumnTableSchemaPreset("default"),
            NLs::HasColumnTableSchemaVersion(1),
            NLs::HasColumnTableTtlSettingsVersion(1),
            NLs::HasColumnTableTtlSettingsDisabled()));

        TString tableSchema3 = R"(
            Name: "Table3"
            TtlSettings {
                Tiering {
                    Tiers { Name: "tier0" Eviction { ColumnName: "timestamp" ExpireAfterSeconds: 300 } }
                    Tiers { Name: "tier1" Eviction { ColumnName: "timestamp" ExpireAfterSeconds: 600 } }
                }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema3);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/Table3", false, NLs::All(
            NLs::HasColumnTableSchemaPreset("default"),
            NLs::HasColumnTableSchemaVersion(1),
            NLs::HasColumnTableTtlSettingsVersion(1),
            NLs::HasColumnTableTtlSettingsTiering(0, "tier0", "timestamp", TDuration::Seconds(300)),
            NLs::HasColumnTableTtlSettingsTiering(1, "tier1", "timestamp", TDuration::Seconds(600))));

        TString tableSchema4 = R"(
            Name: "Table4"
            TtlSettings {
                Tiering {
                    Tiers { Name: "tier0" Eviction { ColumnName: "timestamp" ExpireAfterSeconds: 300 } }
                    Tiers { Name: "tier1" Eviction { ColumnName: "data" ExpireAfterSeconds: 600 } }
                }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema4,
                            {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(AlterStore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

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

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TString tableSchemaX = R"(
            Name: "ColumnTable"
            TtlSettings {
                Enabled {
                    ExpireAfterSeconds: 300
                }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchemaX,
                            {NKikimrScheme::StatusInvalidParameter});

        TString tableSchema = R"(
            Name: "ColumnTable"
            TtlSettings {
                Enabled {
                    ColumnName: "timestamp"
                    ExpireAfterSeconds: 300
                }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
            NLs::HasColumnTableSchemaPreset("default"),
            NLs::HasColumnTableSchemaVersion(1),
            NLs::HasColumnTableTtlSettingsVersion(1),
            NLs::HasColumnTableTtlSettingsEnabled("timestamp", TDuration::Seconds(300))));

        TestAlterOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "OlapStore"
            AlterSchemaPresets {
                Name: "default"
                AlterSchema {
                    AddColumns { Name: "comment" Type: "Utf8" }
                }
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(AlterTtl) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableOlapSchemaOperations(true));
        ui64 txId = 100;

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
                    StorageTiers { Name: "tier0" }
                    StorageTiers { Name: "tier1" }
                }
            }
        )";

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TString tableSchema = R"(
            Name: "ColumnTable"
            TtlSettings {
                Enabled {
                    ColumnName: "timestamp"
                    ExpireAfterSeconds: 300
                }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
            NLs::HasColumnTableSchemaPreset("default"),
            NLs::HasColumnTableSchemaVersion(1),
            NLs::HasColumnTableTtlSettingsVersion(1),
            NLs::HasColumnTableTtlSettingsEnabled("timestamp", TDuration::Seconds(300))));

        TestAlterColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "ColumnTable"
            AlterTtlSettings {
                Enabled {
                    ColumnName: "timestamp"
                    ExpireAfterSeconds: 600
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
            NLs::HasColumnTableSchemaPreset("default"),
            NLs::HasColumnTableSchemaVersion(1),
            NLs::HasColumnTableTtlSettingsVersion(2),
            NLs::HasColumnTableTtlSettingsEnabled("timestamp", TDuration::Seconds(600))));

        TestAlterColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "ColumnTable"
            AlterTtlSettings {
                Disabled {}
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // TODO: support TTL <-> Tiering changes
        TestAlterColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "ColumnTable"
            AlterTtlSettings {
                Tiering {
                    Tiers {
                        Name: "tier0"
                        Eviction {
                            ColumnName: "timestamp"
                            ExpireAfterSeconds: 600
                        }
                    }
                }
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    // TODO: AlterTiers
    // negatives for store: disallow alters
    // negatives for table: wrong tiers count, wrong tiers, wrong eviction column, wrong eviction values,
    //      different TTL columns in tiers
}
