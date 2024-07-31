#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;

namespace NKikimr {
namespace {

namespace NTypeIds = NScheme::NTypeIds;
using TTypeInfo = NScheme::TTypeInfo;

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

static const TVector<NArrow::NTest::TTestColumn> defaultYdbSchema = {
    NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp).SetNullable(false) ),
    NArrow::NTest::TTestColumn("data", TTypeInfo(NTypeIds::Utf8) )
};

}}


Y_UNIT_TEST_SUITE(TOlap) {
    Y_UNIT_TEST(CreateStore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);

        TString olapSchema1 = R"(
            Name: "OlapStore1"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                    Columns { Name: "data" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                    Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                }
            }
        )";

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema1, {NKikimrScheme::StatusAccepted});
    }

    Y_UNIT_TEST(CreateStoreWithDirs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "DirA/DirB/OlapStore"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
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
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);

        TestMkDir(runtime, ++txId, "/MyRoot/OlapStore", "MyDir");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/MyDir", false, NLs::PathExist);

        TString tableSchema = R"(
            Name: "ColumnTable"
            ColumnShardCount: 1
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", tableSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/MyDir/ColumnTable", false, NLs::All(
            NLs::PathExist,
            NLs::HasColumnTableSchemaPreset("default")));

        // Missing column from schema preset
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableMissingDataColumn"
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Extra column not in schema preset
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "ColumnTableExtraColumn"
            ColumnShardCount: 1
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
            ColumnShardCount: 1
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
            ColumnShardCount: 1
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
            ColumnShardCount: 1
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
            ColumnShardCount: 1
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
            ColumnShardCount: 1
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
            ColumnShardCount: 1
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/DirA/DirB/NestedTable", false, NLs::All(
            NLs::PathExist,
            NLs::HasColumnTableSchemaPreset("default")));

        // Additional storage tier in schema
        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore/MyDir", R"(
            Name: "TableWithTiers"
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" }
                Columns { Name: "data" Type: "Utf8" }
                KeyColumnNames: "timestamp"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
        )", {NKikimrScheme::StatusAccepted});
    }

    Y_UNIT_TEST(CustomDefaultPresets) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "ColumnTable"
            ColumnShardCount: 1
            SchemaPresetName: "default"
        )");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::All(
            NLs::PathExist,
            NLs::HasColumnTableSchemaPreset("default")));
    }

    Y_UNIT_TEST(CreateDropTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);
        TestLsPathId(runtime, 2, NLs::PathStringEqual("/MyRoot/OlapStore"));

        TestMkDir(runtime, ++txId, "/MyRoot/OlapStore", "MyDir");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/MyDir", false, NLs::PathExist);

        TString tableSchema = R"(
            Name: "ColumnTable"
            ColumnShardCount: 1
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

    Y_UNIT_TEST(CreateDropStandaloneTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "MyDir");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyDir", false, NLs::PathExist);

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/MyDir", defaultTableSchema);
        env.TestWaitNotification(runtime, txId);

        TestLsPathId(runtime, 3, NLs::PathStringEqual("/MyRoot/MyDir/ColumnTable"));

        TestDropColumnTable(runtime, ++txId, "/MyRoot/MyDir", "ColumnTable");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyDir/ColumnTable", false, NLs::PathNotExist);
        TestLsPathId(runtime, 3, NLs::PathStringEqual(""));

        // PARTITION BY ()

        TString otherSchema = R"(
            Name: "ColumnTable"
            ColumnShardCount: 4
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "some" Type: "Uint64" NotNull: true }
                Columns { Name: "data" Type: "Utf8" NotNull: true }
                KeyColumnNames: "some"
                KeyColumnNames: "data"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            }
            Sharding {
                HashSharding {
                    Columns: ["some", "data"]
                }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/MyDir", otherSchema);
        env.TestWaitNotification(runtime, txId);

        auto checkFn = [&](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetPath(), "/MyRoot/MyDir/ColumnTable");

            auto& sharding = record.GetPathDescription().GetColumnTableDescription().GetSharding();
            UNIT_ASSERT_VALUES_EQUAL(sharding.ColumnShardsSize(), 4);
            UNIT_ASSERT(sharding.HasHashSharding());
            auto& hashSharding = sharding.GetHashSharding();
            UNIT_ASSERT_VALUES_EQUAL(hashSharding.ColumnsSize(), 2);
            UNIT_ASSERT_EQUAL(hashSharding.GetFunction(),
                              NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
            UNIT_ASSERT_VALUES_EQUAL(hashSharding.GetColumns()[0], "some");
            UNIT_ASSERT_VALUES_EQUAL(hashSharding.GetColumns()[1], "data");
        };

        TestLsPathId(runtime, 4, checkFn);

        TestDropColumnTable(runtime, ++txId, "/MyRoot/MyDir", "ColumnTable");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyDir/ColumnTable", false, NLs::PathNotExist);
        TestLsPathId(runtime, 4, NLs::PathStringEqual(""));
    }

    Y_UNIT_TEST(CreateDropStandaloneTableDefaultSharding) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "MyDir");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyDir", false, NLs::PathExist);

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/MyDir", defaultTableSchema);
        env.TestWaitNotification(runtime, txId);

        TestLsPathId(runtime, 3, NLs::PathStringEqual("/MyRoot/MyDir/ColumnTable"));

        TestDropColumnTable(runtime, ++txId, "/MyRoot/MyDir", "ColumnTable");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyDir/ColumnTable", false, NLs::PathNotExist);
        TestLsPathId(runtime, 3, NLs::PathStringEqual(""));

        TString otherSchema = R"(
            Name: "ColumnTable"
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "some" Type: "Uint64" NotNull: true }
                Columns { Name: "data" Type: "Utf8" NotNull: true }
                KeyColumnNames: "some"
                KeyColumnNames: "data"
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/MyDir", otherSchema);
        env.TestWaitNotification(runtime, txId);

        auto checkFn = [&](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT_VALUES_EQUAL(record.GetPath(), "/MyRoot/MyDir/ColumnTable");

            auto& description = record.GetPathDescription().GetColumnTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(description.GetColumnShardCount(), 64);

            auto& sharding = description.GetSharding();
            UNIT_ASSERT_VALUES_EQUAL(sharding.ColumnShardsSize(), 64);
            UNIT_ASSERT(sharding.HasHashSharding());
            auto& hashSharding = sharding.GetHashSharding();
            UNIT_ASSERT_VALUES_EQUAL(hashSharding.ColumnsSize(), 2);
            UNIT_ASSERT_EQUAL(hashSharding.GetFunction(),
                              NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
            UNIT_ASSERT_VALUES_EQUAL(hashSharding.GetColumns()[0], "some");
            UNIT_ASSERT_VALUES_EQUAL(hashSharding.GetColumns()[1], "data");
        };

        TestLsPathId(runtime, 4, checkFn);

        TestDropColumnTable(runtime, ++txId, "/MyRoot/MyDir", "ColumnTable");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyDir/ColumnTable", false, NLs::PathNotExist);
        TestLsPathId(runtime, 4, NLs::PathStringEqual(""));
    }

    Y_UNIT_TEST(CreateTableTtl) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", defaultStoreSchema);
        env.TestWaitNotification(runtime, txId);

        TString tableSchema1 = R"(
            Name: "Table1"
            ColumnShardCount: 1
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
            ColumnShardCount: 1
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
            ColumnShardCount: 1
            TtlSettings {
                UseTiering : "Tiering1"
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema3);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/Table3", false, NLs::All(
            NLs::HasColumnTableSchemaPreset("default"),
            NLs::HasColumnTableSchemaVersion(1),
            NLs::HasColumnTableTtlSettingsVersion(1),
            NLs::HasColumnTableTtlSettingsTiering("Tiering1")));

        TString tableSchema4 = R"(
            Name: "Table4"
            ColumnShardCount: 1
            TtlSettings {
                UseTiering : "Tiering1"
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema4,
                            {NKikimrScheme::StatusAccepted});
    }

    Y_UNIT_TEST(AlterStore) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TString tableSchemaX = R"(
            Name: "ColumnTable"
            ColumnShardCount: 1
            TtlSettings {
                Enabled {
                    ExpireAfterSeconds: 300
                }
            }
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchemaX,
                            {NKikimrScheme::StatusSchemeError});

        TString tableSchema = R"(
            Name: "ColumnTable"
            ColumnShardCount: 1
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
        )", {NKikimrScheme::StatusAccepted});
    }

    Y_UNIT_TEST(AlterTtl) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString olapSchema = R"(
            Name: "OlapStore"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                    Columns { Name: "data" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                    Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                }
            }
        )";

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TString tableSchema = R"(
            Name: "ColumnTable"
            ColumnShardCount: 1
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
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterColumnTable(runtime, ++txId, "/MyRoot/OlapStore", R"(
            Name: "ColumnTable"
            AlterTtlSettings {
                UseTiering : "Tiering1"
            }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(StoreStats) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
        runtime.UpdateCurrentTime(TInstant::Now() - TDuration::Seconds(600));

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetLagForCompactionBeforeTierings(TDuration::Seconds(1));
        csController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);

        // disable stats batching
        auto& appData = runtime.GetAppData();
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        const TString& olapSchema = defaultStoreSchema;

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", olapSchema);
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathExist);
        TestLsPathId(runtime, 2, NLs::PathStringEqual("/MyRoot/OlapStore"));

        TString tableSchema = R"(
            Name: "ColumnTable"
            ColumnShardCount: 1
        )";

        TestCreateColumnTable(runtime, ++txId, "/MyRoot/OlapStore", tableSchema);
        env.TestWaitNotification(runtime, txId);

        ui64 pathId = 0;
        ui64 shardId = 0;
        ui64 planStep = 0;
        auto checkFn = [&](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            auto& self = record.GetPathDescription().GetSelf();
            pathId = self.GetPathId();
            txId = self.GetCreateTxId() + 1;
            planStep = self.GetCreateStep();
            auto& sharding = record.GetPathDescription().GetColumnTableDescription().GetSharding();
            UNIT_ASSERT_VALUES_EQUAL(sharding.ColumnShardsSize(), 1);
            shardId = sharding.GetColumnShards()[0];
            UNIT_ASSERT_VALUES_EQUAL(record.GetPath(), "/MyRoot/OlapStore/ColumnTable");
        };

        TestLsPathId(runtime, 3, checkFn);
        UNIT_ASSERT(shardId);
        UNIT_ASSERT(pathId);
        UNIT_ASSERT(planStep);
        {
            auto description = DescribePrivatePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/OlapStore/ColumnTable", true, true);
            Cerr << description.DebugString() << Endl;
            auto& tabletStats = description.GetPathDescription().GetTableStats();

            UNIT_ASSERT(description.GetPathDescription().HasTableStats());
            UNIT_ASSERT_EQUAL(tabletStats.GetRowCount(), 0);
            UNIT_ASSERT_EQUAL(tabletStats.GetDataSize(), 0);
        }


        ui32 rowsInBatch = 100000;

        {   // Write data directly into shard
            TActorId sender = runtime.AllocateEdgeActor();
            TString data = NTxUT::MakeTestBlob({0, rowsInBatch}, defaultYdbSchema, {}, { "timestamp" });

            ui64 writeId = 0;

            TSet<ui64> txIds;
            for (ui32 i = 0; i < 10; ++i) {
                std::vector<ui64> writeIds;
                NTxUT::WriteData(runtime, sender, shardId, ++writeId, pathId, data, defaultYdbSchema, &writeIds, NEvWrite::EModificationType::Upsert);
                NTxUT::ProposeCommit(runtime, sender, shardId, ++txId, writeIds);
                txIds.insert(txId);
            }

            NTxUT::PlanCommit(runtime, sender, shardId, ++planStep, txIds);

            // emulate timeout
            runtime.UpdateCurrentTime(TInstant::Now());

            // trigger periodic stats at shard (after timeout)
            std::vector<ui64> writeIds;
            NTxUT::WriteData(runtime, sender, shardId, ++writeId, pathId, data, defaultYdbSchema, &writeIds, NEvWrite::EModificationType::Upsert);
            NTxUT::ProposeCommit(runtime, sender, shardId, ++txId, writeIds);
            NTxUT::PlanCommit(runtime, sender, shardId, ++planStep, {txId});
        }
        csController->WaitIndexation(TDuration::Seconds(5));
        {
            auto description = DescribePrivatePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/OlapStore", true, true);
            auto& tabletStats = description.GetPathDescription().GetTableStats();

            UNIT_ASSERT_GT(tabletStats.GetRowCount(), 0);
            UNIT_ASSERT_GT(tabletStats.GetDataSize(), 0);
        }

        {
            auto description = DescribePrivatePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/OlapStore/ColumnTable", true, true);
            Cerr << description.DebugString() << Endl;
            auto& tabletStats = description.GetPathDescription().GetTableStats();

            UNIT_ASSERT_GT(tabletStats.GetRowCount(), 0);
            UNIT_ASSERT_GT(tabletStats.GetDataSize(), 0);
        }

#if 0
        TestDropColumnTable(runtime, ++txId, "/MyRoot/OlapStore", "ColumnTable");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore/ColumnTable", false, NLs::PathNotExist);
        TestLsPathId(runtime, 3, NLs::PathStringEqual(""));

        TestDropOlapStore(runtime, ++txId, "/MyRoot", "OlapStore");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/OlapStore", false, NLs::PathNotExist);
        TestLsPathId(runtime, 2, NLs::PathStringEqual(""));
#endif
    }
}
