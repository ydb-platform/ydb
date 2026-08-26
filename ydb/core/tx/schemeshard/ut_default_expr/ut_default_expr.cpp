#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

void EnableDefaultFromExpression(TTestActorRuntime& runtime) {
    runtime.GetAppData().FeatureFlags.SetEnableDefaultFromExpression(true);
    runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(false);
}

void DisableDefaultFromExpression(TTestActorRuntime& runtime) {
    runtime.GetAppData().FeatureFlags.SetEnableDefaultFromExpression(false);
}

const NKikimrSchemeOp::TColumnDescription* FindColumn(const NKikimrScheme::TEvDescribeSchemeResult& describe, const TString& name) {
    const auto& table = describe.GetPathDescription().GetTable();
    for (const auto& column : table.GetColumns()) {
        if (column.GetName() == name) {
            return &column;
        }
    }
    return nullptr;
}

void CheckDefaultExpression(const NKikimrScheme::TEvDescribeSchemeResult& describe, const TString& name,
    const TString& expectedExprText, const TString& expectedContext = "")
{
    const auto* column = FindColumn(describe, name);
    UNIT_ASSERT_C(column, "column '" << name << "' not found in describe result: " << describe.ShortDebugString());
    UNIT_ASSERT_C(column->HasDefaultFromExpression(),
        "column '" << name << "' has no expression default: " << column->ShortDebugString());

    const auto& defaultExpression = column->GetDefaultFromExpression();
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(defaultExpression.GetKind()),
        static_cast<int>(NKikimrSchemeOp::TDefaultExpressionColumnDescription::DEFAULT));
    UNIT_ASSERT_VALUES_EQUAL(defaultExpression.GetExprText(), expectedExprText);
    UNIT_ASSERT_VALUES_EQUAL(defaultExpression.GetContext(), expectedContext);
    UNIT_ASSERT_VALUES_EQUAL_C(defaultExpression.DependencyColumnNamesSize(), 0u,
        "a DEFAULT expression must not have dependencies: " << defaultExpression.ShortDebugString());
}

constexpr const char* TableWithDefaultExpr = R"pb(
      Name: "Table"
      Columns { Name: "key"   Type: "Uint32" }
      Columns { Name: "a"     Type: "Int32"  }
      Columns {
          Name: "ts"
          Type: "Timestamp"
          DefaultFromExpression {
              ExprText: "CurrentUtcTimestamp()"
              Kind: DEFAULT
              Context: "USE `/MyRoot`;"
          }
      }
      KeyColumnNames: ["key"]
)pb";

}   // namespace

Y_UNIT_TEST_SUITE(TSchemeShardDefaultExprTest) {
    Y_UNIT_TEST(CreateTableWithDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()", "USE `/MyRoot`;");

        const auto* keyColumn = FindColumn(describe, "key");
        UNIT_ASSERT(keyColumn && !keyColumn->HasDefaultFromExpression());
    }

    Y_UNIT_TEST(CreateFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        DisableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr,
            { { NKikimrScheme::StatusSchemeError, "DEFAULT expressions are disabled. Column: ts" } });
    }

    Y_UNIT_TEST(UnsetKindIsSchemaError) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns {
                  Name: "ts"
                  Type: "Timestamp"
                  DefaultFromExpression {
                      ExprText: "CurrentUtcTimestamp()"
                  }
              }
              KeyColumnNames: ["key"]
        )pb",
            { { NKikimrScheme::StatusSchemeError, "expression default of unknown kind" } });
    }

    // A generated column is computed from the row, a DEFAULT column is not, so only the former
    // is barred from the primary key
    Y_UNIT_TEST(DefaultExpressionOnKeyColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns {
                  Name: "key"
                  Type: "Uint64"
                  DefaultFromExpression {
                      ExprText: "RandomNumber(1)"
                      Kind: DEFAULT
                  }
              }
              Columns { Name: "a" Type: "Int32" }
              KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "key", "RandomNumber(1)");
    }

    Y_UNIT_TEST(ColumnFamilyOnDefaultExpressionColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns {
                  Name: "ts"
                  Type: "Timestamp"
                  Family: 1
                  DefaultFromExpression {
                      ExprText: "CurrentUtcTimestamp()"
                      Kind: DEFAULT
                  }
              }
              KeyColumnNames: ["key"]
              PartitionConfig {
                  ColumnFamilies { Id: 0 StorageConfig { SysLog { PreferredPoolKind: "hdd" } Log { PreferredPoolKind: "hdd" } Data { PreferredPoolKind: "hdd" } } }
                  ColumnFamilies { Id: 1 Name: "alt" StorageConfig { Data { PreferredPoolKind: "hdd" } } }
              }
        )pb");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "ts");
        UNIT_ASSERT(column && column->HasDefaultFromExpression());
        UNIT_ASSERT_VALUES_UNEQUAL(column->GetFamily(), 0u);
    }

    // TTL is rejected on generated columns because their value is derived; a DEFAULT column holds
    // an ordinary stored value and may be the TTL column
    Y_UNIT_TEST(TtlOnDefaultExpressionColumnAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns {
                  Name: "expires"
                  Type: "Timestamp"
                  DefaultFromExpression {
                      ExprText: "CurrentUtcTimestamp()"
                      Kind: DEFAULT
                  }
              }
              KeyColumnNames: ["key"]
              TTLSettings {
                  Enabled { ColumnName: "expires" ExpireAfterSeconds: 3600 }
              }
        )pb");
        env.TestWaitNotification(runtime, txId);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "expires", "CurrentUtcTimestamp()");
    }

    Y_UNIT_TEST(SetNotNullOnDefaultExpressionColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns {
                  Name: "ts"
                  Type: "Timestamp"
                  NotNull: true
                  DefaultFromExpression {
                      ExprText: "CurrentUtcTimestamp()"
                      Kind: DEFAULT
                  }
              }
              KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "ts" NotNull: false }
        )pb");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "ts");
        UNIT_ASSERT(column && !column->GetNotNull());
    }

    Y_UNIT_TEST(DropColumnWithDefaultExpressionAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              DropColumns { Name: "ts" }
        )pb");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(!FindColumn(describe, "ts"));
    }

    Y_UNIT_TEST(AlterSetDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32"    }
              Columns { Name: "ts"  Type: "Timestamp" }
              KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns {
                  Name: "ts"
                  DefaultFromExpression {
                      ExprText: "CurrentUtcTimestamp()"
                      Kind: DEFAULT
                      Context: "USE `/MyRoot`;"
                  }
              }
        )pb");
        env.TestWaitNotification(runtime, txId);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()", "USE `/MyRoot`;");
    }

    Y_UNIT_TEST(AlterDropDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "ts" EmptyDefault: NULL_VALUE }
        )pb");
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "ts");
        UNIT_ASSERT(column && !column->HasDefaultFromExpression());
    }

    Y_UNIT_TEST(AlterSetDefaultExpressionFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        DisableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32"    }
              Columns { Name: "ts"  Type: "Timestamp" }
              KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns {
                  Name: "ts"
                  DefaultFromExpression {
                      ExprText: "CurrentUtcTimestamp()"
                      Kind: DEFAULT
                  }
              }
        )pb",
            { { NKikimrScheme::StatusInvalidParameter, "DEFAULT expressions are disabled. Column: ts" } });
    }

    Y_UNIT_TEST(AlterSetDefaultExpressionOnGeneratedRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              Columns {
                  Name: "doubled"
                  Type: "Int32"
                  DefaultFromExpression {
                      ExprText: "a * 2"
                      Kind: GENERATED_STORED
                      DependencyColumnNames: ["a"]
                  }
              }
              KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns {
                  Name: "doubled"
                  DefaultFromExpression {
                      ExprText: "CurrentUtcTimestamp()"
                      Kind: DEFAULT
                  }
              }
        )pb",
            { { NKikimrScheme::StatusInvalidParameter, "Cannot alter the DEFAULT of generated column 'doubled'" } });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "doubled" EmptyDefault: NULL_VALUE }
        )pb",
            { { NKikimrScheme::StatusInvalidParameter, "Cannot alter the DEFAULT of generated column 'doubled'" } });
    }

    Y_UNIT_TEST(AlterAddGeneratedToExistingColumnStillRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "a"   Type: "Int32"  }
              KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns {
                  Name: "a"
                  DefaultFromExpression {
                      ExprText: "key + 1"
                      Kind: GENERATED_STORED
                      DependencyColumnNames: ["key"]
                  }
              }
        )pb",
            { { NKikimrScheme::StatusInvalidParameter,
                "Cannot add a generated (GENERATED ALWAYS AS) expression to the existing column 'a'" } });
    }

    Y_UNIT_TEST(DefaultExpressionSurvivesSchemeShardRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()", "USE `/MyRoot`;");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()", "USE `/MyRoot`;");
    }

    Y_UNIT_TEST(CopyTablePreservesDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot", "TableCopy", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/TableCopy"), "ts", "CurrentUtcTimestamp()", "USE `/MyRoot`;");
    }

    Y_UNIT_TEST(RenameTablePreservesDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestMoveTable(runtime, ++txId, "/MyRoot/Table", "/MyRoot/TableRenamed");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), { NLs::PathNotExist });
        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/TableRenamed"), "ts", "CurrentUtcTimestamp()", "USE `/MyRoot`;");
    }
}
