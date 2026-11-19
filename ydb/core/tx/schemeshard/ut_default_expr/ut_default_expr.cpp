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
    const TString& expectedExprText)
{
    const auto* column = FindColumn(describe, name);
    UNIT_ASSERT_C(column, "column '" << name << "' not found in describe result: " << describe.ShortDebugString());
    UNIT_ASSERT_C(column->HasDefaultFromExpression(),
        "column '" << name << "' has no expression default: " << column->ShortDebugString());

    const auto& defaultExpression = column->GetDefaultFromExpression();
    UNIT_ASSERT(defaultExpression.GetStored());
    UNIT_ASSERT(defaultExpression.GetWritable());
    UNIT_ASSERT_VALUES_EQUAL(defaultExpression.GetExprText(), expectedExprText);
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
              Stored: true Writable: true
          }
      }
      KeyColumnNames: ["key"]
)pb";

enum class ESchemaOperation {
    Create,
    AlterColumn,
    AddColumn,
};

void CheckMalformedDefaultExpression(ESchemaOperation operation, bool stored, bool hasDependency, const TString& error) {
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);
    EnableDefaultFromExpression(runtime);
    runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);
    runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(true);
    ui64 txId = 100;
    const bool alter = operation != ESchemaOperation::Create;

    if (alter) {
        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);
        if (operation == ESchemaOperation::AlterColumn) {
            TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
                Name: "Table"
                Columns { Name: "value" Type: "Uint64" }
            )pb");
            env.TestWaitNotification(runtime, txId);
        }
    }

    NKikimrSchemeOp::TTableDescription schema;
    schema.SetName("Table");
    if (!alter) {
        auto* key = schema.AddColumns();
        key->SetName("key");
        key->SetType("Uint32");
        schema.AddKeyColumnNames("key");
    }
    auto* column = schema.AddColumns();
    column->SetName("value");
    if (operation != ESchemaOperation::AlterColumn) {
        column->SetType("Uint64");
    }
    auto* expression = column->MutableDefaultFromExpression();
    expression->SetExprText("RandomNumber(1)");
    expression->SetStored(stored);
    expression->SetWritable(true);
    if (hasDependency) {
        expression->AddDependencyColumnNames("key");
    }

    if (alter) {
        TestAlterTable(runtime, ++txId, "/MyRoot", schema.DebugString(),
            {{NKikimrScheme::StatusInvalidParameter, error}});
    } else {
        TestCreateTable(runtime, ++txId, "/MyRoot", schema.DebugString(),
            {{NKikimrScheme::StatusSchemeError, error}});
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TSchemeShardDefaultExprTest) {
    Y_UNIT_TEST(CreateRejectsWritableVirtualColumn) {
        CheckMalformedDefaultExpression(ESchemaOperation::Create, false, false, "Writable expression column 'value' must be stored");
    }

    Y_UNIT_TEST(AlterRejectsWritableVirtualColumn) {
        CheckMalformedDefaultExpression(ESchemaOperation::AlterColumn, false, false, "Writable expression column 'value' must be stored");
    }

    Y_UNIT_TEST(AlterAddRejectsWritableVirtualColumn) {
        CheckMalformedDefaultExpression(ESchemaOperation::AddColumn, false, false, "Writable expression column 'value' must be stored");
    }

    Y_UNIT_TEST(CreateRejectsDefaultExpressionDependencies) {
        CheckMalformedDefaultExpression(ESchemaOperation::Create, true, true, "DEFAULT expression column 'value' cannot have dependencies");
    }

    Y_UNIT_TEST(AlterRejectsDefaultExpressionDependencies) {
        CheckMalformedDefaultExpression(ESchemaOperation::AlterColumn, true, true, "DEFAULT expression column 'value' cannot have dependencies");
    }

    Y_UNIT_TEST(AlterAddRejectsDefaultExpressionDependencies) {
        CheckMalformedDefaultExpression(ESchemaOperation::AddColumn, true, true, "DEFAULT expression column 'value' cannot have dependencies");
    }

    Y_UNIT_TEST(CreateTableWithDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        auto describe = DescribePath(runtime, "/MyRoot/Table");
        CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()");

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

    Y_UNIT_TEST(LegacyVirtualExpressionAccepted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(true);

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
        )pb");
        env.TestWaitNotification(runtime, txId);
        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "ts");
        UNIT_ASSERT(column && column->HasDefaultFromExpression());
        UNIT_ASSERT(!column->GetDefaultFromExpression().GetStored());
        UNIT_ASSERT(!column->GetDefaultFromExpression().GetWritable());
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
                      Stored: true Writable: true
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
                      Stored: true Writable: true
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
                      Stored: true Writable: true
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

    Y_UNIT_TEST(DropNotNullOnDefaultExpressionColumn) {
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
                      Stored: true Writable: true
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
        CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()");
    }

    Y_UNIT_TEST_FLAG(SetNotNullValidatesStoredRowsWithDefaultExpression, HasNull) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);
        ui64 txId = 100;
        const TString tablePath = "/MyRoot/Table";

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns {
                Name: "value" Type: "Uint64"
                DefaultFromExpression {
                    ExprText: "RandomNumber(1)"
                    Stored: true Writable: true
                }
            }
            KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        TVector<TCell> cells = {TCell::Make(ui32(1)), HasNull ? TCell() : TCell::Make(ui64(42))};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(cells, 1, 2), true);

        // A DEFAULT expression does not fill NULLs already stored in the table.
        const auto response = TestSetColumnConstraint(runtime, ++txId, TTestTxConfig::SchemeShard,
            "/MyRoot", tablePath, {"value"});
        UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), Ydb::StatusIds::SUCCESS, response.ShortDebugString());
        env.TestWaitNotification(runtime, txId);

        TestCheckColumnsNotNull(runtime, tablePath, {{"value", !HasNull}});
        CheckDefaultExpression(DescribePath(runtime, tablePath), "value", "RandomNumber(1)");
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), 1u);

        TVector<TCell> nullCells = {TCell::Make(ui32(2)), TCell()};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(nullCells, 1, 2), HasNull);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), HasNull ? 2u : 1u);

        TVector<TCell> nonNullCells = {TCell::Make(ui32(3)), TCell::Make(ui64(43))};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, tablePath,
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1, 2}, TSerializedCellMatrix(nonNullCells, 1, 2), true);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, tablePath), HasNull ? 3u : 2u);
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

    Y_UNIT_TEST_FLAG(AlterDefaultExpressionSurvivesAlterAndRestart, AddColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "key" Type: "Uint32"    }
              KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);

        if (!AddColumn) {
            TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
                Name: "Table"
                Columns { Name: "ts" Type: "Timestamp" }
            )pb");
            env.TestWaitNotification(runtime, txId);
        }

        NKikimrSchemeOp::TTableDescription schema;
        schema.SetName("Table");
        auto* column = schema.AddColumns();
        column->SetName("ts");
        if (AddColumn) {
            column->SetType("Timestamp");
        }
        auto* expression = column->MutableDefaultFromExpression();
        expression->SetExprText("CurrentUtcTimestamp()");
        expression->SetStored(true);
        expression->SetWritable(true);
        TestAlterTable(runtime, ++txId, "/MyRoot", schema.DebugString());
        env.TestWaitNotification(runtime, txId);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()");

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
            Name: "Table"
            Columns { Name: "other" Type: "Uint64" }
        )pb");
        env.TestWaitNotification(runtime, txId);
        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()");

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()");
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
                      Stored: true Writable: true
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
                      Stored: true
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
                      Stored: true Writable: true
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
                      Stored: true
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

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()");
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

        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/TableCopy"), "ts", "CurrentUtcTimestamp()");
    }

    Y_UNIT_TEST(CopyTableWithDefaultExpressionRejectedWhenFeatureDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        DisableDefaultFromExpression(runtime);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        // Copy revalidates the schema against the current feature flags.
        TestCopyTable(runtime, ++txId, "/MyRoot", "TableCopy", "/MyRoot/Table", NKikimrScheme::StatusSchemeError);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/TableCopy"), {NLs::PathNotExist});
        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()");
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
        CheckDefaultExpression(DescribePath(runtime, "/MyRoot/TableRenamed"), "ts", "CurrentUtcTimestamp()");
    }
}
