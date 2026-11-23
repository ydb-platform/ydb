#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/string/builder.h>

#include <optional>

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

const NKikimrSchemeOp::TColumnDescription* FindColumn(const NKikimrSchemeOp::TTableDescription& table, const TString& name) {
    for (const auto& column : table.GetColumns()) {
        if (column.GetName() == name) {
            return &column;
        }
    }
    return nullptr;
}

const NKikimrSchemeOp::TColumnDescription* FindColumn(const NKikimrScheme::TEvDescribeSchemeResult& describe, const TString& name) {
    return FindColumn(describe.GetPathDescription().GetTable(), name);
}

NKikimrSchemeOp::TTableDescription GetDataShardTableDescription(TTestActorRuntime& runtime, const TString& path) {
    const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, path);
    UNIT_ASSERT_VALUES_EQUAL_C(shards.size(), 1u, path);

    const auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(shards.front(), sender, new TEvDataShard::TEvGetInfoRequest(), 0, GetPipeConfigWithRetries());
    const auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(sender);
    const auto& info = response->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL_C(info.GetUserTables().size(), 1, info.ShortDebugString());
    return info.GetUserTables(0).GetDescription();
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
        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* existingColumn = FindColumn(describe, "value");
        if (operation == ESchemaOperation::AlterColumn) {
            UNIT_ASSERT(existingColumn && !existingColumn->HasDefaultFromExpression());
        } else {
            UNIT_ASSERT(!existingColumn);
        }
    } else {
        TestCreateTable(runtime, ++txId, "/MyRoot", schema.DebugString(),
            {{NKikimrScheme::StatusSchemeError, error}});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {NLs::PathNotExist});
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
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr,
            { { NKikimrScheme::StatusSchemeError, "DEFAULT expressions are disabled. Column: ts" } });
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {NLs::PathNotExist});
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

    Y_UNIT_TEST(AlterColumnFamilyPreservesDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;
        const TString tablePath = "/MyRoot/Table";
        TestCreateTable(runtime, ++txId, "/MyRoot", TStringBuilder() << TableWithDefaultExpr << R"pb(
              PartitionConfig {
                  ColumnFamilies { Id: 0 StorageConfig { SysLog {} Log {} } }
                  ColumnFamilies { Id: 1 Name: "alt" }
              }
        )pb");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "ts" FamilyName: "alt" }
        )pb");
        env.TestWaitNotification(runtime, txId);

        const auto describe = DescribePath(runtime, tablePath);
        const auto* column = FindColumn(describe, "ts");
        UNIT_ASSERT(column);
        UNIT_ASSERT_VALUES_EQUAL(column->GetFamily(), 1u);
        CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()");

        const auto shardSchema = GetDataShardTableDescription(runtime, tablePath);
        const auto* shardColumn = FindColumn(shardSchema, "ts");
        UNIT_ASSERT(shardColumn);
        UNIT_ASSERT_VALUES_EQUAL(shardColumn->GetFamily(), 1u);
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

        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto& ttl = describe.GetPathDescription().GetTable().GetTTLSettings();
        UNIT_ASSERT(ttl.HasEnabled());
        UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetColumnName(), "expires");
        CheckDefaultExpression(describe, "expires", "CurrentUtcTimestamp()");
    }

    Y_UNIT_TEST(AlterTableEnablesTtlOnDefaultExpressionColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              TTLSettings { Enabled { ColumnName: "ts" ExpireAfterSeconds: 3600 } }
        )pb");
        env.TestWaitNotification(runtime, txId);

        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto& ttl = describe.GetPathDescription().GetTable().GetTTLSettings();
        UNIT_ASSERT(ttl.HasEnabled());
        UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetColumnName(), "ts");
        UNIT_ASSERT_VALUES_EQUAL(ttl.GetEnabled().GetExpireAfterSeconds(), 3600u);
        CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()");
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

    Y_UNIT_TEST(DirectSetNotNullOnDefaultExpressionColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "ts" NotNull: true }
        )pb", {{NKikimrScheme::StatusInvalidParameter, "Cannot set NotNull to true on column 'ts'"}});

        const auto describe = DescribePath(runtime, "/MyRoot/Table");
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
        const TString tablePath = "/MyRoot/Table";

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        const auto initialShardSchema = GetDataShardTableDescription(runtime, tablePath);
        const auto* shardColumn = FindColumn(initialShardSchema, "ts");
        UNIT_ASSERT(shardColumn && !shardColumn->HasDefaultFromExpression());

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              DropColumns { Name: "ts" }
        )pb");
        env.TestWaitNotification(runtime, txId);

        const auto describe = DescribePath(runtime, tablePath);
        UNIT_ASSERT(!FindColumn(describe, "ts"));
        UNIT_ASSERT(FindColumn(describe, "a"));

        const auto shardSchema = GetDataShardTableDescription(runtime, tablePath);
        UNIT_ASSERT(!FindColumn(shardSchema, "ts"));
        UNIT_ASSERT(FindColumn(shardSchema, "a"));
    }

    Y_UNIT_TEST(DropUnrelatedColumnPreservesDefaultExpression) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              DropColumns { Name: "a" }
        )pb");
        env.TestWaitNotification(runtime, txId);

        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        UNIT_ASSERT(!FindColumn(describe, "a"));
        CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()");
    }

    Y_UNIT_TEST(SetAndDropLiteralDefaultOnUnrelatedColumn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAddColumsWithDefaults(true));
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns {
                  Name: "a"
                  DefaultFromLiteral {
                      type { optional_type { item { type_id: INT32 } } }
                      value { items { int32_value: 42 } }
                  }
              }
        )pb");
        env.TestWaitNotification(runtime, txId);

        {
            const auto describe = DescribePath(runtime, "/MyRoot/Table");
            const auto* column = FindColumn(describe, "a");
            UNIT_ASSERT(column && column->HasDefaultFromLiteral());
            CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()");
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "a" EmptyDefault: NULL_VALUE }
        )pb");
        env.TestWaitNotification(runtime, txId);

        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "a");
        UNIT_ASSERT(column);
        UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultValueCase(), NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET);
        CheckDefaultExpression(describe, "ts", "CurrentUtcTimestamp()");
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

        TVector<TCell> cells = {TCell::Make(ui32(1))};
        WriteOp(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot/Table",
            0, NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT,
            {1}, TSerializedCellMatrix(cells, 1, 1), true);

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
        if (AddColumn) {
            for (const bool featureEnabled : {true, false}) {
                runtime.GetAppData().FeatureFlags.SetEnableDefaultFromExpression(featureEnabled);
                TestAlterTable(runtime, ++txId, "/MyRoot", schema.DebugString(),
                    {{NKikimrScheme::StatusInvalidParameter, "Adding a column with a DEFAULT expression is not supported"}});
                UNIT_ASSERT(!FindColumn(DescribePath(runtime, "/MyRoot/Table"), "ts"));
                UNIT_ASSERT_VALUES_EQUAL(CountRows(runtime, "/MyRoot/Table"), 1u);
            }
            return;
        }
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

    Y_UNIT_TEST(RejectsEmptyDefaultExpressionText) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        for (const auto& text : {std::optional<TString>{}, std::optional<TString>{""}, std::optional<TString>{" \t\r\n"}}) {
            NKikimrSchemeOp::TTableDescription schema;
            schema.SetName("Invalid");
            auto* column = schema.AddColumns();
            column->SetName("value");
            column->SetType("Uint64");
            schema.AddKeyColumnNames("value");
            auto* expression = column->MutableDefaultFromExpression();
            expression->SetStored(true);
            expression->SetWritable(true);
            if (text) {
                expression->SetExprText(*text);
            }
            TestCreateTable(runtime, ++txId, "/MyRoot", schema.DebugString(),
                {{NKikimrScheme::StatusSchemeError, "DEFAULT expression column 'value' must have non-empty expression text"}});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Invalid"), {NLs::PathNotExist});

            schema.SetName("Table");
            schema.ClearKeyColumnNames();
            column->SetName("ts");
            column->ClearType();
            TestAlterTable(runtime, ++txId, "/MyRoot", schema.DebugString(),
                {{NKikimrScheme::StatusInvalidParameter, "DEFAULT expression column 'ts' must have non-empty expression text"}});
            CheckDefaultExpression(DescribePath(runtime, "/MyRoot/Table"), "ts", "CurrentUtcTimestamp()");
        }
    }

    Y_UNIT_TEST(AlterExpressionDefaultPreservesSerialSequence) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);
        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"pb(
            TableDescription {
                Name: "Table"
                Columns { Name: "id" Type: "Uint64" DefaultFromSequence: "_serial_column_id" }
                KeyColumnNames: ["id"]
            }
            SequenceDescription { Name: "_serial_column_id" MinValue: 1 }
        )pb");
        env.TestWaitNotification(runtime, txId);
        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
            Name: "Table"
            Columns {
                Name: "id"
                DefaultFromExpression { ExprText: "RandomNumber(1)" Stored: true Writable: true }
            }
        )pb", {{NKikimrScheme::StatusInvalidParameter, "Cannot alter serial column 'id'"}});

        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "id");
        UNIT_ASSERT(column && column->HasDefaultFromSequence());
        UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultFromSequence(), "_serial_column_id");
        TestLs(runtime, "/MyRoot/Table/_serial_column_id",
            TDescribeOptionsBuilder().SetShowPrivateTable(true), NLs::PathExist);
    }

    Y_UNIT_TEST(AlterExpressionDefaultRejectsDisabledPgCompatibility) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(true).DisablePgCompatibility(false));
        EnableDefaultFromExpression(runtime);
        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", R"pb(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "pgint8" }
            KeyColumnNames: ["key"]
        )pb");
        env.TestWaitNotification(runtime, txId);
        runtime.GetAppData().FeatureFlags.SetDisablePgCompatibility(true);
        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
            Name: "Table"
            Columns {
                Name: "value"
                DefaultFromExpression { ExprText: "RandomNumber(1)" Stored: true Writable: true }
            }
        )pb", {{NKikimrScheme::StatusInvalidParameter, "PostgreSQL compatibility is disabled for column 'value'"}});
        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "value");
        UNIT_ASSERT(column && !column->HasDefaultFromExpression());
    }

    Y_UNIT_TEST_FLAG(AlterDropDefaultExpression, FeatureEnabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        EnableDefaultFromExpression(runtime);

        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", TableWithDefaultExpr);
        env.TestWaitNotification(runtime, txId);

        if (!FeatureEnabled) {
            DisableDefaultFromExpression(runtime);
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"pb(
              Name: "Table"
              Columns { Name: "ts" EmptyDefault: NULL_VALUE }
        )pb");
        env.TestWaitNotification(runtime, txId);

        const auto checkNoDefault = [&] {
            auto describe = DescribePath(runtime, "/MyRoot/Table");
            const auto* column = FindColumn(describe, "ts");
            UNIT_ASSERT(column);
            UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultValueCase(), NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET);
        };
        checkNoDefault();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        checkNoDefault();
    }

    Y_UNIT_TEST(AlterSetDefaultExpressionFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        DisableDefaultFromExpression(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedStored(true);

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
        const auto describe = DescribePath(runtime, "/MyRoot/Table");
        const auto* column = FindColumn(describe, "ts");
        UNIT_ASSERT(column);
        UNIT_ASSERT_VALUES_EQUAL(column->GetDefaultValueCase(), NKikimrSchemeOp::TColumnDescription::DEFAULTVALUE_NOT_SET);
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
