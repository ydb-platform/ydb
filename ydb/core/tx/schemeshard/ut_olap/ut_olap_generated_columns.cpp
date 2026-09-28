#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/olap_helpers.h>

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace NKikimr {
namespace {

const TString ValidGeneratedTable = R"(
    Name: "GeneratedTable"
    ColumnShardCount: 1
    Schema {
        Columns { Name: "key" Type: "Uint64" NotNull: true }
        Columns { Name: "source" Type: "Int64" }
        Columns {
            Name: "derived"
            Type: "Int64"
            DefaultFromExpression {
                ExprText: "COALESCE(source, 0) * 2"
                DependencyColumnNames: "source"
                Stored: false
            }
        }
        KeyColumnNames: "key"
    }
)";

const NKikimrSchemeOp::TOlapColumnDescription& FindColumn(
    const NKikimrSchemeOp::TColumnTableSchema& schema, const TString& name)
{
    for (const auto& column : schema.GetColumns()) {
        if (column.GetName() == name) {
            return column;
        }
    }
    UNIT_FAIL("Column '" << name << "' was not found");
    return schema.GetColumns(0);
}

void CheckGeneratedDescriptor(TTestBasicRuntime& runtime, ui32 expectedPhysicalId = 0) {
    const auto describe = DescribePrivatePath(runtime, "/MyRoot/GeneratedTable");
    const auto& schema = describe.GetPathDescription().GetColumnTableDescription().GetSchema();

    const auto& generatedColumn = FindColumn(schema, "derived");
    UNIT_ASSERT_VALUES_EQUAL(generatedColumn.GetId(), 3u);
    UNIT_ASSERT(generatedColumn.HasDefaultFromExpression());
    const auto& generated = generatedColumn.GetDefaultFromExpression();
    UNIT_ASSERT_VALUES_EQUAL(generated.GetExprText(), "COALESCE(source, 0) * 2");
    UNIT_ASSERT_VALUES_EQUAL(generated.GetStored(), false);
    UNIT_ASSERT_VALUES_EQUAL(generated.DependencyColumnNamesSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(generated.GetDependencyColumnNames(0), "source");

    if (expectedPhysicalId) {
        UNIT_ASSERT_VALUES_EQUAL(FindColumn(schema, "physical_after_virtual").GetId(), expectedPhysicalId);
    }
}

Y_UNIT_TEST_SUITE(OlapGeneratedVirtualColumns) {
    Y_UNIT_TEST(CreatePersistsAndUnrelatedAlterKeepsLogicalIds) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(true);
        ui64 txId = 100;

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", ValidGeneratedTable);
        env.TestWaitNotification(runtime, txId);
        CheckGeneratedDescriptor(runtime);

        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        CheckGeneratedDescriptor(runtime);

        // Loading and altering an existing table must not depend on the creation flag.
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(false);
        TestAlterColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "GeneratedTable"
            AlterSchema {
                AddColumns { Name: "physical_after_virtual" Type: "Uint64" }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        CheckGeneratedDescriptor(runtime, 4);
    }

    Y_UNIT_TEST(RejectsInvalidGeneratedDefinitions) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto reject = [&](const TString& name, const TString& generatedColumn, const TString& key = "key") {
            const TString request = TStringBuilder() << R"(
                Name: ")" << name << R"("
                ColumnShardCount: 1
                Schema {
                    Columns { Name: "key" Type: "Uint64" NotNull: true }
                    Columns { Name: "source" Type: "Int64" }
                )" << generatedColumn << R"(
                    KeyColumnNames: ")" << key << R"("
                }
            )";
            TestCreateColumnTable(runtime, ++txId, "/MyRoot", request,
                {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
        };

        const TString virtualPrefix = R"(
            Columns {
                Name: "derived"
                Type: "Int64"
                DefaultFromExpression {
                    ExprText: "source * 2"
        )";
        const TString virtualSuffix = R"(
                    Stored: false
                }
            }
        )";

        // The feature gate is checked only for new request parsing.
        reject("Disabled", virtualPrefix + R"(DependencyColumnNames: "source")" + virtualSuffix);

        runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(true);
        reject("Stored", virtualPrefix + R"(DependencyColumnNames: "source" Stored: true } })");
        reject("PrimaryKey", virtualPrefix + R"(DependencyColumnNames: "source")" + virtualSuffix, "derived");
        reject("MissingDependency", virtualPrefix + R"(DependencyColumnNames: "missing")" + virtualSuffix);
        reject("DuplicateDependency", virtualPrefix
            + R"(DependencyColumnNames: "source" DependencyColumnNames: "source")" + virtualSuffix);
        reject("SelfDependency", virtualPrefix + R"(DependencyColumnNames: "derived")" + virtualSuffix);
        reject("ScalarDefault", virtualPrefix + R"(DependencyColumnNames: "source")" + R"(
                    Stored: false
                }
                DefaultValue { Scalar { Int64: 1 } }
            }
        )");
        reject("Serializer", virtualPrefix + R"(DependencyColumnNames: "source")" + R"(
                    Stored: false
                }
                Serializer {}
            }
        )");
        reject("Accessor", virtualPrefix + R"(DependencyColumnNames: "source")" + R"(
                    Stored: false
                }
                DataAccessorConstructor {}
            }
        )");
        reject("Storage", virtualPrefix + R"(DependencyColumnNames: "source")" + R"(
                    Stored: false
                }
                StorageId: ""
            }
        )");
        reject("Family", virtualPrefix + R"(DependencyColumnNames: "source")" + R"(
                    Stored: false
                }
                ColumnFamilyName: "default"
            }
        )");
        reject("GeneratedDependency", virtualPrefix + R"(DependencyColumnNames: "other")" + virtualSuffix
            + R"(
                Columns {
                    Name: "other"
                    Type: "Int64"
                    DefaultFromExpression { ExprText: "source" DependencyColumnNames: "source" Stored: false }
                }
            )");
    }

    Y_UNIT_TEST(RejectsPhysicalReferencesAndOutOfScopeOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableGeneratedVirtual(true);
        ui64 txId = 100;

        const auto generatedSchema = [](const TString& name, const TString& tail) {
            return TStringBuilder() << R"(
                Name: ")" << name << R"("
                ColumnShardCount: 1
                Schema {
                    Columns { Name: "key" Type: "Uint64" NotNull: true }
                    Columns { Name: "source" Type: "Int64" }
                    Columns {
                        Name: "derived"
                        Type: "Int64"
                        DefaultFromExpression {
                            ExprText: "COALESCE(source, 0) * 2"
                            DependencyColumnNames: "source"
                            Stored: false
                        }
                    }
                    KeyColumnNames: "key"
                )" << tail;
        };

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", generatedSchema("TtlGenerated", R"(
                }
                TtlSettings { Enabled { ColumnName: "derived" ExpireAfterSeconds: 300 } }
            )"), {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", generatedSchema("IndexGenerated", R"(
                    Indexes {
                        Id: 4
                        Name: "derived_idx"
                        ClassName: "BLOOM_FILTER"
                        BloomFilter { ColumnIds: 3 FalsePositiveProbability: 0.01 }
                    }
                }
            )"), {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", generatedSchema("StatisticsGenerated", R"(
                }
                MultiColumnStatistics {
                    Name: "derived_stats"
                    ColumnNames: "derived"
                    Types: COUNT_MIN_SKETCH
                }
            )"), {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestCreateOlapStore(runtime, ++txId, "/MyRoot", R"(
            Name: "GeneratedStore"
            ColumnShardCount: 1
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "key" Type: "Uint64" NotNull: true }
                    Columns {
                        Name: "derived"
                        Type: "Int64"
                        DefaultFromExpression { ExprText: "key + 1" DependencyColumnNames: "key" Stored: false }
                    }
                    KeyColumnNames: "key"
                }
            }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestCreateColumnTable(runtime, ++txId, "/MyRoot", ValidGeneratedTable);
        env.TestWaitNotification(runtime, txId);

        TestAlterColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "GeneratedTable"
            AlterSchema {
                AddColumns {
                    Name: "added_generated"
                    Type: "Int64"
                    DefaultFromExpression { ExprText: "source + 1" DependencyColumnNames: "source" Stored: false }
                }
            }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestAlterColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "GeneratedTable"
            AlterSchema { DropColumns { Name: "source" } }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestAlterColumnTable(runtime, ++txId, "/MyRoot", R"(
            Name: "GeneratedTable"
            AlterSchema { AlterColumns { Name: "derived" StorageId: "hot" } }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
    }
}

} // namespace
} // namespace NKikimr
