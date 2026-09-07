#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>

// TBuildIndexConfig holds a TVector<NYdb::NTable::TGlobalIndexSettings>, which helpers.h
// only forward-declares; constructing one needs the complete type.
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

/**
 * Validate the description for the given table, restart Scheme Shard
 * and make sure the table description is still valid.
 *
 * @param[in] runtime The test runtime
 * @param[in] tableName The name of the table to verify
 * @param[in] validTableChecks The validation checks to apply to the table description
 */
void VerifyTableDescriptionAndRestartSchemeShard(
    TTestBasicRuntime& runtime,
    const TString& tableName,
    const TVector<NLs::TCheckFunc>& validTableChecks
) {
    // First, validate the current table description
    auto describeResult = DescribePath(runtime, tableName);

    Cerr << "TEST TEvDescribeSchemeResult:" << Endl
        << describeResult.DebugString()
        << Endl;

    TestDescribeResult(describeResult, validTableChecks);

    // Restart Scheme Shard and make sure the metrics settings are still valid
    RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

    describeResult = DescribePath(runtime, tableName);

    Cerr << "TEST TEvDescribeSchemeResult after restarting Scheme Shard:" << Endl
        << describeResult.DebugString()
        << Endl;

    TestDescribeResult(describeResult, validTableChecks);
}

/**
 * Validate the description for the given PRIVATE table (e.g. an index impl table),
 * restart Scheme Shard and make sure the table description is still valid.
 *
 * @param[in] runtime The test runtime
 * @param[in] tableName The path of the private table to verify
 * @param[in] validTableChecks The validation checks to apply to the table description
 */
void VerifyPrivateTableDescriptionAndRestartSchemeShard(
    TTestBasicRuntime& runtime,
    const TString& tableName,
    const TVector<NLs::TCheckFunc>& validTableChecks
) {
    // First, validate the current table description
    auto describeResult = DescribePrivatePath(runtime, tableName);

    Cerr << "TEST TEvDescribeSchemeResult:" << Endl
        << describeResult.DebugString()
        << Endl;

    TestDescribeResult(describeResult, validTableChecks);

    // Restart Scheme Shard and make sure the metrics settings are still valid
    RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

    describeResult = DescribePrivatePath(runtime, tableName);

    Cerr << "TEST TEvDescribeSchemeResult after restarting Scheme Shard:" << Endl
        << describeResult.DebugString()
        << Endl;

    TestDescribeResult(describeResult, validTableChecks);
}

} // namespace <anonymous>

/**
 * Unit test for the logic in Scheme Shard, which configures detailed metrics settings
 * for individual tables.
 */
Y_UNIT_TEST_SUITE(TSchemeShardTableDetailedMetricsSettingsTest) {
    /**
     * Verify that CREATE TABLE without the detailed metrics level specified works correctly
     * regardless of EnableDataShardDetailedMetrics feature flag state.
     */
    Y_UNIT_TEST_TWIN(CreateTableNoDetailedMetricsLevel, EnableDetailedMetrics) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(EnableDetailedMetrics);

        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
            )"
        );

        env.TestWaitNotification(runtime, 100);

        // Make sure the detailed metrics settings are not configured for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(!tableDescription.HasDetailedMetricsSettings());
                },
            }
        );
    }

    /**
     * Verify that CREATE TABLE with the detailed metrics settings explicitly dropped
     * is not allowed and fails with an error.
     */
    Y_UNIT_TEST(CreateTableDroppingDetailedMetricsSettingsNotAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
                DetailedMetricsSettings {
                    NotConfigured {
                    }
                }
            )",
            {{
                NKikimrScheme::StatusInvalidParameter,
                "Unable to remove the detailed metrics settings in CREATE TABLE",
            }}
        );
    }

    /**
     * Verify that CREATE TABLE fails correctly, when an invalid metrics level
     * is specified (UNSPECIFIED).
     */
    Y_UNIT_TEST(CreateTableInvalidDetailedMetricsLevelUnspecified) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
                DetailedMetricsSettings {
                    Configured {
                        MetricsLevel: MetricsLevelUnspecified
                    }
                }
            )",
            {{
                NKikimrScheme::StatusInvalidParameter,
                "Only DISABLED, TABLE and PARTITION detailed metrics levels are supported",
            }}
        );
    }

    /**
     * Verify that CREATE TABLE fails correctly, when the given detailed metrics
     * level (or an explicit "drop") is specified in the request and
     * the EnableDataShardDetailedMetrics feature flag is disabled.
     *
     * @param[in] metricsLevel The detailed metrics level to verify (unset == use drop)
     */
    void VerifyCreateTableWithDetailedMetricsFlagDisabled(
        std::optional<NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel> metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(false);

        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            (!metricsLevel)
                ? R"(
                    Name: "TestTable"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "String" }
                    KeyColumnNames: ["key"]
                    DetailedMetricsSettings {
                        NotConfigured {
                        }
                    }
                )"
                : Sprintf(
                    R"(
                        Name: "TestTable"
                        Columns { Name: "key"   Type: "Uint64" }
                        Columns { Name: "value" Type: "String" }
                        KeyColumnNames: ["key"]
                        DetailedMetricsSettings {
                            Configured {
                                MetricsLevel: %s
                            }
                        }
                    )",
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(*metricsLevel).c_str()
                ),
            {{
                NKikimrScheme::StatusInvalidParameter,
                "The detailed metrics settings are specified in the request, "
                "but the detailed metrics feature is disabled by the corresponding "
                "feature flag (EnableDataShardDetailedMetrics)",
            }}
        );
    }

    /**
     * Verify that CREATE TABLE fails correctly, with different detailed metrics levels
     * and the EnableDataShardDetailedMetrics feature flag disabled.
     */
    Y_UNIT_TEST(CreateTableDroppingDetailedMetricsSettingsNotAllowedFeatureFlagDisabled) {
        VerifyCreateTableWithDetailedMetricsFlagDisabled({});
    }

    Y_UNIT_TEST(CreateTableDetailedMetricsLevelUnspecifiedNotAllowedFeatureFlagDisabled) {
        VerifyCreateTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified
        );
    }

    Y_UNIT_TEST(CreateTableDetailedMetricsLevelDisabledNotAllowedFeatureFlagDisabled) {
        VerifyCreateTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    Y_UNIT_TEST(CreateTableDetailedMetricsLevelTableNotAllowedFeatureFlagDisabled) {
        VerifyCreateTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    Y_UNIT_TEST(CreateTableDetailedMetricsLevelPartitionNotAllowedFeatureFlagDisabled) {
        VerifyCreateTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that ALTER TABLE fails correctly, when the given detailed metrics
     * level (or an explicit "drop") is specified in the request and
     * the EnableDataShardDetailedMetrics feature flag is disabled.
     *
     * @param[in] metricsLevel The detailed metrics level to verify (unset == use drop)
     */
    void VerifyAlterTableWithDetailedMetricsFlagDisabled(
        std::optional<NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel> metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(false);

        // First, create a table without any detailed metrics settings
        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
            )"
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE with the detailed metrics settings explicitly specified
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            (!metricsLevel)
                ? R"(
                    Name: "TestTable"
                    DetailedMetricsSettings {
                        NotConfigured {
                        }
                    }
                )"
                : Sprintf(
                    R"(
                        Name: "TestTable"
                        DetailedMetricsSettings {
                            Configured {
                                MetricsLevel: %s
                            }
                        }
                    )",
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(*metricsLevel).c_str()
                ),
            {{
                NKikimrScheme::StatusInvalidParameter,
                "The detailed metrics settings are specified in the request, "
                "but the detailed metrics feature is disabled by the corresponding "
                "feature flag (EnableDataShardDetailedMetrics)",
            }}
        );
    }

    /**
     * Verify that CREATE TABLE fails correctly, with different detailed metrics levels
     * and the EnableDataShardDetailedMetrics feature flag disabled.
     */
    Y_UNIT_TEST(AlterTableDroppingDetailedMetricsSettingsNotAllowedFeatureFlagDisabled) {
        VerifyAlterTableWithDetailedMetricsFlagDisabled({});
    }

    Y_UNIT_TEST(AlterTableDetailedMetricsLevelUnspecifiedNotAllowedFeatureFlagDisabled) {
        VerifyAlterTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified
        );
    }

    Y_UNIT_TEST(AlterTableDetailedMetricsLevelDisabledNotAllowedFeatureFlagDisabled) {
        VerifyAlterTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    Y_UNIT_TEST(AlterTableDetailedMetricsLevelTableNotAllowedFeatureFlagDisabled) {
        VerifyAlterTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    Y_UNIT_TEST(AlterTableDetailedMetricsLevelPartitionNotAllowedFeatureFlagDisabled) {
        VerifyAlterTableWithDetailedMetricsFlagDisabled(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that CREATE TABLE works correctly, when the given valid
     * detailed metrics level is specified in the request.
     *
     * @note This functions also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     *
     * @param[in] metricsLevel The detailed metrics level to verify
     */
    void VerifyCreateTableValidDetailedMetricsLevel(
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            Sprintf(
                R"(
                    Name: "TestTable"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "String" }
                    KeyColumnNames: ["key"]
                    DetailedMetricsSettings {
                        Configured {
                            MetricsLevel: %s
                        }
                    }
                )",
                NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
            )
        );

        env.TestWaitNotification(runtime, 100);

        // Make sure the detailed metrics settings are configured correctly for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [metricsLevel](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT(tableDescription.GetDetailedMetricsSettings().HasConfigured());
                    UNIT_ASSERT(!tableDescription.GetDetailedMetricsSettings().HasNotConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                        metricsLevel
                    );
                },
            }
        );
    }

    /**
     * Verify that CREATE TABLE works correctly with a valid
     * detailed metrics level (DISABLED).
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(CreateTableValidDetailedMetricsLevelDisabled) {
        VerifyCreateTableValidDetailedMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    /**
     * Verify that CREATE TABLE works correctly with a valid
     * detailed metrics level (TABLE).
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(CreateTableValidDetailedMetricsLevelTable) {
        VerifyCreateTableValidDetailedMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    /**
     * Verify that CREATE TABLE works correctly with a valid
     * detailed metrics level (PARTITION).
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(CreateTableValidDetailedMetricsLevelPartition) {
        VerifyCreateTableValidDetailedMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that ALTER TABLE without the detailed metrics level specified works correctly,
     * when applied to a table, which does not have any detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST_TWIN(AlterTableSourceNoDetailedMetricsLevelTargetNoDetailedMetricsLevel, EnableDetailedMetrics) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(EnableDetailedMetrics);

        // First, create a table without any detailed metrics settings
        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
            )"
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE without specifying detailed metrics settings
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            R"(
                Name: "TestTable"
                DropColumns { Name: "value" }
            )"
        );

        env.TestWaitNotification(runtime, 101);

        // Make sure the detailed metrics settings are not configured for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(!tableDescription.HasDetailedMetricsSettings());
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE with the detailed metrics level explicitly removed
     * works correctly.
     *
     * @note This functions also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     *
     * @param[in] sourceHasMetricsLevel Indicates whether the source table has
     *                                  the detailed metrics level configured
     */
    void VerifyAlterTableRemoveDetailedMetricsLevel(bool sourceHasMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        // First, create a table with or without detailed metrics settings configured
        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            (!sourceHasMetricsLevel)
                ? R"(
                    Name: "TestTable"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "String" }
                    KeyColumnNames: ["key"]
                  )"
                : R"(
                    Name: "TestTable"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "String" }
                    KeyColumnNames: ["key"]
                    DetailedMetricsSettings {
                        Configured {
                            MetricsLevel: MetricsLevelPartition
                        }
                    }
                  )"
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE with the detailed metrics settings explicitly removed
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            R"(
                Name: "TestTable"
                DetailedMetricsSettings {
                    NotConfigured {
                    }
                }
            )"
        );

        env.TestWaitNotification(runtime, 101);

        // Make sure the detailed metrics settings are not configured for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(!tableDescription.HasDetailedMetricsSettings());
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE with the detailed metrics level explicitly removed
     * works correctly, when applied to a table, which does not have
     * any detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoDetailedMetricsLevelTargetRemoveDetailedMetricsLevel) {
        VerifyAlterTableRemoveDetailedMetricsLevel(false /* sourceHasMetricsLevel */);
    }

    /**
     * Verify that ALTER TABLE fails correctly, when an invalid detailed metrics level
     * is specified (UNSPECIFIED).
     */
    Y_UNIT_TEST(AlterTableInvalidDetailedMetricsLevelUnspecified) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        // First, create a table without any detailed metrics settings
        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
            )"
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE with an invalid detailed metrics level
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            R"(
                Name: "TestTable"
                DetailedMetricsSettings {
                    Configured {
                        MetricsLevel: MetricsLevelUnspecified
                    }
                }
            )",
            {{
                NKikimrScheme::StatusInvalidParameter,
                "Only DISABLED, TABLE and PARTITION detailed metrics levels are supported",
            }}
        );
    }

    /**
     * Verify that ALTER TABLE works correctly, when the given valid detailed metrics
     * level is specified in the request.
     *
     * @note This functions also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     *
     * @param[in] sourceHasMetricsLevel Indicates whether the source table has
     *                                  the detailed metrics level configured
     * @param[in] metricsLevel The metrics level to verify
     */
    void VerifyAlterTableValidDetailedMetricsLevel(
        bool sourceHasMetricsLevel,
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        // First, create a table with or without detailed metrics settings configured
        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            (!sourceHasMetricsLevel)
                ? R"(
                    Name: "TestTable"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "String" }
                    KeyColumnNames: ["key"]
                  )"
                : Sprintf(
                    R"(
                        Name: "TestTable"
                        Columns { Name: "key"   Type: "Uint64" }
                        Columns { Name: "value" Type: "String" }
                        KeyColumnNames: ["key"]
                        DetailedMetricsSettings {
                            Configured {
                                MetricsLevel: %s
                            }
                        }
                    )",
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(
                        // NOTE: Use any valid level here, but it must be different
                        //       from the requested target level to be able to detect
                        //       the changes after ALTER TABLE is completed
                        (metricsLevel == NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition)
                            ? NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
                            : NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
                    ).c_str()
                  )
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE with the detailed metrics settings explicitly specified
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            Sprintf(
                R"(
                    Name: "TestTable"
                    DetailedMetricsSettings {
                        Configured {
                            MetricsLevel: %s
                        }
                    }
                )",
                NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
            )
        );

        env.TestWaitNotification(runtime, 101);

        // Make sure the detailed metrics settings are configured correctly for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [metricsLevel](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT(tableDescription.GetDetailedMetricsSettings().HasConfigured());
                    UNIT_ASSERT(!tableDescription.GetDetailedMetricsSettings().HasNotConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                        metricsLevel
                    );
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid detailed metrics level (DISABLED),
     * when applied to a table, which does not have any detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoDetailedMetricsLevelTargetValidDetailedMetricsLevelDisabled) {
        VerifyAlterTableValidDetailedMetricsLevel(
            false /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid detailed metrics level (TABLE),
     * when applied to a table, which does not have any detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoDetailedMetricsLevelTargetValidDetailedMetricsLevelTable) {
        VerifyAlterTableValidDetailedMetricsLevel(
            false /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid detailed metrics level (PARTITION),
     * when applied to a table, which does not have any detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoDetailedMetricsLevelTargetValidDetailedMetricsLevelPartition) {
        VerifyAlterTableValidDetailedMetricsLevel(
            false /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that ALTER TABLE without the detailed metrics level specified works correctly,
     * when applied to a table, which has some detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithDetailedMetricsLevelTargetNoDetailedMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        // First, create a table with some detailed metrics settings configured
        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
                DetailedMetricsSettings {
                    Configured {
                        MetricsLevel: MetricsLevelPartition
                    }
                }
            )"
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE without specifying detailed metrics settings
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            R"(
                Name: "TestTable"
                DropColumns { Name: "value" }
            )"
        );

        env.TestWaitNotification(runtime, 101);

        // Make sure the detailed metrics settings are configured correctly for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT(tableDescription.GetDetailedMetricsSettings().HasConfigured());
                    UNIT_ASSERT(!tableDescription.GetDetailedMetricsSettings().HasNotConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
                    );
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE with the detailed metrics level explicitly removed
     * works correctly,  when applied to a table, which has some detailed metrics
     * settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithDetailedMetricsLevelTargetRemoveDetailedMetricsLevel) {
        VerifyAlterTableRemoveDetailedMetricsLevel(true /* sourceHasMetricsLevel */);
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid detailed metrics level (DISABLED),
     * when applied to a table, which has some detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithDetailedMetricsLevelTargetValidDetailedMetricsLevelDisabled) {
        VerifyAlterTableValidDetailedMetricsLevel(
            true /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid detailed metrics level (TABLE),
     * when applied to a table, which has some detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithDetailedMetricsLevelTargetValidDetailedMetricsLevelTable) {
        VerifyAlterTableValidDetailedMetricsLevel(
            true /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid detailed metrics level (PARTITION),
     * when applied to a table, which has some detailed metrics settings configured.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithDetailedMetricsLevelTargetValidDetailedMetricsLevelPartition) {
        VerifyAlterTableValidDetailedMetricsLevel(
            true /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that ALTER TABLE, which specifies the detailed metrics settings for a table
     * with a global secondary index, propagates the settings to the index impl table.
     *
     * @param[in] metricsLevel The detailed metrics level to verify
     */
    void VerifyAlterTablePropagatesToIndexImplTable(
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // The index impl table has no detailed metrics settings yet
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/UserDefinedIndex/indexImplTable"), {
            NLs::PathExist,
            [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                UNIT_ASSERT(!record.GetPathDescription().GetTable().HasDetailedMetricsSettings());
            },
        });

        TestAlterTable(
            runtime,
            ++txId,
            "/MyRoot",
            Sprintf(
                R"(
                    Name: "Table"
                    DetailedMetricsSettings {
                        Configured {
                            MetricsLevel: %s
                        }
                    }
                )",
                NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
            )
        );

        env.TestWaitNotification(runtime, txId);

        auto checkLevel = [metricsLevel](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            const auto& tableDescription = record.GetPathDescription().GetTable();

            UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
            );

            UNIT_ASSERT(tableDescription.GetDetailedMetricsSettings().HasConfigured());
            UNIT_ASSERT(!tableDescription.GetDetailedMetricsSettings().HasNotConfigured());

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                metricsLevel
            );
        };

        // The base table is configured correctly
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table",
            { NLs::PathExist, checkLevel }
        );

        // The index impl table inherited the same detailed metrics settings
        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            { NLs::PathExist, checkLevel }
        );
    }

    Y_UNIT_TEST(AlterTablePropagatesToIndexImplTableLevelTable) {
        VerifyAlterTablePropagatesToIndexImplTable(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    Y_UNIT_TEST(AlterTablePropagatesToIndexImplTableLevelPartition) {
        VerifyAlterTablePropagatesToIndexImplTable(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that a single ALTER TABLE, which both specifies the detailed metrics settings
     * and disables KEY_BLOOM_FILTER (routing through the local-bloom-drop branch of
     * CreateConsistentAlterTable, which returns before the common-sense-path branch), still
     * propagates the detailed metrics settings to a global secondary index's impl table.
     */
    Y_UNIT_TEST(AlterTableBloomDropBranchPropagatesToIndexImplTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);
        runtime.GetAppData().FeatureFlags.SetEnableLocalIndexAsSchemeObject(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
              PartitionConfig {
                ByKeyFilterPrefixes { PrefixLength: 1 FalsePositiveProbability: 0.01 }
              }
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
            }
            IndexDescription {
              Name: "idx_bloom_1"
              Type: EIndexTypeLocalBloomFilter
              State: EIndexStateReady
              KeyColumnNames: ["key"]
              BloomFilterDescription { FalsePositiveProbability: 0.01 }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DetailedMetricsSettings {
                Configured {
                    MetricsLevel: MetricsLevelPartition
                }
            }
            PartitionConfig {
                EnableFilterByKey: false
            }
        )");
        env.TestWaitNotification(runtime, txId);

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());
                    UNIT_ASSERT(tableDescription.GetDetailedMetricsSettings().HasConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
                    );
                },
            }
        );
    }

    /**
     * Verify that a single ALTER TABLE, which both specifies the detailed metrics settings
     * and adds a local bloom filter index (routing through the local-bloom-add branch of
     * CreateConsistentAlterTable, which returns before the common-sense-path branch), still
     * propagates the detailed metrics settings to a global secondary index's impl table.
     */
    Y_UNIT_TEST(AlterTableBloomAddBranchPropagatesToIndexImplTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);
        runtime.GetAppData().FeatureFlags.SetEnableLocalIndexAsSchemeObject(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DetailedMetricsSettings {
                Configured {
                    MetricsLevel: MetricsLevelPartition
                }
            }
            TableIndexes {
                Name: "idx_bloom_1"
                Type: EIndexTypeLocalBloomFilter
                KeyColumnNames: ["key"]
                BloomFilterDescription { FalsePositiveProbability: 0.01 }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());
                    UNIT_ASSERT(tableDescription.GetDetailedMetricsSettings().HasConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
                    );
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE, which explicitly removes the detailed metrics settings
     * from a table with a global secondary index, also clears the settings on the
     * index impl table.
     */
    Y_UNIT_TEST(AlterTableClearingDetailedMetricsSettingsClearsIndexImplTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DetailedMetricsSettings {
                Configured {
                    MetricsLevel: MetricsLevelPartition
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DetailedMetricsSettings {
                NotConfigured {
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto checkNoSettings = [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            UNIT_ASSERT(!record.GetPathDescription().GetTable().HasDetailedMetricsSettings());
        };

        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table",
            { NLs::PathExist, checkNoSettings }
        );

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            { NLs::PathExist, checkNoSettings }
        );
    }

    /**
     * Verify that CREATE TABLE with an indexed table and the detailed metrics level
     * specified on the base table seeds the same level on the global index impl table.
     *
     * @param[in] metricsLevel The detailed metrics level to verify
     */
    void VerifyCreateIndexedTableSeedsIndexImplTable(
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", Sprintf(
            R"(
                TableDescription {
                  Name: "Table"
                  Columns { Name: "key" Type: "Uint64" }
                  Columns { Name: "indexed" Type: "Uint64" }
                  KeyColumnNames: ["key"]
                  DetailedMetricsSettings {
                      Configured {
                          MetricsLevel: %s
                      }
                  }
                }
                IndexDescription {
                  Name: "UserDefinedIndex"
                  KeyColumnNames: ["indexed"]
                }
            )",
            NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
        ));

        env.TestWaitNotification(runtime, txId);

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            {
                NLs::PathExist,
                [metricsLevel](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                        metricsLevel
                    );
                },
            }
        );
    }

    Y_UNIT_TEST(CreateIndexedTableSeedsIndexImplTableLevelTable) {
        VerifyCreateIndexedTableSeedsIndexImplTable(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    Y_UNIT_TEST(CreateIndexedTableSeedsIndexImplTableLevelPartition) {
        VerifyCreateIndexedTableSeedsIndexImplTable(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that CREATE TABLE with a vector index and the detailed metrics level
     * specified on the base table seeds the same level on BOTH vector index impl tables
     * (the level table and the posting table).
     */
    Y_UNIT_TEST(CreateVectorIndexedTableSeedsIndexImplTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "covered" Type: "String" }
              KeyColumnNames: ["id"]
              DetailedMetricsSettings {
                  Configured {
                      MetricsLevel: MetricsLevelPartition
                  }
              }
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["embedding"]
              DataColumnNames: ["covered"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 4, levels: 5 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto checkLevel = [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            const auto& tableDescription = record.GetPathDescription().GetTable();

            UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
            );

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
            );
        };

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/vectors/idx_vector/indexImplLevelTable",
            { NLs::PathExist, checkLevel }
        );

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/vectors/idx_vector/indexImplPostingTable",
            { NLs::PathExist, checkLevel }
        );
    }

    /**
     * Verify that CREATE TABLE with a PREFIXED vector index and the detailed metrics level
     * specified on the base table seeds the same level on ALL THREE vector index impl tables
     * (the prefix table, the level table and the posting table).
     */
    Y_UNIT_TEST(CreatePrefixedVectorIndexedTableSeedsIndexImplTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "vectors"
              Columns { Name: "id" Type: "Uint64" }
              Columns { Name: "embedding" Type: "String" }
              Columns { Name: "prefix" Type: "String" }
              KeyColumnNames: ["id"]
              DetailedMetricsSettings {
                  Configured {
                      MetricsLevel: MetricsLevelPartition
                  }
              }
            }
            IndexDescription {
              Name: "idx_vector"
              KeyColumnNames: ["prefix", "embedding"]
              Type: EIndexTypeGlobalVectorKmeansTree
              VectorIndexKmeansTreeDescription: { Settings: { settings: { metric: DISTANCE_COSINE, vector_type: VECTOR_TYPE_FLOAT, vector_dimension: 1024 }, clusters: 4, levels: 5 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto checkLevel = [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            const auto& tableDescription = record.GetPathDescription().GetTable();

            UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
            );

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
            );
        };

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/vectors/idx_vector/indexImplPrefixTable",
            { NLs::PathExist, checkLevel }
        );

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/vectors/idx_vector/indexImplLevelTable",
            { NLs::PathExist, checkLevel }
        );

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/vectors/idx_vector/indexImplPostingTable",
            { NLs::PathExist, checkLevel }
        );
    }

    /**
     * Verify that building a global secondary index on a table, which already has the
     * detailed metrics level configured, seeds the same level on the new index impl table.
     */
    Y_UNIT_TEST(BuildIndexSeedsIndexImplTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "indexed" Type: "Uint64" }
            KeyColumnNames: ["key"]
            DetailedMetricsSettings {
                Configured {
                    MetricsLevel: MetricsLevelPartition
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", TBuildIndexConfig{
            "UserDefinedIndex", NKikimrSchemeOp::EIndexTypeGlobal, {"indexed"}, {}, {}
        });
        env.TestWaitNotification(runtime, txId);

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                        NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
                    );
                },
            }
        );
    }

    /**
     * Verify that building a VECTOR index on a table, which already has the detailed metrics
     * level configured, seeds the same level on BOTH new vector index impl tables (the level
     * table and the posting table). This covers the CreateBuildPropose path in
     * build_index__progress.cpp, distinct from the CreateIndexedTable path already covered by
     * CreateVectorIndexedTableSeedsIndexImplTables.
     */
    Y_UNIT_TEST(BuildVectorIndexSeedsIndexImplTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "vectors"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: ["id"]
            DetailedMetricsSettings {
                Configured {
                    MetricsLevel: MetricsLevelPartition
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/vectors", TBuildIndexConfig{
            "idx_vector", NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, {"embedding"}, {}, {}
        });
        env.TestWaitNotification(runtime, txId);

        auto checkLevel = [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
            const auto& tableDescription = record.GetPathDescription().GetTable();

            UNIT_ASSERT(tableDescription.HasDetailedMetricsSettings());

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetStatusCase(),
                NKikimrSchemeOp::TTableDetailedMetricsSettings::kConfigured
            );

            UNIT_ASSERT_EQUAL(
                tableDescription.GetDetailedMetricsSettings().GetConfigured().GetMetricsLevel(),
                NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
            );
        };

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/vectors/idx_vector/indexImplLevelTable",
            { NLs::PathExist, checkLevel }
        );

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/vectors/idx_vector/indexImplPostingTable",
            { NLs::PathExist, checkLevel }
        );
    }

    /**
     * Verify that an ALTER TABLE, which does not touch the detailed metrics settings,
     * does not affect the (unconfigured) detailed metrics settings of the index impl table.
     */
    Y_UNIT_TEST(AlterTableUnrelatedChangeDoesNotTouchIndexImplTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "add" Type: "Uint64" }
        )");
        env.TestWaitNotification(runtime, txId);

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    UNIT_ASSERT(!record.GetPathDescription().GetTable().HasDetailedMetricsSettings());
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE on an indexed table fails correctly, when the detailed
     * metrics settings are specified in the request and the EnableDataShardDetailedMetrics
     * feature flag is disabled, and that the index impl table is left untouched.
     */
    Y_UNIT_TEST(AlterIndexedTableDetailedMetricsNotAllowedFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(false);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(
            runtime,
            ++txId,
            "/MyRoot",
            R"(
                Name: "Table"
                DetailedMetricsSettings {
                    Configured {
                        MetricsLevel: MetricsLevelPartition
                    }
                }
            )",
            {{
                NKikimrScheme::StatusInvalidParameter,
                "The detailed metrics settings are specified in the request, "
                "but the detailed metrics feature is disabled by the corresponding "
                "feature flag (EnableDataShardDetailedMetrics)",
            }}
        );

        VerifyPrivateTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/Table/UserDefinedIndex/indexImplTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    UNIT_ASSERT(!record.GetPathDescription().GetTable().HasDetailedMetricsSettings());
                },
            }
        );
    }
}

/**
 * Unit test for the logic in Scheme Shard, which configures the database-wide default
 * detailed metrics level (TABLES_METRICS_LEVEL) for row/column tables and publishes it
 * in the subdomain description, where DataShard picks it up.
 */
Y_UNIT_TEST_SUITE(TSchemeShardDatabaseDetailedMetricsSettingsTest) {
    constexpr const char* SubDomainSettings =
        "PlanResolution: 50 "
        "Coordinators: 1 "
        "Mediators: 1 "
        "TimeCastBucketsPerMediator: 2 "
        "Name: \"USER_0\" ";

    ui32 GetPublishedTablesMetricsLevel(TTestBasicRuntime& runtime, const TString& path) {
        const auto describeResult = DescribePath(runtime, path);
        UNIT_ASSERT(describeResult.GetPathDescription().HasDomainDescription());
        return describeResult.GetPathDescription().GetDomainDescription().GetTablesMetricsLevel();
    }

    void VerifyAlterDatabaseTablesMetricsLevel(
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel level
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        // No database default configured yet
        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot/USER_0"),
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified));

        TestAlterSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf("%sTablesMetricsLevel: %u", SubDomainSettings, ui32(level)));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot/USER_0"), ui32(level));

        // The database default is persisted, so it survives a Scheme Shard restart
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot/USER_0"), ui32(level));

        // An ALTER that says nothing about the level keeps the current one
        TestAlterSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot/USER_0"), ui32(level));

        // Setting the level back to Unspecified clears the database default
        TestAlterSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf("%sTablesMetricsLevel: %u", SubDomainSettings,
                ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified)));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot/USER_0"),
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified));
    }

    Y_UNIT_TEST(AlterDatabaseTablesMetricsLevelDisabled) {
        VerifyAlterDatabaseTablesMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    Y_UNIT_TEST(AlterDatabaseTablesMetricsLevelTable) {
        VerifyAlterDatabaseTablesMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    Y_UNIT_TEST(AlterDatabaseTablesMetricsLevelPartition) {
        VerifyAlterDatabaseTablesMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    Y_UNIT_TEST(AlterDatabaseTablesMetricsLevelNotAllowedFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(false);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        TestAlterSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf("%sTablesMetricsLevel: %u", SubDomainSettings,
                ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable)),
            {NKikimrScheme::StatusInvalidParameter});
    }

    // The root database has no SysView Processor, so detailed metrics can never be
    // aggregated for it
    void VerifyAlterRootDatabaseTablesMetricsLevelRejected(
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel level
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);
        // So that the request is rejected by the root-database check, not by the
        // ALTER DATABASE gate
        runtime.GetAppData().FeatureFlags.SetEnableAlterDatabase(true);

        TestAlterSubDomain(runtime, ++txId, "/",
            Sprintf("Name: \"MyRoot\" TablesMetricsLevel: %u", ui32(level)),
            {NKikimrScheme::StatusInvalidParameter});

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot"),
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified));

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot"),
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified));
    }

    Y_UNIT_TEST(AlterRootDatabaseTablesMetricsLevelUnspecifiedNotAllowed) {
        VerifyAlterRootDatabaseTablesMetricsLevelRejected(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified
        );
    }

    Y_UNIT_TEST(AlterRootDatabaseTablesMetricsLevelDisabledNotAllowed) {
        VerifyAlterRootDatabaseTablesMetricsLevelRejected(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    Y_UNIT_TEST(AlterRootDatabaseTablesMetricsLevelTableNotAllowed) {
        VerifyAlterRootDatabaseTablesMetricsLevelRejected(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable
        );
    }

    Y_UNIT_TEST(AlterRootDatabaseTablesMetricsLevelPartitionNotAllowed) {
        VerifyAlterRootDatabaseTablesMetricsLevelRejected(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition
        );
    }

    Y_UNIT_TEST(CreateDatabaseWithTablesMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf("%sTablesMetricsLevel: %u", SubDomainSettings,
                ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable)));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot/USER_0"),
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));
    }
}
