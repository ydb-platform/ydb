#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/schemeshard_user_attr_limits.h>
#include <ydb/core/tx/schemeshard/user_attributes.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>

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

    // The root database has no SysView Processor, so it can never produce detailed
    // metrics, and TTxInit does not restore the root domain from its SubDomains
    // row. TABLES_METRICS_LEVEL is therefore rejected there for every value,
    // including the ones that mean "off": there is nothing to turn off or clear.
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

    // CREATE validates before it touches the database. Getting this wrong does
    // not produce an error reply: the failed Propose trips the
    // IsUndoChangesSafe() verify in IgniteOperation and aborts the Scheme Shard.
    Y_UNIT_TEST(CreateDatabaseTablesMetricsLevelNotAllowedFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(false);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf("%sTablesMetricsLevel: %u", SubDomainSettings,
                ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable)),
            {NKikimrScheme::StatusInvalidParameter});

        // The Scheme Shard is still alive and the rejected database was not created
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {NLs::PathNotExist});

        // A subsequent valid request on the same Scheme Shard still works
        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf("%sTablesMetricsLevel: %u", SubDomainSettings,
                ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable)));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedTablesMetricsLevel(runtime, "/MyRoot/USER_0"),
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));
    }
}

/**
 * Unit test for the well-known user attribute __monitoring_project_id, the
 * database attribute that names the Monitoring project detailed metrics are
 * shipped to. It is published with the database path, where DataShard picks it
 * up off the subdomain watch.
 */
Y_UNIT_TEST_SUITE(TSchemeShardDatabaseMonitoringProjectIdTest) {
    constexpr const char* SubDomainSettings =
        "PlanResolution: 50 "
        "Coordinators: 1 "
        "Mediators: 1 "
        "TimeCastBucketsPerMediator: 2 "
        "Name: \"USER_0\" ";

    const TString AttrName = TString(ATTR_MONITORING_PROJECT_ID);

    // An absent attribute and an empty value both mean "no project", which is
    // exactly how DataShard reads it
    TString GetPublishedMonitoringProjectId(TTestBasicRuntime& runtime, const TString& path) {
        const auto describeResult = DescribePath(runtime, path);
        for (const auto& attr : describeResult.GetPathDescription().GetUserAttributes()) {
            if (attr.GetKey() == AttrName) {
                return attr.GetValue();
            }
        }
        return "";
    }

    NKikimrSchemeOp::TAlterUserAttributes SetProjectId(const TString& projectId) {
        return AlterUserAttrs({{AttrName, projectId}});
    }

    NKikimrSchemeOp::TAlterUserAttributes DropProjectId() {
        return AlterUserAttrs({}, {AttrName});
    }

    Y_UNIT_TEST(AlterDatabaseMonitoringProjectId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        // No project id configured yet
        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0", SetProjectId("proj1"));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "proj1");

        // Persisted, so it survives a Scheme Shard restart
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "proj1");

        // An alter that says nothing about the project id keeps the current one
        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0", AlterUserAttrs({{"AttrA", "ValA"}}));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "proj1");

        // Changing it to another value overwrites the current one
        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0", SetProjectId("proj2"));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "proj2");
    }

    // Removing the attribute is how the project id is cleared
    Y_UNIT_TEST(AlterDatabaseMonitoringProjectIdDropped) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings, SetProjectId("proj1"));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "proj1");

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0", DropProjectId());
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");

        // The clear is persisted, the old value does not come back on restart
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");
    }

    // Dropping stays possible after the feature flag is turned off again, so a
    // disabled flag cannot strand a project id on a database
    Y_UNIT_TEST(AlterDatabaseMonitoringProjectIdDroppedFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings, SetProjectId("proj1"));
        env.TestWaitNotification(runtime, txId);

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(false);

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0", DropProjectId());
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");
    }

    // An explicit empty value is accepted and means the same as no attribute
    Y_UNIT_TEST(AlterDatabaseMonitoringProjectIdEmpty) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings, SetProjectId("proj1"));
        env.TestWaitNotification(runtime, txId);

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0", SetProjectId(""));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");
    }

    Y_UNIT_TEST(AlterDatabaseMonitoringProjectIdMaxLength) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        const TString projectId(TUserAttributesLimits::MaxMonitoringProjectIdLen, 'p');

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0", SetProjectId(projectId));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), projectId);
    }

    Y_UNIT_TEST(AlterDatabaseMonitoringProjectIdTooLongRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        const TString projectId(TUserAttributesLimits::MaxMonitoringProjectIdLen + 1, 'p');

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0",
            {NKikimrScheme::StatusInvalidParameter}, SetProjectId(projectId));

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");
    }

    // A value that would not survive being used as a metric label is rejected
    Y_UNIT_TEST(AlterDatabaseMonitoringProjectIdInvalidValueRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0",
            {NKikimrScheme::StatusInvalidParameter}, SetProjectId("proj 1"));

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");
    }

    Y_UNIT_TEST(AlterDatabaseMonitoringProjectIdNotAllowedFeatureFlagDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(false);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings);
        env.TestWaitNotification(runtime, txId);

        TestUserAttrs(runtime, ++txId, "/MyRoot", "USER_0",
            {NKikimrScheme::StatusInvalidParameter}, SetProjectId("proj1"));

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "");
    }

    // The root database has no SysView Processor, so it can never produce
    // detailed metrics. The project id is rejected there for every value,
    // including the empty one: there is nothing to label and nothing to clear.
    void VerifyRootDatabaseMonitoringProjectIdRejected(const TString& projectId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestUserAttrs(runtime, ++txId, "", "MyRoot",
            {NKikimrScheme::StatusInvalidParameter}, SetProjectId(projectId));

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot"), "");

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot"), "");
    }

    Y_UNIT_TEST(AlterRootDatabaseMonitoringProjectIdNotAllowed) {
        VerifyRootDatabaseMonitoringProjectIdRejected("proj1");
    }

    Y_UNIT_TEST(AlterRootDatabaseMonitoringProjectIdEmptyNotAllowed) {
        VerifyRootDatabaseMonitoringProjectIdRejected("");
    }

    // Only a database can ship detailed metrics, so the attribute is refused on
    // any other kind of path rather than sitting there doing nothing
    Y_UNIT_TEST(AlterDirMonitoringProjectIdNotAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestUserAttrs(runtime, ++txId, "/MyRoot", "DirA",
            {NKikimrScheme::StatusInvalidParameter}, SetProjectId("proj1"));

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/DirA"), "");
    }

    Y_UNIT_TEST(MkDirMonitoringProjectIdNotAllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA",
            {NKikimrScheme::StatusInvalidParameter}, SetProjectId("proj1"));

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateDatabaseWithMonitoringProjectId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings, SetProjectId("proj1"));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "proj1");
    }

    // CREATE validates before it touches the database. Getting this wrong does
    // not produce an error reply: the failed Propose trips the
    // IsUndoChangesSafe() verify in IgniteOperation and aborts the Scheme Shard.
    void VerifyCreateDatabaseMonitoringProjectIdRejected(
        bool detailedMetricsEnabled,
        const TString& projectId
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(detailedMetricsEnabled);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings,
            {NKikimrScheme::StatusInvalidParameter}, SetProjectId(projectId));

        // The Scheme Shard is still alive and the rejected database was not created
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {NLs::PathNotExist});

        // A subsequent valid request on the same Scheme Shard still works
        runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(true);

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", SubDomainSettings, SetProjectId("proj1"));
        env.TestWaitNotification(runtime, txId);

        UNIT_ASSERT_VALUES_EQUAL(GetPublishedMonitoringProjectId(runtime, "/MyRoot/USER_0"), "proj1");
    }

    Y_UNIT_TEST(CreateDatabaseMonitoringProjectIdNotAllowedFeatureFlagDisabled) {
        VerifyCreateDatabaseMonitoringProjectIdRejected(/* detailedMetricsEnabled */ false, "proj1");
    }

    Y_UNIT_TEST(CreateDatabaseMonitoringProjectIdTooLongRejected) {
        VerifyCreateDatabaseMonitoringProjectIdRejected(/* detailedMetricsEnabled */ true,
            TString(TUserAttributesLimits::MaxMonitoringProjectIdLen + 1, 'p'));
    }
}
