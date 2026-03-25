#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

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
 * Unit test for the logic in Scheme Shard, which configures metrics settings
 * for individual tables.
 */
Y_UNIT_TEST_SUITE(TSchemeShardTableMetricsSettingsTest) {
    /**
     * Verify that CREATE TABLE without the metrics level specified works correctly.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across SchemeShard restarts.
     */
    Y_UNIT_TEST(CreateTableNoMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

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

        // Make sure the metrics settings are not configured for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(!tableDescription.HasMetricsSettings());
                },
            }
        );
    }

    /**
     * Verify that CREATE TABLE with the metrics settings explicitly dropped
     * is not allowed and fails with an error.
     */
    Y_UNIT_TEST(CreateTableDroppingMetricsSettingsNotAlllowed) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
                MetricsSettings {
                    NotConfigured {
                    }
                }
            )",
            {{
                NKikimrScheme::StatusInvalidParameter,
                "Unable to remove the metrics settings in CREATE TABLE",
            }}
        );
    }

    /**
     * Verify that CREATE TABLE fails correctly, when the given invalid metrics
     * level is specified in the request.
     *
     * @param[in] metricsLevel The metrics level to verify
     */
    void VerifyCreateTableInvalidMetricsLevel(
        NKikimrSchemeOp::TMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

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
                    MetricsSettings {
                        Configured {
                            MetricsLevel: %s
                        }
                    }
                )",
                NKikimrSchemeOp::TMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
            ),
            {{
                NKikimrScheme::StatusInvalidParameter,
                "Only DATABASE, TABLE and PARTITION metrics levels are supported",
            }}
        );
    }

    /**
     * Verify that CREATE TABLE fails correctly, when an invalid metrics level
     * is specified (UNSPECIFIED).
     */
    Y_UNIT_TEST(CreateTableInvalidMetricsLevelUnspecified) {
        VerifyCreateTableInvalidMetricsLevel(
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelUnspecified
        );
    }

    /**
     * Verify that CREATE TABLE fails correctly, when an invalid metrics level
     * is specified (DISABLED).
     */
    Y_UNIT_TEST(CreateTableInvalidMetricsLevelDisabled) {
        VerifyCreateTableInvalidMetricsLevel(
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelDisabled
        );
    }

    /**
     * Verify that CREATE TABLE works correctly, when the given valid metrics
     * level is specified in the request.
     *
     * @note This functions also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     *
     * @param[in] metricsLevel The metrics level to verify
     */
    void VerifyCreateTableValidMetricsLevel(
        NKikimrSchemeOp::TMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

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
                    MetricsSettings {
                        Configured {
                            MetricsLevel: %s
                        }
                    }
                )",
                NKikimrSchemeOp::TMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
            )
        );

        env.TestWaitNotification(runtime, 100);

        // Make sure the metrics settings are configured correctly for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [metricsLevel](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT(tableDescription.GetMetricsSettings().HasConfigured());
                    UNIT_ASSERT(!tableDescription.GetMetricsSettings().HasNotConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetMetricsSettings().GetConfigured().GetMetricsLevel(),
                        metricsLevel
                    );
                },
            }
        );
    }

    /**
     * Verify that CREATE TABLE works correctly with a valid metrics level (DATABASE).
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(CreateTableValidMetricsLevelDatabase) {
        VerifyCreateTableValidMetricsLevel(
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelDatabase
        );
    }

    /**
     * Verify that CREATE TABLE works correctly with a valid metrics level (TABLE).
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(CreateTableValidMetricsLevelTable) {
        VerifyCreateTableValidMetricsLevel(
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelTable
        );
    }

    /**
     * Verify that CREATE TABLE works correctly with a valid metrics level (PARTITION).
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(CreateTableValidMetricsLevelPartition) {
        VerifyCreateTableValidMetricsLevel(
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that ALTER TABLE without the metrics level specified works correctly,
     * when applied to a table, which does not have any metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoMetricsLevelTargetNoMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // First, create a table without any metrics settings
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

        // Second, execute ALTER TABLE without specifying metrics settings
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

        // Make sure the metrics settings are not configured for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(!tableDescription.HasMetricsSettings());
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE with the metrics level explicitly removed works correctly.
     *
     * @note This functions also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     *
     * @param[in] sourceHasMetricsLevel Indicates whether the source table has the metrics level configured
     */
    void VerifyAlterTableRemoveMetricsLevel(bool sourceHasMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // First, create a table with or without metrics settings configured
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
                    MetricsSettings {
                        Configured {
                            MetricsLevel: MetricsLevelPartition
                        }
                    }
                  )"
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE with the metrics settings explicitly removed
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            R"(
                Name: "TestTable"
                MetricsSettings {
                    NotConfigured {
                    }
                }
            )"
        );

        env.TestWaitNotification(runtime, 101);

        // Make sure the metrics settings are not configured for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(!tableDescription.HasMetricsSettings());
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE with the metrics level explicitly removed works correctly,
     * when applied to a table, which does not have any metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoMetricsLevelTargetRemoveMetricsLevel) {
        VerifyAlterTableRemoveMetricsLevel(false /* sourceHasMetricsLevel */);
    }

    /**
     * Verify that ALTER TABLE fails correctly, when the given invalid metrics
     * level is specified in the request.
     *
     * @param[in] metricsLevel The metrics level to verify
     */
    void VerifyAlterTableInvalidMetricsLevel(
        NKikimrSchemeOp::TMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // First, create a table without any metrics settings
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

        // Second, execute ALTER TABLE with an invalid metrics level
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            Sprintf(
                R"(
                    Name: "TestTable"
                    MetricsSettings {
                        Configured {
                            MetricsLevel: %s
                        }
                    }
                )",
                NKikimrSchemeOp::TMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
            ),
            {{
                NKikimrScheme::StatusInvalidParameter,
                "Only DATABASE, TABLE and PARTITION metrics levels are supported",
            }}
        );
    }

    /**
     * Verify that ALTER TABLE fails correctly, when an invalid metrics level
     * is specified (UNSPECIFIED).
     */
    Y_UNIT_TEST(AlterTableInvalidMetricsLevelUnspecified) {
        VerifyAlterTableInvalidMetricsLevel(
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelUnspecified
        );
    }

    /**
     * Verify that ALTER TABLE fails correctly, when an invalid metrics level
     * is specified (DISABLED).
     */
    Y_UNIT_TEST(AlterTableInvalidMetricsLevelDisabled) {
        VerifyAlterTableInvalidMetricsLevel(
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelDisabled
        );
    }

    /**
     * Verify that ALTER TABLE works correctly, when the given valid metrics
     * level is specified in the request.
     *
     * @note This functions also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     *
     * @param[in] sourceHasMetricsLevel Indicates whether the source table has the metrics level configured
     * @param[in] metricsLevel The metrics level to verify
     */
    void VerifyAlterTableValidMetricsLevel(
        bool sourceHasMetricsLevel,
        NKikimrSchemeOp::TMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // First, create a table with or without metrics settings configured
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
                        MetricsSettings {
                            Configured {
                                MetricsLevel: %s
                            }
                        }
                    )",
                    NKikimrSchemeOp::TMetricsSettings::EMetricsLevel_Name(
                        // NOTE: Use any valid level here, but it must be different
                        //       from the requested target level to be able to detect
                        //       the changes after ALTER TABLE is completed
                        (metricsLevel == NKikimrSchemeOp::TMetricsSettings::MetricsLevelPartition)
                            ? NKikimrSchemeOp::TMetricsSettings::MetricsLevelTable
                            : NKikimrSchemeOp::TMetricsSettings::MetricsLevelPartition
                    ).c_str()
                  )
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE with the metrics settings explicitly specified
        TestAlterTable(
            runtime,
            101,
            "/MyRoot",
            Sprintf(
                R"(
                    Name: "TestTable"
                    MetricsSettings {
                        Configured {
                            MetricsLevel: %s
                        }
                    }
                )",
                NKikimrSchemeOp::TMetricsSettings::EMetricsLevel_Name(metricsLevel).c_str()
            )
        );

        env.TestWaitNotification(runtime, 101);

        // Make sure the metrics settings are configured correctly for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [metricsLevel](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT(tableDescription.GetMetricsSettings().HasConfigured());
                    UNIT_ASSERT(!tableDescription.GetMetricsSettings().HasNotConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetMetricsSettings().GetConfigured().GetMetricsLevel(),
                        metricsLevel
                    );
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid metrics level (DATABASE),
     * when applied to a table, which does not have any metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoMetricsLevelTargetValidMetricsLevelDatabase) {
        VerifyAlterTableValidMetricsLevel(
            false /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelDatabase
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid metrics level (TABLE),
     * when applied to a table, which does not have any metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoMetricsLevelTargetValidMetricsLevelTable) {
        VerifyAlterTableValidMetricsLevel(
            false /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelTable
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid metrics level (PARTITION),
     * when applied to a table, which does not have any metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceNoMetricsLevelTargetValidMetricsLevelPartition) {
        VerifyAlterTableValidMetricsLevel(
            false /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelPartition
        );
    }

    /**
     * Verify that ALTER TABLE without the metrics level specified works correctly,
     * when applied to a table, which has some metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithMetricsLevelTargetNoMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // First, create a table with some metrics settings configured
        TestCreateTable(
            runtime,
            100,
            "/MyRoot",
            R"(
                Name: "TestTable"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "String" }
                KeyColumnNames: ["key"]
                MetricsSettings {
                    Configured {
                        MetricsLevel: MetricsLevelPartition
                    }
                }
            )"
        );

        env.TestWaitNotification(runtime, 100);

        // Second, execute ALTER TABLE without specifying metrics settings
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

        // Make sure the metrics settings are configured correctly for this table
        VerifyTableDescriptionAndRestartSchemeShard(
            runtime,
            "/MyRoot/TestTable",
            {
                NLs::PathExist,
                [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& tableDescription = record.GetPathDescription().GetTable();

                    UNIT_ASSERT(tableDescription.HasMetricsSettings());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetMetricsSettings().GetStatusCase(),
                        NKikimrSchemeOp::TMetricsSettings::kConfigured
                    );

                    UNIT_ASSERT(tableDescription.GetMetricsSettings().HasConfigured());
                    UNIT_ASSERT(!tableDescription.GetMetricsSettings().HasNotConfigured());

                    UNIT_ASSERT_EQUAL(
                        tableDescription.GetMetricsSettings().GetConfigured().GetMetricsLevel(),
                        NKikimrSchemeOp::TMetricsSettings::MetricsLevelPartition
                    );
                },
            }
        );
    }

    /**
     * Verify that ALTER TABLE with the metrics level explicitly removed works correctly,
     * when applied to a table, which has some metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithMetricsLevelTargetRemoveMetricsLevel) {
        VerifyAlterTableRemoveMetricsLevel(true /* sourceHasMetricsLevel */);
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid metrics level (DATABASE),
     * when applied to a table, which has some metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithMetricsLevelTargetValidMetricsLevelDatabase) {
        VerifyAlterTableValidMetricsLevel(
            true /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelDatabase
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid metrics level (TABLE),
     * when applied to a table, which has some metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithMetricsLevelTargetValidMetricsLevelTable) {
        VerifyAlterTableValidMetricsLevel(
            true /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelTable
        );
    }

    /**
     * Verify that ALTER TABLE works correctly with a valid metrics level (PARTITION),
     * when applied to a table, which has some metrics settings configured.
     *
     * @note This test also verifies that the metrics settings are preserved
     *       across Scheme Shard restarts.
     */
    Y_UNIT_TEST(AlterTableSourceWithMetricsLevelTargetValidMetricsLevelPartition) {
        VerifyAlterTableValidMetricsLevel(
            true /* sourceHasMetricsLevel */,
            NKikimrSchemeOp::TMetricsSettings::MetricsLevelPartition
        );
    }
}
