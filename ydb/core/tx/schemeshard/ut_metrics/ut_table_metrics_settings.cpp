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
 * Unit test for the logic in Scheme Shard, which configures detailed metrics settings
 * for individual tables.
 */
Y_UNIT_TEST_SUITE(TSchemeShardTableDetailedMetricsSettingsTest) {
    /**
     * Verify that CREATE TABLE without the detailed metrics level specified works correctly.
     *
     * @note This test also verifies that the detailed metrics settings are preserved
     *       across SchemeShard restarts.
     */
    Y_UNIT_TEST(CreateTableNoDetailedMetricsLevel) {
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
    Y_UNIT_TEST(AlterTableSourceNoDetailedMetricsLevelTargetNoDetailedMetricsLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

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
