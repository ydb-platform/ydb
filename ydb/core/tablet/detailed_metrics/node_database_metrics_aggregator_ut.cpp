#include "ut_helpers.h"
#include "ut_sensors_json_builder.h"

#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

// Retrieve the private counters as a JSON string.
TString GetPrivateJsonForCounters(const NMonitoring::TDynamicCounters& counters) {
    TStringStream stream;

    counters.Accept(
        TString{},
        TString{},
        *(NMonitoring::CreateEncoder(
            &stream,
            NMonitoring::EFormat::JSON,
            "sensor",
            NMonitoring::TCountableBase::EVisibility::Private
        ).Get())
    );

    return stream.Str();
}

} // namespace <anonymous>

/**
 * Unit tests for the per-node per-database metrics aggregator class (TNodeDatabaseMetricsAggregator).
 */
Y_UNIT_TEST_SUITE(TNodeDatabaseMetricsAggregatorTest) {
    /**
     * Verify that the Tablet Counters Aggregator (leaders) exposes no detailed metrics,
     * if there are no tablets reporting detailed metrics.
     */
    Y_UNIT_TEST(NoDetailedMetricsLeaders) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        InitializeTabletCountersAggregator(runtime, false /* forFollowers */);

        // The "ydb_detailed_raw" counter group should contain no metrics
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(GetPrivateJsonForCounters(*detailedCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
R"json(
{
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator (followers) does not try to expose
     * detailed metrics at all.
     */
    Y_UNIT_TEST(NoDetailedMetricsFollowers) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        InitializeTabletCountersAggregator(runtime, true /* forFollowers */);

        // The "ydb_detailed_raw" counter group should not exist
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(!detailedCounters);
    }

    /**
     * Verify that the Tablet Counters Aggregator (leaders) does not process
     * any detailed metrics, if the EnableDetailedMetrics feature flag is disabled.
     */
    Y_UNIT_TEST(NoDetailedMetricsFeatureFlagDisabled) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(
            runtime,
            false /* forFollowers */,
            false /* enableDetailedMetrics */
        );

        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Send an update for detailed metrics, but it should not be processed
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig = {
            .TableId = 1234,
            .TablePath = "/Root/fake-db/fake-table",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 1, 101, 0, &tableMetricsConfig);

        // The "ydb_detailed_raw" counter group should contain no metrics
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(GetPrivateJsonForCounters(*detailedCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
R"json(
{
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator ignores detailed metrics
     * for new tables with the given low metrics level.
     *
     * @param[in] metricsLevel The metrics level to test with
     */
    void VerifyNoDetailedMetricsNewTableForLowMetricsLevel(
        NKikimrSchemeOp::TTableDetailedMetricsSettings::EMetricsLevel metricsLevel
    ) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Send detailed metrics with the given metrics level (too low)
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig = {
            .TableId = 1234,
            .TablePath = "/Root/fake-db/fake-table",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = metricsLevel,
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 1, 0, 20, &tableMetricsConfig);

        // The "ydb_detailed_raw" counter group should contain no metrics
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(GetPrivateJsonForCounters(*detailedCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
R"json(
{
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator ignores detailed metrics
     * for new tables with low metrics level (UNSPECIFIED).
     */
    Y_UNIT_TEST(NoDetailedMetricsNewTableForLowMetricsLevelUnspecified) {
        VerifyNoDetailedMetricsNewTableForLowMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator ignores detailed metrics
     * for new tables with low metrics level (DISABLED).
     */
    Y_UNIT_TEST(NoDetailedMetricsNewTableForLowMetricsLevelDisabled) {
        VerifyNoDetailedMetricsNewTableForLowMetricsLevel(
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator (leaders) correctly handles
     * the case when the TEvTabletCountersForgetTablet message is received
     * for an unknown follower ID or for an unknown tablet ID.
     */
    Y_UNIT_TEST(ForgetUnknownFollowerOrTablet) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Simulate 1 table with 1 partition and only leaders for each of them
        // and try to delete an unknown follower
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig = {
            .TableId = 1234,
            .TablePath = "/Root/fake-db/fake-table",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        auto expectedMetrics = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 1, 101, 0, &tableMetricsConfig);

        // Try deleting unknown tablet/follower
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 1, 123456789); // Unknown follower
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 987654321, 101); // Unknown tablet

        // The "ydb_detailed_raw" counter group should contain detailed metrics for this tables
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "1")
                                    .AddNestedGroup("follower_id", "101", expectedMetrics)
                                    .AddNestedGroup("follower_id", "replicas_only", expectedMetrics)
                                    .AddGroup(expectedMetrics)
                                .EndNested()
                            .EndNested()
                            .AddGroup(expectedMetrics)
                        .EndNested()
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator (leaders) handles detailed metrics
     * for Data Shard tables, which include only leaders
     * (including adding, updating and removing tablets).
     */
    Y_UNIT_TEST(DataShardLeaders) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // TEST 1: Simulate 2 tables with 3 partitions and only leaders for each of them
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig1 = {
            .TableId = 1234,
            .TablePath = "/Root/fake-db/fake-table1",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            // .MonitoringProjectId = ... - no monitoring project ID for this table
        };

        auto expected0x01 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x01, 0, 20, &tableMetricsConfig1);
        auto expected0x02 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x02, 0, 40, &tableMetricsConfig1);
        auto expected0x04 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x04, 0, 60, &tableMetricsConfig1);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 4321,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        auto expected0x10 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x10, 0, 10, &tableMetricsConfig2);
        auto expected0x20 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x20, 0, 30, &tableMetricsConfig2);
        auto expected0x40 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x40, 0, 50, &tableMetricsConfig2);

        auto emptyMetrics = EmptyDataShardMetrics();

        // The "ydb_detailed_raw" counter group should contain detailed metrics for all tables
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (initial):" << Endl << countersJson << Endl;

        auto makeExpectedJson = [&] () -> TString {

#undef AddExpected
#define AddExpected(tabletId, expectedMetrics) \
    AddNestedIf("tablet_id", tabletId, [&] () { return !expectedMetrics.empty(); }, [&] (auto json) { return json \
        .AddNestedGroup("follower_id", "0", expectedMetrics) \
        .AddNestedGroup("follower_id", "replicas_only", emptyMetrics) \
        .AddGroup(expectedMetrics); \
    })

        auto expectedTable1 = expected0x01 | expected0x02 | expected0x04;
            auto expectedTable2 = expected0x10 | expected0x20 | expected0x40;
            return NormalizeJson(TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .AddNestedIf("monitoring_project_id", "fake-monitoring-project-id",
                            [&] () { return !expectedTable2.empty(); }, [&] (auto json) { return json
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .AddExpected("16", expected0x10)
                                .AddExpected("32", expected0x20)
                                .AddExpected("64", expected0x40)
                            .EndNested()
                            .AddGroup(expectedTable2)
                        .EndNested();
                    })
                    .AddNestedIf("table", "/Root/fake-db/fake-table1",
                            [&] () { return !expectedTable1.empty(); }, [&] (auto json) { return json
                        .StartNested("detailed_metrics", "per_partition")
                            .AddExpected("1", expected0x01)
                            .AddExpected("2", expected0x02)
                            .AddExpected("4", expected0x04)
                        .EndNested()
                        .AddGroup(expectedTable1);
                    })
                .EndNested()
                .BuildJson()
            );
        };

        TString expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 2: Update metrics for the second partition for each table
        expected0x02 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x02, 0, 80, &tableMetricsConfig1, 0x08);
        expected0x20 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x20, 0, 60, &tableMetricsConfig2, 0x80);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 3: Remove metrics for the second partition for each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x02, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x20, 0);

        expected0x02.clear();
        expected0x20.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting leaders):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 4: Remove metrics for all remaining partitions in the first table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x01, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x04, 0);

        expected0x01.clear();
        expected0x04.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting table 1):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 5: Remove metrics for all remaining partitions in the second table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x10, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x40, 0);

        countersJson = NormalizeJson(GetPrivateJsonForCounters(*detailedCounters));
        Cerr << "TEST Current counters (after deleting table 2):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
R"json(
{
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator (leaders) handles detailed metrics
     * for Data Shard tables, which include only followers
     * (including adding, updating and removing tablets).
     */
    Y_UNIT_TEST(DataShardFollowers) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // TEST 1: Simulate 2 tables with 3 partitions and only followers for each of them
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig1 = {
            .TableId = 1234,
            .TablePath = "/Root/fake-db/fake-table1",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            // .MonitoringProjectId = ... - no monitoring project ID for this table
        };

        auto expected10101 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 10101, 10, &tableMetricsConfig1, 0x0001);
        auto expected20101 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101, 20, &tableMetricsConfig1, 0x0002);
        auto expected30101 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 30101, 30, &tableMetricsConfig1, 0x0004);

        auto expected10201 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 10201, 40, &tableMetricsConfig1, 0x0010);
        auto expected20201 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201, 50, &tableMetricsConfig1, 0x0020);
        auto expected30201 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 30201, 60, &tableMetricsConfig1, 0x0040);

        auto expected10301 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 10301, 70, &tableMetricsConfig1, 0x0100);
        auto expected20301 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301, 80, &tableMetricsConfig1, 0x0200);
        auto expected30301 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 30301, 90, &tableMetricsConfig1, 0x0400);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 4321,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        auto expected10102 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 10102, 10, &tableMetricsConfig2, 0x0002);
        auto expected20102 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102, 20, &tableMetricsConfig2, 0x0004);
        auto expected30102 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 30102, 30, &tableMetricsConfig2, 0x0008);

        auto expected10202 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 10202, 40, &tableMetricsConfig2, 0x0020);
        auto expected20202 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202, 50, &tableMetricsConfig2, 0x0040);
        auto expected30202 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 30202, 60, &tableMetricsConfig2, 0x0080);

        auto expected10302 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 10302, 70, &tableMetricsConfig2, 0x0200);
        auto expected20302 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302, 80, &tableMetricsConfig2, 0x0400);
        auto expected30302 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 30302, 90, &tableMetricsConfig2, 0x0800);

        // The "ydb_detailed_raw" counter group should contain detailed metrics for all tables
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (initial):" << Endl << countersJson << Endl;

        auto makeExpectedJson = [&] () -> TString {

#undef AddExpected
#define AddExpected(tabletId) \
    AddNestedIf("tablet_id", #tabletId, [&] () { return !expectedTablet##tabletId.empty(); }, [&] (auto json) { return json \
        .AddNestedGroup("follower_id", "10" #tabletId, expected10##tabletId) \
        .AddNestedGroup("follower_id", "20" #tabletId, expected20##tabletId) \
        .AddNestedGroup("follower_id", "30" #tabletId, expected30##tabletId) \
        .AddNestedGroup("follower_id", "replicas_only", expectedTablet##tabletId) \
        .AddGroup(expectedTablet##tabletId); \
    })

            auto expectedTablet101 = expected10101 | expected20101 | expected30101;
            auto expectedTablet201 = expected10201 | expected20201 | expected30201;
            auto expectedTablet301 = expected10301 | expected20301 | expected30301;
            auto expectedTable1 = expectedTablet101 | expectedTablet201 | expectedTablet301 ;
            auto expectedTablet102 = expected10102 | expected20102 | expected30102;
            auto expectedTablet202 = expected10202 | expected20202 | expected30202;
            auto expectedTablet302 = expected10302 | expected20302 | expected30302;
            auto expectedTable2 = expectedTablet102 | expectedTablet202 | expectedTablet302;
            return NormalizeJson(TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .AddNestedIf("monitoring_project_id", "fake-monitoring-project-id",
                            [&] () { return !expectedTable2.empty(); }, [&] (auto json) { return json
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .AddExpected(102)
                                .AddExpected(202)
                                .AddExpected(302)
                            .EndNested()
                            .AddGroup(expectedTable2)
                        .EndNested();
                    })
                    .AddNestedIf("table", "/Root/fake-db/fake-table1",
                            [&] () { return !expectedTable1.empty(); }, [&] (auto json) { return json
                        .StartNested("detailed_metrics", "per_partition")
                            .AddExpected(101)
                            .AddExpected(201)
                            .AddExpected(301)
                        .EndNested()
                        .AddGroup(expectedTable1);
                    })
                .EndNested()
                .BuildJson()
            );
        };

        TString expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 2: Update metrics for the second follower for each partition for each table
        expected20101 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101, 0, &tableMetricsConfig1, 0x0008);
        expected20201 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201, 0, &tableMetricsConfig1, 0x0080);
        expected20301 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301, 0, &tableMetricsConfig1, 0x0800);

        expected20102 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102, 0, &tableMetricsConfig2, 0x0001);
        expected20202 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202, 0, &tableMetricsConfig2, 0x0010);
        expected20302 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302, 0, &tableMetricsConfig2, 0x0100);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 3: Remove metrics for the second follower for each partition of each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 101, 20101);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 201, 20201);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 301, 20301);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 102, 20102);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 202, 20202);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 302, 20302);

        expected20101.clear();
        expected20201.clear();
        expected20301.clear();
    
        expected20102.clear();
        expected20202.clear();
        expected20302.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting followers):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 4: Remove metrics for the second partition in each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 201, 10201);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 201, 30201);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 202, 10202);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 202, 30202);

        expected10201.clear();
        expected30201.clear();

        expected10202.clear();
        expected30202.clear();
        
        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting partitions):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 5: Remove metrics for all remaining partitions in the first table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 101, 10101);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 101, 30101);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 301, 10301);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 301, 30301);

        expected10101.clear();
        expected30101.clear();

        expected10301.clear();
        expected30301.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting table 1):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 6: Remove metrics for all remaining partitions in the second table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 102, 10102);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 102, 30102);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 302, 10302);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 302, 30302);

        countersJson = NormalizeJson(GetPrivateJsonForCounters(*detailedCounters));
        Cerr << "TEST Current counters (after deleting table 2):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
R"json(
{
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator (leaders) handles detailed metrics
     * for Data Shard tables, which include both leaders and followers
     * (including adding, updating and removing tablets).
     */
    Y_UNIT_TEST(DataShardLeadersAndsFollowers) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // TEST 1: Simulate 2 tables with 3 partitions with the leader + 2 followers for each of them
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig1 = {
            .TableId = 1234,
            .TablePath = "/Root/fake-db/fake-table1",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            // .MonitoringProjectId = ... - no monitoring project ID for this table
        };

        auto expected101Leader = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101,     0,  10, &tableMetricsConfig1, 0x0001);
        auto expected10101 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 10101,  20, &tableMetricsConfig1, 0x0002);
        auto expected20101 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101,  30, &tableMetricsConfig1, 0x0004);

        auto expected201Leader = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201,     0,  40, &tableMetricsConfig1, 0x0010);
        auto expected10201 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 10201,  50, &tableMetricsConfig1, 0x0020);
        auto expected20201 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201,  60, &tableMetricsConfig1, 0x0040);

        auto expected301Leader = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301,     0,  70, &tableMetricsConfig1, 0x0100);
        auto expected10301 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 10301,  80, &tableMetricsConfig1, 0x0200);
        auto expected20301 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301,  90, &tableMetricsConfig1, 0x0400);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 4321,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        auto expected102Leader = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102,     0,  10, &tableMetricsConfig2, 0x0001);
        auto expected10102 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 10102,  20, &tableMetricsConfig2, 0x0002);
        auto expected20102 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102,  30, &tableMetricsConfig2, 0x0004);

        auto expected202Leader = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202,     0,  40, &tableMetricsConfig2, 0x0010);
        auto expected10202 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 10202,  50, &tableMetricsConfig2, 0x0020);
        auto expected20202 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202,  60, &tableMetricsConfig2, 0x0040);

        auto expected302Leader = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302,     0,  70, &tableMetricsConfig2, 0x0100);
        auto expected10302 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 10302,  80, &tableMetricsConfig2, 0x0200);
        auto expected20302 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302,  90, &tableMetricsConfig2, 0x0400);

        // The "ydb_detailed_raw" counter group should contain detailed metrics for all tables
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (initial):" << Endl << countersJson << Endl;

        auto makeExpectedJson = [&] () -> TString {

#undef AddExpected
#define AddExpected(tabletId) \
    AddNestedIf("tablet_id", #tabletId, [&] () { return !expectedTablet##tabletId.empty(); }, [&] (auto json) { return json \
        .AddNestedGroup("follower_id", "0", expected##tabletId##Leader) \
        .AddNestedGroup("follower_id", "10" #tabletId, expected10##tabletId) \
        .AddNestedGroup("follower_id", "20" #tabletId, expected20##tabletId) \
        .AddNestedGroup("follower_id", "replicas_only", expected10##tabletId | expected20##tabletId) \
        .AddGroup(expectedTablet##tabletId); \
    })

            auto expectedTablet101 = expected101Leader | expected10101 | expected20101;
            auto expectedTablet201 = expected201Leader | expected10201 | expected20201;
            auto expectedTablet301 = expected301Leader | expected10301 | expected20301;
            auto expectedTable1 = expectedTablet101 | expectedTablet201 | expectedTablet301;
            auto expectedTablet102 = expected102Leader | expected10102 | expected20102;
            auto expectedTablet202 = expected202Leader | expected10202 | expected20202;
            auto expectedTablet302 = expected302Leader | expected10302 | expected20302;
            auto expectedTable2 = expectedTablet102 | expectedTablet202 | expectedTablet302;
            return NormalizeJson(TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .AddNestedIf("monitoring_project_id", "fake-monitoring-project-id",
                            [&] () { return !expectedTable2.empty(); }, [&] (auto json) { return json
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .AddExpected(102)
                                .AddExpected(202)
                                .AddExpected(302)
                            .EndNested()
                            .AddGroup(expectedTable2)
                        .EndNested();
                    })
                    .AddNestedIf("table", "/Root/fake-db/fake-table1",
                            [&] () { return !expectedTable1.empty(); }, [&] (auto json) { return json
                        .StartNested("detailed_metrics", "per_partition")
                            .AddExpected(101)
                            .AddExpected(201)
                            .AddExpected(301)
                        .EndNested()
                        .AddGroup(expectedTable1);
                    })
                .EndNested()
                .BuildJson()
            );
        };

        TString expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 2: Update metrics for the second follower for each partition for each table
        expected20101 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101, 0, &tableMetricsConfig1, 0x0008);
        expected20201 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201, 0, &tableMetricsConfig1, 0x0080);
        expected20301 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301, 0, &tableMetricsConfig1, 0x0800);

        expected20102 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102, 0, &tableMetricsConfig2, 0x0008);
        expected20202 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202, 0, &tableMetricsConfig2, 0x0080);
        expected20302 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302, 0, &tableMetricsConfig2, 0x0800);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 3: Remove metrics for the second follower for each partition of each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 101, 20101);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 201, 20201);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 301, 20301);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 102, 20102);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 202, 20202);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 302, 20302);

        expected20101.clear();
        expected20201.clear();
        expected20301.clear();

        expected20102.clear();
        expected20202.clear();
        expected20302.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting followers):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 4: Remove metrics for the leader for each partition of each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 101, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 201, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 301, 0);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 102, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 202, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 302, 0);

        expected101Leader.clear();
        expected201Leader.clear();
        expected301Leader.clear();

        expected102Leader.clear();
        expected202Leader.clear();
        expected302Leader.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting leaders):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 5: Remove metrics for the second partition in each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 201, 10201);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 202, 10202);

        expected10201.clear();

        expected10202.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting partitions):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 6: Remove metrics for all remaining partitions in the first table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 101, 10101);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 301, 10301);

        expected10101.clear();

        expected10301.clear();

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting table 1):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 7: Remove metrics for all remaining partitions in the second table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 102, 10102);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 302, 10302);

        countersJson = NormalizeJson(GetPrivateJsonForCounters(*detailedCounters));
        Cerr << "TEST Current counters (after deleting table 2):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
R"json(
{
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

#undef AddExpected
#define AddExpected(tabletId) \
    StartNested("detailed_metrics", "per_partition") \
        .StartNested("tablet_id", #tabletId) \
            .AddNestedGroup("follower_id", "0", expectedTablet##tabletId) \
            .AddNestedGroup("follower_id", "replicas_only", emptyMetrics) \
            .AddGroup(expectedTablet##tabletId) \
        .EndNested() \
    .EndNested() \
    .AddGroup(expectedTablet##tabletId)

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is ignored, if the tenant database schema version is older than the current one.
     *
     * @param[in] hasOldMonitoringProjectId Indicates if the current monitoring
     *                                      project ID (to change from) is set
     * @param[in] newMonitoringProjectId The new monitoring project ID (to change to)
     */
    void VerifyChangeMonitoringProjectIdOldSchemaVersion(
        bool hasOldMonitoringProjectId,
        const std::optional<TString>& newMonitoringProjectId
    ) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Simulate 3 tables, one without a monitoring project ID and two with different
        // monitoring project IDs set (all three with a single partition and a leader)
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig1 = {
            .TableId = 12340001,
            .TablePath = "/Root/fake-db/fake-table1",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            // .MonitoringProjectId = ... - no monitoring project ID for this table
        };

        auto expectedTablet101 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 10, &tableMetricsConfig1, 0x01);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 12340002,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-2"
        };

        auto expectedTablet202 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 20, &tableMetricsConfig2, 0x02);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig3 = {
            .TableId = 12340003,
            .TablePath = "/Root/fake-db/fake-table3",
            .TableSchemaVersion = 1000333,
            .TenantDbSchemaVersion = 2000333,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-3"
        };

        auto expectedTablet303 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 30, &tableMetricsConfig3, 0x04);

        auto emptyMetrics = EmptyDataShardMetrics();

        // The "ydb_detailed_raw" counter group should contain detailed metrics for all tables
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (initial):" << Endl << countersJson << Endl;

        auto makeExpectedJson = [&] () -> TString {
            return NormalizeJson(TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .AddExpected(202)
                        .EndNested()
                    .EndNested()
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                        .StartNested("table", "/Root/fake-db/fake-table3")
                            .AddExpected(303)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .AddExpected(101)
                    .EndNested()
                .EndNested()
                .BuildJson()
            );
        };

        TString expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // Try changing the monitoring project ID with an old schema version
        if (hasOldMonitoringProjectId) {
            TEvTabletCounters::TTableMetricsConfig invalidTableMetricsConfig2 = tableMetricsConfig2;

            invalidTableMetricsConfig2.TenantDbSchemaVersion = 2000221;
            invalidTableMetricsConfig2.MonitoringProjectId = newMonitoringProjectId;

            expectedTablet202 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &invalidTableMetricsConfig2, 0x08);

            // Update the other two tables to make sure the regular updates work
            expectedTablet101 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &tableMetricsConfig1, 0x10);
            expectedTablet303 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
        } else {
            TEvTabletCounters::TTableMetricsConfig invalidTableMetricsConfig1 = tableMetricsConfig1;

            invalidTableMetricsConfig1.TenantDbSchemaVersion = 2000110;
            invalidTableMetricsConfig1.MonitoringProjectId = newMonitoringProjectId;

            expectedTablet101 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &invalidTableMetricsConfig1, 0x10);

            // Update the other two tables to make sure the regular updates work
            expectedTablet202 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &tableMetricsConfig2, 0x08);
            expectedTablet303 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
        }

        // Verify all counters in the "ydb_detailed_raw" group - the counters should change,
        // but the monitoring project IDs should not
        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        expectedJson = makeExpectedJson();

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is ignored, if the tenant database schema version is older than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being removed.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdOldVersionIgnoredIdRemoved) {
        VerifyChangeMonitoringProjectIdOldSchemaVersion(
            true /* hasOldMonitoringProjectId */,
            {} // Empty std::optional == remove the monitoring project ID
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is ignored, if the tenant database schema version is older than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being added
     *       and it does not match any of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdOldVersionIgnoredIdAddedNew) {
        VerifyChangeMonitoringProjectIdOldSchemaVersion(
            false /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-1" // Does not match any table
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is ignored, if the tenant database schema version is older than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being added
     *       and it matches one of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdOldVersionIgnoredIdAddedExisting) {
        VerifyChangeMonitoringProjectIdOldSchemaVersion(
            false /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-3" // Matches table 3
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is ignored, if the tenant database schema version is older than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being changed
     *       and it does not match any of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdOldVersionIgnoredIdChangedToNew) {
        VerifyChangeMonitoringProjectIdOldSchemaVersion(
            true /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-1" // Does not match any table
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is ignored, if the tenant database schema version is older than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being changed
     *       and it matches one of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdOldVersionIgnoredIdChangedToExisting) {
        VerifyChangeMonitoringProjectIdOldSchemaVersion(
            true /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-3" // Matches table 3
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed, if the tenant database schema version is newer than the current one.
     *
     * @param[in] hasOldMonitoringProjectId Indicates if the current monitoring
     *                                      project ID (to change from) is set
     * @param[in] newMonitoringProjectId The new monitoring project ID (to change to)
     * @param[in] finalExectedJson The final expected JSON for all counters (after all updates)
     */
    void VerifyChangeMonitoringProjectIdNewSchemaVersion(
        bool hasOldMonitoringProjectId,
        const std::optional<TString>& newMonitoringProjectId,
        std::function<TString(const TExpectedSensorGroup&, const TExpectedSensorGroup&, const TExpectedSensorGroup&)> makeFinalExpectedJson
    ) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_TRACE);

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Simulate 3 tables, one without a monitoring project ID and two with different
        // monitoring project IDs set (all three with a single partition and a leader)
        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig1 = {
            .TableId = 12340001,
            .TablePath = "/Root/fake-db/fake-table1",
            .TableSchemaVersion = 1000111,
            .TenantDbSchemaVersion = 2000111,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            // .MonitoringProjectId = ... - no monitoring project ID for this table
        };

        auto expectedTablet101 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 10, &tableMetricsConfig1, 0x01);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 12340002,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-2"
        };

        auto expectedTablet202 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 20, &tableMetricsConfig2, 0x02);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig3 = {
            .TableId = 12340003,
            .TablePath = "/Root/fake-db/fake-table3",
            .TableSchemaVersion = 1000333,
            .TenantDbSchemaVersion = 2000333,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-3"
        };

        auto expectedTablet303 = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 30, &tableMetricsConfig3, 0x04);

        auto emptyMetrics = EmptyDataShardMetrics();

        // The "ydb_detailed_raw" counter group should contain detailed metrics for all tables
        auto detailedCounters = runtime.GetAppData(0).Counters->FindSubgroup(
            "counters",
            "ydb_detailed_raw"
        );

        UNIT_ASSERT(detailedCounters);

        TString countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (initial):" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .AddExpected(202)
                        .EndNested()
                    .EndNested()
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                        .StartNested("table", "/Root/fake-db/fake-table3")
                            .AddExpected(303)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .AddExpected(101)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // Try changing the monitoring project ID with a new schema version
        if (hasOldMonitoringProjectId) {
            TEvTabletCounters::TTableMetricsConfig invalidTableMetricsConfig2 = tableMetricsConfig2;

            invalidTableMetricsConfig2.TenantDbSchemaVersion = 2000223;
            invalidTableMetricsConfig2.MonitoringProjectId = newMonitoringProjectId;

            expectedTablet202 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &invalidTableMetricsConfig2, 0x08);

            // Update the other two tables to make sure the regular updates work
            expectedTablet101 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &tableMetricsConfig1, 0x10);
            expectedTablet303 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
        } else {
            TEvTabletCounters::TTableMetricsConfig invalidTableMetricsConfig1 = tableMetricsConfig1;

            invalidTableMetricsConfig1.TenantDbSchemaVersion = 2000112;
            invalidTableMetricsConfig1.MonitoringProjectId = newMonitoringProjectId;

            expectedTablet101 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &invalidTableMetricsConfig1, 0x10);

            // Update the other two tables to make sure the regular updates work
            expectedTablet202 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &tableMetricsConfig2, 0x08);
            expectedTablet303 += SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
        }

        // Verify all counters in the "ydb_detailed_raw" group - the counters should change,
        // but the monitoring project IDs should not
        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        auto finalExpectedJson = makeFinalExpectedJson(expectedTablet101, expectedTablet202, expectedTablet303);

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            finalExpectedJson,
            "Expected JSON:" << Endl << finalExpectedJson
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed correctly, if the tenant database schema version is newer
     * than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being removed.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdNewVersionProcessedIdRemoved) {
        VerifyChangeMonitoringProjectIdNewSchemaVersion(
            true /* hasOldMonitoringProjectId */,
            {}, // Empty std::optional == remove the monitoring project ID
            [] (const TExpectedSensorGroup& expectedTablet101, const TExpectedSensorGroup& expectedTablet202, const TExpectedSensorGroup& expectedTablet303) {
                auto emptyMetrics = EmptyDataShardMetrics();
                return NormalizeJson(TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .AddExpected(303)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .AddExpected(101)
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .AddExpected(202)
                        .EndNested()
                    .EndNested()
                    .BuildJson());
                }
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed correctly, if the tenant database schema version is newer
     * than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being added
     *       and it does not match any of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdNewVersionProcessedIdAddedNew) {
        VerifyChangeMonitoringProjectIdNewSchemaVersion(
            false /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-1", // Does not match any table
            [] (const TExpectedSensorGroup& expectedTablet101, const TExpectedSensorGroup& expectedTablet202, const TExpectedSensorGroup& expectedTablet303) {
                auto emptyMetrics = EmptyDataShardMetrics();
                return NormalizeJson(TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-1")
                            .StartNested("table", "/Root/fake-db/fake-table1")
                                .AddExpected(101)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .AddExpected(202)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .AddExpected(303)
                            .EndNested()
                        .EndNested()
                    .EndNested()
                    .BuildJson());
            }
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed correctly, if the tenant database schema version is newer
     * than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being added
     *       and it matches one of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdNewVersionProcessedIdAddedExisting) {
        VerifyChangeMonitoringProjectIdNewSchemaVersion(
            false /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-3", // Matches table 3
            [] (const TExpectedSensorGroup& expectedTablet101, const TExpectedSensorGroup& expectedTablet202, const TExpectedSensorGroup& expectedTablet303) {
                auto emptyMetrics = EmptyDataShardMetrics();
                return NormalizeJson(TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .AddExpected(202)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table1")
                                .AddExpected(101)
                            .EndNested()
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .AddExpected(303)
                            .EndNested()
                        .EndNested()
                    .EndNested()
                    .BuildJson());
            }
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed correctly, if the tenant database schema version is newer
     * than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being changed
     *       and it does not match any of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdNewVersionProcessedIdChangedToNew) {
        VerifyChangeMonitoringProjectIdNewSchemaVersion(
            true /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-1", // Does not match any table
            [] (const TExpectedSensorGroup& expectedTablet101, const TExpectedSensorGroup& expectedTablet202, const TExpectedSensorGroup& expectedTablet303) {
                auto emptyMetrics = EmptyDataShardMetrics();
                return NormalizeJson(TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-1")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .AddExpected(202)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .AddExpected(303)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .AddExpected(101)
                        .EndNested()
                    .EndNested()
                    .BuildJson());
            }
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed correctly, if the tenant database schema version is newer
     * than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is being changed
     *       and it matches one of the existing monitoring project IDs.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdNewVersionProcessedIdChangedToExisting) {
        VerifyChangeMonitoringProjectIdNewSchemaVersion(
            true /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-3", // Matches table 3
            [] (const TExpectedSensorGroup& expectedTablet101, const TExpectedSensorGroup& expectedTablet202, const TExpectedSensorGroup& expectedTablet303) {
                auto emptyMetrics = EmptyDataShardMetrics();
                return NormalizeJson(TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .AddExpected(202)
                            .EndNested()
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .AddExpected(303)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .AddExpected(101)
                        .EndNested()
                    .EndNested()
                    .BuildJson());
            }
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed correctly, if the tenant database schema version is newer
     * than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is not changed
     *       at all (was not set before) even though the tenant database schema version
     *       is increased.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdNewVersionProcessedIdNotChangedUnset) {
        VerifyChangeMonitoringProjectIdNewSchemaVersion(
            false /* hasOldMonitoringProjectId */,
            {}, // Empty std::optional == unset monitoring project ID (the current value)
            [] (const TExpectedSensorGroup& expectedTablet101, const TExpectedSensorGroup& expectedTablet202, const TExpectedSensorGroup& expectedTablet303) {
                auto emptyMetrics = EmptyDataShardMetrics();
                return NormalizeJson(TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .AddExpected(202)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .AddExpected(303)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .AddExpected(101)
                        .EndNested()
                    .EndNested()
                    .BuildJson());
            }
        );
    }

    /**
     * Verify that the message to change the monitoring project ID for a table
     * is processed correctly, if the tenant database schema version is newer
     * than the current one.
     *
     * @note This test is for the variation, when the monitoring project ID is not changed
     *       at all (was set before) even though the tenant database schema version
     *       is increased.
     */
    Y_UNIT_TEST(ChangeMonitoringProjectIdNewVersionProcessedIdNotChangedSet) {
        VerifyChangeMonitoringProjectIdNewSchemaVersion(
            true /* hasOldMonitoringProjectId */,
            "fake-monitoring-project-id-2", // Matches table 2 (the current value)
            [] (const TExpectedSensorGroup& expectedTablet101, const TExpectedSensorGroup& expectedTablet202, const TExpectedSensorGroup& expectedTablet303) {
                auto emptyMetrics = EmptyDataShardMetrics();
                return NormalizeJson(TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .AddExpected(202)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .AddExpected(303)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .AddExpected(101)
                        .EndNested()
                    .EndNested()
                    .BuildJson());
            }
        );
    }
}
