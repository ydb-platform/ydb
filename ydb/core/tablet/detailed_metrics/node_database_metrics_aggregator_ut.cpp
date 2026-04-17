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

/**
 * Retrieve the given private counters as a JSON string.
 *
 * @param[in] counters The counters for which to retrieve the JSON
 *
 * @return The corresponding JSON as a string
 */
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

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 1, 101, 0, &tableMetricsConfig);

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
                                    .StartNested("follower_id", "101")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                        .AddRate("table.datashard.cache_hit.bytes", 30001)
                                        .AddRate("table.datashard.cache_miss.bytes", 40001)
                                        .AddRate("table.datashard.consumed_cpu_us", 1)
                                        .AddRate("table.datashard.erase.bytes", 120001)
                                        .AddRate("table.datashard.erase.rows", 110001)
                                        .AddRate("table.datashard.read.bytes", 70190002)
                                        .AddRate("table.datashard.read.rows", 30150002)
                                        .AddGauge("table.datashard.row_count", 10001)
                                        .AddRate("table.datashard.scan.bytes", 160001)
                                        .AddRate("table.datashard.scan.rows", 150001)
                                        .AddGauge("table.datashard.size_bytes", 20001)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 60001)
                                        .AddRate("table.datashard.write.rows", 50001)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                        .AddRate("table.datashard.cache_hit.bytes", 30001)
                                        .AddRate("table.datashard.cache_miss.bytes", 40001)
                                        .AddRate("table.datashard.consumed_cpu_us", 1)
                                        .AddRate("table.datashard.erase.bytes", 120001)
                                        .AddRate("table.datashard.erase.rows", 110001)
                                        .AddRate("table.datashard.read.bytes", 70190002)
                                        .AddRate("table.datashard.read.rows", 30150002)
                                        .AddGauge("table.datashard.row_count", 10001)
                                        .AddRate("table.datashard.scan.bytes", 160001)
                                        .AddRate("table.datashard.scan.rows", 150001)
                                        .AddGauge("table.datashard.size_bytes", 20001)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 60001)
                                        .AddRate("table.datashard.write.rows", 50001)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 1)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                            .AddRate("table.datashard.bulk_upsert.rows", 130001)
                            .AddRate("table.datashard.cache_hit.bytes", 30001)
                            .AddRate("table.datashard.cache_miss.bytes", 40001)
                            .AddRate("table.datashard.consumed_cpu_us", 1)
                            .AddRate("table.datashard.erase.bytes", 120001)
                            .AddRate("table.datashard.erase.rows", 110001)
                            .AddRate("table.datashard.read.bytes", 70190002)
                            .AddRate("table.datashard.read.rows", 30150002)
                            .AddGauge("table.datashard.row_count", 10001)
                            .AddRate("table.datashard.scan.bytes", 160001)
                            .AddRate("table.datashard.scan.rows", 150001)
                            .AddGauge("table.datashard.size_bytes", 20001)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                            .AddRate("table.datashard.write.bytes", 60001)
                            .AddRate("table.datashard.write.rows", 50001)
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

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x01, 0, 20, &tableMetricsConfig1);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x02, 0, 40, &tableMetricsConfig1);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x04, 0, 60, &tableMetricsConfig1);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 4321,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x10, 0, 10, &tableMetricsConfig2);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x20, 0, 30, &tableMetricsConfig2);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x40, 0, 50, &tableMetricsConfig2);

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
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "16")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                        .AddRate("table.datashard.cache_hit.bytes", 30016)
                                        .AddRate("table.datashard.cache_miss.bytes", 40016)
                                        .AddRate("table.datashard.consumed_cpu_us", 100016)
                                        .AddRate("table.datashard.erase.bytes", 120016)
                                        .AddRate("table.datashard.erase.rows", 110016)
                                        .AddRate("table.datashard.read.bytes", 70190032)
                                        .AddRate("table.datashard.read.rows", 30150032)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 160016)
                                        .AddRate("table.datashard.scan.rows", 150016)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60016)
                                        .AddRate("table.datashard.write.rows", 50016)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 100016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("tablet_id", "32")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 300032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 300032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .StartNested("tablet_id", "64")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                        .AddRate("table.datashard.cache_hit.bytes", 30064)
                                        .AddRate("table.datashard.cache_miss.bytes", 40064)
                                        .AddRate("table.datashard.consumed_cpu_us", 500064)
                                        .AddRate("table.datashard.erase.bytes", 120064)
                                        .AddRate("table.datashard.erase.rows", 110064)
                                        .AddRate("table.datashard.read.bytes", 70190128)
                                        .AddRate("table.datashard.read.rows", 30150128)
                                        .AddGauge("table.datashard.row_count", 10064)
                                        .AddRate("table.datashard.scan.bytes", 160064)
                                        .AddRate("table.datashard.scan.rows", 150064)
                                        .AddGauge("table.datashard.size_bytes", 20064)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60064)
                                        .AddRate("table.datashard.write.rows", 50064)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 500064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 420112)
                            .AddRate("table.datashard.bulk_upsert.rows", 390112)
                            .AddRate("table.datashard.cache_hit.bytes", 90112)
                            .AddRate("table.datashard.cache_miss.bytes", 120112)
                            .AddRate("table.datashard.consumed_cpu_us", 900112)
                            .AddRate("table.datashard.erase.bytes", 360112)
                            .AddRate("table.datashard.erase.rows", 330112)
                            .AddRate("table.datashard.read.bytes", 210570224)
                            .AddRate("table.datashard.read.rows", 90450224)
                            .AddGauge("table.datashard.row_count", 30112)
                            .AddRate("table.datashard.scan.bytes", 480112)
                            .AddRate("table.datashard.scan.rows", 450112)
                            .AddGauge("table.datashard.size_bytes", 60112)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {40, 1},
                                    {60, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 180112)
                            .AddRate("table.datashard.write.rows", 150112)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "1")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 200001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                .AddRate("table.datashard.cache_hit.bytes", 30001)
                                .AddRate("table.datashard.cache_miss.bytes", 40001)
                                .AddRate("table.datashard.consumed_cpu_us", 200001)
                                .AddRate("table.datashard.erase.bytes", 120001)
                                .AddRate("table.datashard.erase.rows", 110001)
                                .AddRate("table.datashard.read.bytes", 70190002)
                                .AddRate("table.datashard.read.rows", 30150002)
                                .AddGauge("table.datashard.row_count", 10001)
                                .AddRate("table.datashard.scan.bytes", 160001)
                                .AddRate("table.datashard.scan.rows", 150001)
                                .AddGauge("table.datashard.size_bytes", 20001)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                .AddRate("table.datashard.write.bytes", 60001)
                                .AddRate("table.datashard.write.rows", 50001)
                            .EndNested()
                            .StartNested("tablet_id", "2")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 400002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                .AddRate("table.datashard.cache_hit.bytes", 30002)
                                .AddRate("table.datashard.cache_miss.bytes", 40002)
                                .AddRate("table.datashard.consumed_cpu_us", 400002)
                                .AddRate("table.datashard.erase.bytes", 120002)
                                .AddRate("table.datashard.erase.rows", 110002)
                                .AddRate("table.datashard.read.bytes", 70190004)
                                .AddRate("table.datashard.read.rows", 30150004)
                                .AddGauge("table.datashard.row_count", 10002)
                                .AddRate("table.datashard.scan.bytes", 160002)
                                .AddRate("table.datashard.scan.rows", 150002)
                                .AddGauge("table.datashard.size_bytes", 20002)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                .AddRate("table.datashard.write.bytes", 60002)
                                .AddRate("table.datashard.write.rows", 50002)
                            .EndNested()
                            .StartNested("tablet_id", "4")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 600004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                .AddRate("table.datashard.cache_hit.bytes", 30004)
                                .AddRate("table.datashard.cache_miss.bytes", 40004)
                                .AddRate("table.datashard.consumed_cpu_us", 600004)
                                .AddRate("table.datashard.erase.bytes", 120004)
                                .AddRate("table.datashard.erase.rows", 110004)
                                .AddRate("table.datashard.read.bytes", 70190008)
                                .AddRate("table.datashard.read.rows", 30150008)
                                .AddGauge("table.datashard.row_count", 10004)
                                .AddRate("table.datashard.scan.bytes", 160004)
                                .AddRate("table.datashard.scan.rows", 150004)
                                .AddGauge("table.datashard.size_bytes", 20004)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                .AddRate("table.datashard.write.bytes", 60004)
                                .AddRate("table.datashard.write.rows", 50004)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 420007)
                        .AddRate("table.datashard.bulk_upsert.rows", 390007)
                        .AddRate("table.datashard.cache_hit.bytes", 90007)
                        .AddRate("table.datashard.cache_miss.bytes", 120007)
                        .AddRate("table.datashard.consumed_cpu_us", 1200007)
                        .AddRate("table.datashard.erase.bytes", 360007)
                        .AddRate("table.datashard.erase.rows", 330007)
                        .AddRate("table.datashard.read.bytes", 210570014)
                        .AddRate("table.datashard.read.rows", 90450014)
                        .AddGauge("table.datashard.row_count", 30007)
                        .AddRate("table.datashard.scan.bytes", 480007)
                        .AddRate("table.datashard.scan.rows", 450007)
                        .AddGauge("table.datashard.size_bytes", 60007)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {30, 1},
                                {50, 1},
                                {70, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 180007)
                        .AddRate("table.datashard.write.rows", 150007)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 2: Update metrics for the second partition for each table
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x02, 0, 80, &tableMetricsConfig1, 0x08);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 0x20, 0, 60, &tableMetricsConfig2, 0x80);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "16")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                        .AddRate("table.datashard.cache_hit.bytes", 30016)
                                        .AddRate("table.datashard.cache_miss.bytes", 40016)
                                        .AddRate("table.datashard.consumed_cpu_us", 100016)
                                        .AddRate("table.datashard.erase.bytes", 120016)
                                        .AddRate("table.datashard.erase.rows", 110016)
                                        .AddRate("table.datashard.read.bytes", 70190032)
                                        .AddRate("table.datashard.read.rows", 30150032)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 160016)
                                        .AddRate("table.datashard.scan.rows", 150016)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60016)
                                        .AddRate("table.datashard.write.rows", 50016)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 100016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("tablet_id", "32")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280160)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260160)
                                        .AddRate("table.datashard.cache_hit.bytes", 60160)
                                        .AddRate("table.datashard.cache_miss.bytes", 80160)
                                        .AddRate("table.datashard.consumed_cpu_us", 900160)
                                        .AddRate("table.datashard.erase.bytes", 240160)
                                        .AddRate("table.datashard.erase.rows", 220160)
                                        .AddRate("table.datashard.read.bytes", 140380320)
                                        .AddRate("table.datashard.read.rows", 60300320)
                                        .AddGauge("table.datashard.row_count", 10128)
                                        .AddRate("table.datashard.scan.bytes", 320160)
                                        .AddRate("table.datashard.scan.rows", 300160)
                                        .AddGauge("table.datashard.size_bytes", 20128)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                        .AddRate("table.datashard.write.bytes", 120160)
                                        .AddRate("table.datashard.write.rows", 100160)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280160)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260160)
                                    .AddRate("table.datashard.cache_hit.bytes", 60160)
                                    .AddRate("table.datashard.cache_miss.bytes", 80160)
                                    .AddRate("table.datashard.consumed_cpu_us", 900160)
                                    .AddRate("table.datashard.erase.bytes", 240160)
                                    .AddRate("table.datashard.erase.rows", 220160)
                                    .AddRate("table.datashard.read.bytes", 140380320)
                                    .AddRate("table.datashard.read.rows", 60300320)
                                    .AddGauge("table.datashard.row_count", 10128)
                                    .AddRate("table.datashard.scan.bytes", 320160)
                                    .AddRate("table.datashard.scan.rows", 300160)
                                    .AddGauge("table.datashard.size_bytes", 20128)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 120160)
                                    .AddRate("table.datashard.write.rows", 100160)
                                .EndNested()
                                .StartNested("tablet_id", "64")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                        .AddRate("table.datashard.cache_hit.bytes", 30064)
                                        .AddRate("table.datashard.cache_miss.bytes", 40064)
                                        .AddRate("table.datashard.consumed_cpu_us", 500064)
                                        .AddRate("table.datashard.erase.bytes", 120064)
                                        .AddRate("table.datashard.erase.rows", 110064)
                                        .AddRate("table.datashard.read.bytes", 70190128)
                                        .AddRate("table.datashard.read.rows", 30150128)
                                        .AddGauge("table.datashard.row_count", 10064)
                                        .AddRate("table.datashard.scan.bytes", 160064)
                                        .AddRate("table.datashard.scan.rows", 150064)
                                        .AddGauge("table.datashard.size_bytes", 20064)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60064)
                                        .AddRate("table.datashard.write.rows", 50064)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 500064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 560240)
                            .AddRate("table.datashard.bulk_upsert.rows", 520240)
                            .AddRate("table.datashard.cache_hit.bytes", 120240)
                            .AddRate("table.datashard.cache_miss.bytes", 160240)
                            .AddRate("table.datashard.consumed_cpu_us", 1500240)
                            .AddRate("table.datashard.erase.bytes", 480240)
                            .AddRate("table.datashard.erase.rows", 440240)
                            .AddRate("table.datashard.read.bytes", 280760480)
                            .AddRate("table.datashard.read.rows", 120600480)
                            .AddGauge("table.datashard.row_count", 30208)
                            .AddRate("table.datashard.scan.bytes", 640240)
                            .AddRate("table.datashard.scan.rows", 600240)
                            .AddGauge("table.datashard.size_bytes", 60208)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {60, 1},
                                    {70, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 240240)
                            .AddRate("table.datashard.write.rows", 200240)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "1")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 200001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                .AddRate("table.datashard.cache_hit.bytes", 30001)
                                .AddRate("table.datashard.cache_miss.bytes", 40001)
                                .AddRate("table.datashard.consumed_cpu_us", 200001)
                                .AddRate("table.datashard.erase.bytes", 120001)
                                .AddRate("table.datashard.erase.rows", 110001)
                                .AddRate("table.datashard.read.bytes", 70190002)
                                .AddRate("table.datashard.read.rows", 30150002)
                                .AddGauge("table.datashard.row_count", 10001)
                                .AddRate("table.datashard.scan.bytes", 160001)
                                .AddRate("table.datashard.scan.rows", 150001)
                                .AddGauge("table.datashard.size_bytes", 20001)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                .AddRate("table.datashard.write.bytes", 60001)
                                .AddRate("table.datashard.write.rows", 50001)
                            .EndNested()
                            .StartNested("tablet_id", "2")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                    .AddRate("table.datashard.cache_hit.bytes", 60010)
                                    .AddRate("table.datashard.cache_miss.bytes", 80010)
                                    .AddRate("table.datashard.consumed_cpu_us", 1200010)
                                    .AddRate("table.datashard.erase.bytes", 240010)
                                    .AddRate("table.datashard.erase.rows", 220010)
                                    .AddRate("table.datashard.read.bytes", 140380020)
                                    .AddRate("table.datashard.read.rows", 60300020)
                                    .AddGauge("table.datashard.row_count", 10008)
                                    .AddRate("table.datashard.scan.bytes", 320010)
                                    .AddRate("table.datashard.scan.rows", 300010)
                                    .AddGauge("table.datashard.size_bytes", 20008)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 120010)
                                    .AddRate("table.datashard.write.rows", 100010)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                .AddRate("table.datashard.cache_hit.bytes", 60010)
                                .AddRate("table.datashard.cache_miss.bytes", 80010)
                                .AddRate("table.datashard.consumed_cpu_us", 1200010)
                                .AddRate("table.datashard.erase.bytes", 240010)
                                .AddRate("table.datashard.erase.rows", 220010)
                                .AddRate("table.datashard.read.bytes", 140380020)
                                .AddRate("table.datashard.read.rows", 60300020)
                                .AddGauge("table.datashard.row_count", 10008)
                                .AddRate("table.datashard.scan.bytes", 320010)
                                .AddRate("table.datashard.scan.rows", 300010)
                                .AddGauge("table.datashard.size_bytes", 20008)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 120010)
                                .AddRate("table.datashard.write.rows", 100010)
                            .EndNested()
                            .StartNested("tablet_id", "4")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 600004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                .AddRate("table.datashard.cache_hit.bytes", 30004)
                                .AddRate("table.datashard.cache_miss.bytes", 40004)
                                .AddRate("table.datashard.consumed_cpu_us", 600004)
                                .AddRate("table.datashard.erase.bytes", 120004)
                                .AddRate("table.datashard.erase.rows", 110004)
                                .AddRate("table.datashard.read.bytes", 70190008)
                                .AddRate("table.datashard.read.rows", 30150008)
                                .AddGauge("table.datashard.row_count", 10004)
                                .AddRate("table.datashard.scan.bytes", 160004)
                                .AddRate("table.datashard.scan.rows", 150004)
                                .AddGauge("table.datashard.size_bytes", 20004)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                .AddRate("table.datashard.write.bytes", 60004)
                                .AddRate("table.datashard.write.rows", 50004)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 560015)
                        .AddRate("table.datashard.bulk_upsert.rows", 520015)
                        .AddRate("table.datashard.cache_hit.bytes", 120015)
                        .AddRate("table.datashard.cache_miss.bytes", 160015)
                        .AddRate("table.datashard.consumed_cpu_us", 2000015)
                        .AddRate("table.datashard.erase.bytes", 480015)
                        .AddRate("table.datashard.erase.rows", 440015)
                        .AddRate("table.datashard.read.bytes", 280760030)
                        .AddRate("table.datashard.read.rows", 120600030)
                        .AddGauge("table.datashard.row_count", 30013)
                        .AddRate("table.datashard.scan.bytes", 640015)
                        .AddRate("table.datashard.scan.rows", 600015)
                        .AddGauge("table.datashard.size_bytes", 60013)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {30, 1},
                                {70, 1},
                                {90, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 240015)
                        .AddRate("table.datashard.write.rows", 200015)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 3: Remove metrics for the second partition for each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x02, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x20, 0);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting leaders):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "16")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                        .AddRate("table.datashard.cache_hit.bytes", 30016)
                                        .AddRate("table.datashard.cache_miss.bytes", 40016)
                                        .AddRate("table.datashard.consumed_cpu_us", 100016)
                                        .AddRate("table.datashard.erase.bytes", 120016)
                                        .AddRate("table.datashard.erase.rows", 110016)
                                        .AddRate("table.datashard.read.bytes", 70190032)
                                        .AddRate("table.datashard.read.rows", 30150032)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 160016)
                                        .AddRate("table.datashard.scan.rows", 150016)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60016)
                                        .AddRate("table.datashard.write.rows", 50016)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 100016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("tablet_id", "64")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                        .AddRate("table.datashard.cache_hit.bytes", 30064)
                                        .AddRate("table.datashard.cache_miss.bytes", 40064)
                                        .AddRate("table.datashard.consumed_cpu_us", 500064)
                                        .AddRate("table.datashard.erase.bytes", 120064)
                                        .AddRate("table.datashard.erase.rows", 110064)
                                        .AddRate("table.datashard.read.bytes", 70190128)
                                        .AddRate("table.datashard.read.rows", 30150128)
                                        .AddGauge("table.datashard.row_count", 10064)
                                        .AddRate("table.datashard.scan.bytes", 160064)
                                        .AddRate("table.datashard.scan.rows", 150064)
                                        .AddGauge("table.datashard.size_bytes", 20064)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60064)
                                        .AddRate("table.datashard.write.rows", 50064)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 500064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280080)
                            .AddRate("table.datashard.bulk_upsert.rows", 260080)
                            .AddRate("table.datashard.cache_hit.bytes", 60080)
                            .AddRate("table.datashard.cache_miss.bytes", 80080)
                            .AddRate("table.datashard.consumed_cpu_us", 600080)
                            .AddRate("table.datashard.erase.bytes", 240080)
                            .AddRate("table.datashard.erase.rows", 220080)
                            .AddRate("table.datashard.read.bytes", 140380160)
                            .AddRate("table.datashard.read.rows", 60300160)
                            .AddGauge("table.datashard.row_count", 20080)
                            .AddRate("table.datashard.scan.bytes", 320080)
                            .AddRate("table.datashard.scan.rows", 300080)
                            .AddGauge("table.datashard.size_bytes", 40080)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {60, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 120080)
                            .AddRate("table.datashard.write.rows", 100080)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "1")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 200001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                .AddRate("table.datashard.cache_hit.bytes", 30001)
                                .AddRate("table.datashard.cache_miss.bytes", 40001)
                                .AddRate("table.datashard.consumed_cpu_us", 200001)
                                .AddRate("table.datashard.erase.bytes", 120001)
                                .AddRate("table.datashard.erase.rows", 110001)
                                .AddRate("table.datashard.read.bytes", 70190002)
                                .AddRate("table.datashard.read.rows", 30150002)
                                .AddGauge("table.datashard.row_count", 10001)
                                .AddRate("table.datashard.scan.bytes", 160001)
                                .AddRate("table.datashard.scan.rows", 150001)
                                .AddGauge("table.datashard.size_bytes", 20001)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                .AddRate("table.datashard.write.bytes", 60001)
                                .AddRate("table.datashard.write.rows", 50001)
                            .EndNested()
                            .StartNested("tablet_id", "4")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 600004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                .AddRate("table.datashard.cache_hit.bytes", 30004)
                                .AddRate("table.datashard.cache_miss.bytes", 40004)
                                .AddRate("table.datashard.consumed_cpu_us", 600004)
                                .AddRate("table.datashard.erase.bytes", 120004)
                                .AddRate("table.datashard.erase.rows", 110004)
                                .AddRate("table.datashard.read.bytes", 70190008)
                                .AddRate("table.datashard.read.rows", 30150008)
                                .AddGauge("table.datashard.row_count", 10004)
                                .AddRate("table.datashard.scan.bytes", 160004)
                                .AddRate("table.datashard.scan.rows", 150004)
                                .AddGauge("table.datashard.size_bytes", 20004)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                .AddRate("table.datashard.write.bytes", 60004)
                                .AddRate("table.datashard.write.rows", 50004)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 280005)
                        .AddRate("table.datashard.bulk_upsert.rows", 260005)
                        .AddRate("table.datashard.cache_hit.bytes", 60005)
                        .AddRate("table.datashard.cache_miss.bytes", 80005)
                        .AddRate("table.datashard.consumed_cpu_us", 800005)
                        .AddRate("table.datashard.erase.bytes", 240005)
                        .AddRate("table.datashard.erase.rows", 220005)
                        .AddRate("table.datashard.read.bytes", 140380010)
                        .AddRate("table.datashard.read.rows", 60300010)
                        .AddGauge("table.datashard.row_count", 20005)
                        .AddRate("table.datashard.scan.bytes", 320005)
                        .AddRate("table.datashard.scan.rows", 300005)
                        .AddGauge("table.datashard.size_bytes", 40005)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {30, 1},
                                {70, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 120005)
                        .AddRate("table.datashard.write.rows", 100005)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 4: Remove metrics for all remaining partitions in the first table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x01, 0);
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 0x04, 0);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting table 1):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "16")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                        .AddRate("table.datashard.cache_hit.bytes", 30016)
                                        .AddRate("table.datashard.cache_miss.bytes", 40016)
                                        .AddRate("table.datashard.consumed_cpu_us", 100016)
                                        .AddRate("table.datashard.erase.bytes", 120016)
                                        .AddRate("table.datashard.erase.rows", 110016)
                                        .AddRate("table.datashard.read.bytes", 70190032)
                                        .AddRate("table.datashard.read.rows", 30150032)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 160016)
                                        .AddRate("table.datashard.scan.rows", 150016)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60016)
                                        .AddRate("table.datashard.write.rows", 50016)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 100016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("tablet_id", "64")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                        .AddRate("table.datashard.cache_hit.bytes", 30064)
                                        .AddRate("table.datashard.cache_miss.bytes", 40064)
                                        .AddRate("table.datashard.consumed_cpu_us", 500064)
                                        .AddRate("table.datashard.erase.bytes", 120064)
                                        .AddRate("table.datashard.erase.rows", 110064)
                                        .AddRate("table.datashard.read.bytes", 70190128)
                                        .AddRate("table.datashard.read.rows", 30150128)
                                        .AddGauge("table.datashard.row_count", 10064)
                                        .AddRate("table.datashard.scan.bytes", 160064)
                                        .AddRate("table.datashard.scan.rows", 150064)
                                        .AddGauge("table.datashard.size_bytes", 20064)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60064)
                                        .AddRate("table.datashard.write.rows", 50064)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 500064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280080)
                            .AddRate("table.datashard.bulk_upsert.rows", 260080)
                            .AddRate("table.datashard.cache_hit.bytes", 60080)
                            .AddRate("table.datashard.cache_miss.bytes", 80080)
                            .AddRate("table.datashard.consumed_cpu_us", 600080)
                            .AddRate("table.datashard.erase.bytes", 240080)
                            .AddRate("table.datashard.erase.rows", 220080)
                            .AddRate("table.datashard.read.bytes", 140380160)
                            .AddRate("table.datashard.read.rows", 60300160)
                            .AddGauge("table.datashard.row_count", 20080)
                            .AddRate("table.datashard.scan.bytes", 320080)
                            .AddRate("table.datashard.scan.rows", 300080)
                            .AddGauge("table.datashard.size_bytes", 40080)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {60, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 120080)
                            .AddRate("table.datashard.write.rows", 100080)
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

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 10101, 10, &tableMetricsConfig1, 0x0001);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101, 20, &tableMetricsConfig1, 0x0002);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 30101, 30, &tableMetricsConfig1, 0x0004);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 10201, 40, &tableMetricsConfig1, 0x0010);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201, 50, &tableMetricsConfig1, 0x0020);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 30201, 60, &tableMetricsConfig1, 0x0040);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 10301, 70, &tableMetricsConfig1, 0x0100);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301, 80, &tableMetricsConfig1, 0x0200);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 30301, 90, &tableMetricsConfig1, 0x0400);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 4321,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 10102, 10, &tableMetricsConfig2, 0x0002);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102, 20, &tableMetricsConfig2, 0x0004);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 30102, 30, &tableMetricsConfig2, 0x0008);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 10202, 40, &tableMetricsConfig2, 0x0020);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202, 50, &tableMetricsConfig2, 0x0040);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 30202, 60, &tableMetricsConfig2, 0x0080);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 10302, 70, &tableMetricsConfig2, 0x0200);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302, 80, &tableMetricsConfig2, 0x0400);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 30302, 90, &tableMetricsConfig2, 0x0800);

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
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 100002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "20102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                        .AddRate("table.datashard.cache_hit.bytes", 30004)
                                        .AddRate("table.datashard.cache_miss.bytes", 40004)
                                        .AddRate("table.datashard.consumed_cpu_us", 200004)
                                        .AddRate("table.datashard.erase.bytes", 120004)
                                        .AddRate("table.datashard.erase.rows", 110004)
                                        .AddRate("table.datashard.read.bytes", 70190008)
                                        .AddRate("table.datashard.read.rows", 30150008)
                                        .AddGauge("table.datashard.row_count", 10004)
                                        .AddRate("table.datashard.scan.bytes", 160004)
                                        .AddRate("table.datashard.scan.rows", 150004)
                                        .AddGauge("table.datashard.size_bytes", 20004)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60004)
                                        .AddRate("table.datashard.write.rows", 50004)
                                    .EndNested()
                                    .StartNested("follower_id", "30102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140008)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130008)
                                        .AddRate("table.datashard.cache_hit.bytes", 30008)
                                        .AddRate("table.datashard.cache_miss.bytes", 40008)
                                        .AddRate("table.datashard.consumed_cpu_us", 300008)
                                        .AddRate("table.datashard.erase.bytes", 120008)
                                        .AddRate("table.datashard.erase.rows", 110008)
                                        .AddRate("table.datashard.read.bytes", 70190016)
                                        .AddRate("table.datashard.read.rows", 30150016)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 160008)
                                        .AddRate("table.datashard.scan.rows", 150008)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60008)
                                        .AddRate("table.datashard.write.rows", 50008)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 420014)
                                        .AddRate("table.datashard.bulk_upsert.rows", 390014)
                                        .AddRate("table.datashard.cache_hit.bytes", 90014)
                                        .AddRate("table.datashard.cache_miss.bytes", 120014)
                                        .AddRate("table.datashard.consumed_cpu_us", 600014)
                                        .AddRate("table.datashard.erase.bytes", 360014)
                                        .AddRate("table.datashard.erase.rows", 330014)
                                        .AddRate("table.datashard.read.bytes", 210570028)
                                        .AddRate("table.datashard.read.rows", 90450028)
                                        .AddGauge("table.datashard.row_count", 30014)
                                        .AddRate("table.datashard.scan.bytes", 480014)
                                        .AddRate("table.datashard.scan.rows", 450014)
                                        .AddGauge("table.datashard.size_bytes", 60014)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {20, 1},
                                                {30, 1},
                                                {40, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 180014)
                                        .AddRate("table.datashard.write.rows", 150014)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420014)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390014)
                                    .AddRate("table.datashard.cache_hit.bytes", 90014)
                                    .AddRate("table.datashard.cache_miss.bytes", 120014)
                                    .AddRate("table.datashard.consumed_cpu_us", 600014)
                                    .AddRate("table.datashard.erase.bytes", 360014)
                                    .AddRate("table.datashard.erase.rows", 330014)
                                    .AddRate("table.datashard.read.bytes", 210570028)
                                    .AddRate("table.datashard.read.rows", 90450028)
                                    .AddGauge("table.datashard.row_count", 30014)
                                    .AddRate("table.datashard.scan.bytes", 480014)
                                    .AddRate("table.datashard.scan.rows", 450014)
                                    .AddGauge("table.datashard.size_bytes", 60014)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {30, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180014)
                                    .AddRate("table.datashard.write.rows", 150014)
                                .EndNested()
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "10202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 400032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "20202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                        .AddRate("table.datashard.cache_hit.bytes", 30064)
                                        .AddRate("table.datashard.cache_miss.bytes", 40064)
                                        .AddRate("table.datashard.consumed_cpu_us", 500064)
                                        .AddRate("table.datashard.erase.bytes", 120064)
                                        .AddRate("table.datashard.erase.rows", 110064)
                                        .AddRate("table.datashard.read.bytes", 70190128)
                                        .AddRate("table.datashard.read.rows", 30150128)
                                        .AddGauge("table.datashard.row_count", 10064)
                                        .AddRate("table.datashard.scan.bytes", 160064)
                                        .AddRate("table.datashard.scan.rows", 150064)
                                        .AddGauge("table.datashard.size_bytes", 20064)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60064)
                                        .AddRate("table.datashard.write.rows", 50064)
                                    .EndNested()
                                    .StartNested("follower_id", "30202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140128)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130128)
                                        .AddRate("table.datashard.cache_hit.bytes", 30128)
                                        .AddRate("table.datashard.cache_miss.bytes", 40128)
                                        .AddRate("table.datashard.consumed_cpu_us", 600128)
                                        .AddRate("table.datashard.erase.bytes", 120128)
                                        .AddRate("table.datashard.erase.rows", 110128)
                                        .AddRate("table.datashard.read.bytes", 70190256)
                                        .AddRate("table.datashard.read.rows", 30150256)
                                        .AddGauge("table.datashard.row_count", 10128)
                                        .AddRate("table.datashard.scan.bytes", 160128)
                                        .AddRate("table.datashard.scan.rows", 150128)
                                        .AddGauge("table.datashard.size_bytes", 20128)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                        .AddRate("table.datashard.write.bytes", 60128)
                                        .AddRate("table.datashard.write.rows", 50128)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 420224)
                                        .AddRate("table.datashard.bulk_upsert.rows", 390224)
                                        .AddRate("table.datashard.cache_hit.bytes", 90224)
                                        .AddRate("table.datashard.cache_miss.bytes", 120224)
                                        .AddRate("table.datashard.consumed_cpu_us", 1500224)
                                        .AddRate("table.datashard.erase.bytes", 360224)
                                        .AddRate("table.datashard.erase.rows", 330224)
                                        .AddRate("table.datashard.read.bytes", 210570448)
                                        .AddRate("table.datashard.read.rows", 90450448)
                                        .AddGauge("table.datashard.row_count", 30224)
                                        .AddRate("table.datashard.scan.bytes", 480224)
                                        .AddRate("table.datashard.scan.rows", 450224)
                                        .AddGauge("table.datashard.size_bytes", 60224)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {50, 1},
                                                {60, 1},
                                                {70, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 180224)
                                        .AddRate("table.datashard.write.rows", 150224)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420224)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390224)
                                    .AddRate("table.datashard.cache_hit.bytes", 90224)
                                    .AddRate("table.datashard.cache_miss.bytes", 120224)
                                    .AddRate("table.datashard.consumed_cpu_us", 1500224)
                                    .AddRate("table.datashard.erase.bytes", 360224)
                                    .AddRate("table.datashard.erase.rows", 330224)
                                    .AddRate("table.datashard.read.bytes", 210570448)
                                    .AddRate("table.datashard.read.rows", 90450448)
                                    .AddGauge("table.datashard.row_count", 30224)
                                    .AddRate("table.datashard.scan.bytes", 480224)
                                    .AddRate("table.datashard.scan.rows", 450224)
                                    .AddGauge("table.datashard.size_bytes", 60224)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {50, 1},
                                            {60, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180224)
                                    .AddRate("table.datashard.write.rows", 150224)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 700512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "20302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 141024)
                                        .AddRate("table.datashard.bulk_upsert.rows", 131024)
                                        .AddRate("table.datashard.cache_hit.bytes", 31024)
                                        .AddRate("table.datashard.cache_miss.bytes", 41024)
                                        .AddRate("table.datashard.consumed_cpu_us", 801024)
                                        .AddRate("table.datashard.erase.bytes", 121024)
                                        .AddRate("table.datashard.erase.rows", 111024)
                                        .AddRate("table.datashard.read.bytes", 70192048)
                                        .AddRate("table.datashard.read.rows", 30152048)
                                        .AddGauge("table.datashard.row_count", 11024)
                                        .AddRate("table.datashard.scan.bytes", 161024)
                                        .AddRate("table.datashard.scan.rows", 151024)
                                        .AddGauge("table.datashard.size_bytes", 21024)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 61024)
                                        .AddRate("table.datashard.write.rows", 51024)
                                    .EndNested()
                                    .StartNested("follower_id", "30302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 142048)
                                        .AddRate("table.datashard.bulk_upsert.rows", 132048)
                                        .AddRate("table.datashard.cache_hit.bytes", 32048)
                                        .AddRate("table.datashard.cache_miss.bytes", 42048)
                                        .AddRate("table.datashard.consumed_cpu_us", 902048)
                                        .AddRate("table.datashard.erase.bytes", 122048)
                                        .AddRate("table.datashard.erase.rows", 112048)
                                        .AddRate("table.datashard.read.bytes", 70194096)
                                        .AddRate("table.datashard.read.rows", 30154096)
                                        .AddGauge("table.datashard.row_count", 12048)
                                        .AddRate("table.datashard.scan.bytes", 162048)
                                        .AddRate("table.datashard.scan.rows", 152048)
                                        .AddGauge("table.datashard.size_bytes", 22048)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 62048)
                                        .AddRate("table.datashard.write.rows", 52048)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 423584)
                                        .AddRate("table.datashard.bulk_upsert.rows", 393584)
                                        .AddRate("table.datashard.cache_hit.bytes", 93584)
                                        .AddRate("table.datashard.cache_miss.bytes", 123584)
                                        .AddRate("table.datashard.consumed_cpu_us", 2403584)
                                        .AddRate("table.datashard.erase.bytes", 363584)
                                        .AddRate("table.datashard.erase.rows", 333584)
                                        .AddRate("table.datashard.read.bytes", 210577168)
                                        .AddRate("table.datashard.read.rows", 90457168)
                                        .AddGauge("table.datashard.row_count", 33584)
                                        .AddRate("table.datashard.scan.bytes", 483584)
                                        .AddRate("table.datashard.scan.rows", 453584)
                                        .AddGauge("table.datashard.size_bytes", 63584)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {80, 1},
                                                {90, 1},
                                                {100, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 183584)
                                        .AddRate("table.datashard.write.rows", 153584)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 423584)
                                    .AddRate("table.datashard.bulk_upsert.rows", 393584)
                                    .AddRate("table.datashard.cache_hit.bytes", 93584)
                                    .AddRate("table.datashard.cache_miss.bytes", 123584)
                                    .AddRate("table.datashard.consumed_cpu_us", 2403584)
                                    .AddRate("table.datashard.erase.bytes", 363584)
                                    .AddRate("table.datashard.erase.rows", 333584)
                                    .AddRate("table.datashard.read.bytes", 210577168)
                                    .AddRate("table.datashard.read.rows", 90457168)
                                    .AddGauge("table.datashard.row_count", 33584)
                                    .AddRate("table.datashard.scan.bytes", 483584)
                                    .AddRate("table.datashard.scan.rows", 453584)
                                    .AddGauge("table.datashard.size_bytes", 63584)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {90, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 183584)
                                    .AddRate("table.datashard.write.rows", 153584)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 1263822)
                            .AddRate("table.datashard.bulk_upsert.rows", 1173822)
                            .AddRate("table.datashard.cache_hit.bytes", 273822)
                            .AddRate("table.datashard.cache_miss.bytes", 363822)
                            .AddRate("table.datashard.consumed_cpu_us", 4503822)
                            .AddRate("table.datashard.erase.bytes", 1083822)
                            .AddRate("table.datashard.erase.rows", 993822)
                            .AddRate("table.datashard.read.bytes", 631717644)
                            .AddRate("table.datashard.read.rows", 271357644)
                            .AddGauge("table.datashard.row_count", 93822)
                            .AddRate("table.datashard.scan.bytes", 1443822)
                            .AddRate("table.datashard.scan.rows", 1353822)
                            .AddGauge("table.datashard.size_bytes", 183822)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {30, 1},
                                    {40, 1},
                                    {50, 1},
                                    {60, 1},
                                    {70, 1},
                                    {80, 1},
                                    {90, 1},
                                    {100, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 543822)
                            .AddRate("table.datashard.write.rows", 453822)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "20101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("follower_id", "30101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 300004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420007)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390007)
                                    .AddRate("table.datashard.cache_hit.bytes", 90007)
                                    .AddRate("table.datashard.cache_miss.bytes", 120007)
                                    .AddRate("table.datashard.consumed_cpu_us", 600007)
                                    .AddRate("table.datashard.erase.bytes", 360007)
                                    .AddRate("table.datashard.erase.rows", 330007)
                                    .AddRate("table.datashard.read.bytes", 210570014)
                                    .AddRate("table.datashard.read.rows", 90450014)
                                    .AddGauge("table.datashard.row_count", 30007)
                                    .AddRate("table.datashard.scan.bytes", 480007)
                                    .AddRate("table.datashard.scan.rows", 450007)
                                    .AddGauge("table.datashard.size_bytes", 60007)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {30, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180007)
                                    .AddRate("table.datashard.write.rows", 150007)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 420007)
                                .AddRate("table.datashard.bulk_upsert.rows", 390007)
                                .AddRate("table.datashard.cache_hit.bytes", 90007)
                                .AddRate("table.datashard.cache_miss.bytes", 120007)
                                .AddRate("table.datashard.consumed_cpu_us", 600007)
                                .AddRate("table.datashard.erase.bytes", 360007)
                                .AddRate("table.datashard.erase.rows", 330007)
                                .AddRate("table.datashard.read.bytes", 210570014)
                                .AddRate("table.datashard.read.rows", 90450014)
                                .AddGauge("table.datashard.row_count", 30007)
                                .AddRate("table.datashard.scan.bytes", 480007)
                                .AddRate("table.datashard.scan.rows", 450007)
                                .AddGauge("table.datashard.size_bytes", 60007)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {20, 1},
                                        {30, 1},
                                        {40, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 180007)
                                .AddRate("table.datashard.write.rows", 150007)
                            .EndNested()
                            .StartNested("tablet_id", "201")
                                .StartNested("follower_id", "10201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 400016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("follower_id", "20201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .StartNested("follower_id", "30201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 600064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420112)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390112)
                                    .AddRate("table.datashard.cache_hit.bytes", 90112)
                                    .AddRate("table.datashard.cache_miss.bytes", 120112)
                                    .AddRate("table.datashard.consumed_cpu_us", 1500112)
                                    .AddRate("table.datashard.erase.bytes", 360112)
                                    .AddRate("table.datashard.erase.rows", 330112)
                                    .AddRate("table.datashard.read.bytes", 210570224)
                                    .AddRate("table.datashard.read.rows", 90450224)
                                    .AddGauge("table.datashard.row_count", 30112)
                                    .AddRate("table.datashard.scan.bytes", 480112)
                                    .AddRate("table.datashard.scan.rows", 450112)
                                    .AddGauge("table.datashard.size_bytes", 60112)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {50, 1},
                                            {60, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180112)
                                    .AddRate("table.datashard.write.rows", 150112)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 420112)
                                .AddRate("table.datashard.bulk_upsert.rows", 390112)
                                .AddRate("table.datashard.cache_hit.bytes", 90112)
                                .AddRate("table.datashard.cache_miss.bytes", 120112)
                                .AddRate("table.datashard.consumed_cpu_us", 1500112)
                                .AddRate("table.datashard.erase.bytes", 360112)
                                .AddRate("table.datashard.erase.rows", 330112)
                                .AddRate("table.datashard.read.bytes", 210570224)
                                .AddRate("table.datashard.read.rows", 90450224)
                                .AddGauge("table.datashard.row_count", 30112)
                                .AddRate("table.datashard.scan.bytes", 480112)
                                .AddRate("table.datashard.scan.rows", 450112)
                                .AddGauge("table.datashard.size_bytes", 60112)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {50, 1},
                                        {60, 1},
                                        {70, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 180112)
                                .AddRate("table.datashard.write.rows", 150112)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                    .AddRate("table.datashard.cache_hit.bytes", 30256)
                                    .AddRate("table.datashard.cache_miss.bytes", 40256)
                                    .AddRate("table.datashard.consumed_cpu_us", 700256)
                                    .AddRate("table.datashard.erase.bytes", 120256)
                                    .AddRate("table.datashard.erase.rows", 110256)
                                    .AddRate("table.datashard.read.bytes", 70190512)
                                    .AddRate("table.datashard.read.rows", 30150512)
                                    .AddGauge("table.datashard.row_count", 10256)
                                    .AddRate("table.datashard.scan.bytes", 160256)
                                    .AddRate("table.datashard.scan.rows", 150256)
                                    .AddGauge("table.datashard.size_bytes", 20256)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 60256)
                                    .AddRate("table.datashard.write.rows", 50256)
                                .EndNested()
                                .StartNested("follower_id", "20301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .StartNested("follower_id", "30301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 141024)
                                    .AddRate("table.datashard.bulk_upsert.rows", 131024)
                                    .AddRate("table.datashard.cache_hit.bytes", 31024)
                                    .AddRate("table.datashard.cache_miss.bytes", 41024)
                                    .AddRate("table.datashard.consumed_cpu_us", 901024)
                                    .AddRate("table.datashard.erase.bytes", 121024)
                                    .AddRate("table.datashard.erase.rows", 111024)
                                    .AddRate("table.datashard.read.bytes", 70192048)
                                    .AddRate("table.datashard.read.rows", 30152048)
                                    .AddGauge("table.datashard.row_count", 11024)
                                    .AddRate("table.datashard.scan.bytes", 161024)
                                    .AddRate("table.datashard.scan.rows", 151024)
                                    .AddGauge("table.datashard.size_bytes", 21024)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                    .AddRate("table.datashard.write.bytes", 61024)
                                    .AddRate("table.datashard.write.rows", 51024)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 421792)
                                    .AddRate("table.datashard.bulk_upsert.rows", 391792)
                                    .AddRate("table.datashard.cache_hit.bytes", 91792)
                                    .AddRate("table.datashard.cache_miss.bytes", 121792)
                                    .AddRate("table.datashard.consumed_cpu_us", 2401792)
                                    .AddRate("table.datashard.erase.bytes", 361792)
                                    .AddRate("table.datashard.erase.rows", 331792)
                                    .AddRate("table.datashard.read.bytes", 210573584)
                                    .AddRate("table.datashard.read.rows", 90453584)
                                    .AddGauge("table.datashard.row_count", 31792)
                                    .AddRate("table.datashard.scan.bytes", 481792)
                                    .AddRate("table.datashard.scan.rows", 451792)
                                    .AddGauge("table.datashard.size_bytes", 61792)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {90, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 181792)
                                    .AddRate("table.datashard.write.rows", 151792)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 421792)
                                .AddRate("table.datashard.bulk_upsert.rows", 391792)
                                .AddRate("table.datashard.cache_hit.bytes", 91792)
                                .AddRate("table.datashard.cache_miss.bytes", 121792)
                                .AddRate("table.datashard.consumed_cpu_us", 2401792)
                                .AddRate("table.datashard.erase.bytes", 361792)
                                .AddRate("table.datashard.erase.rows", 331792)
                                .AddRate("table.datashard.read.bytes", 210573584)
                                .AddRate("table.datashard.read.rows", 90453584)
                                .AddGauge("table.datashard.row_count", 31792)
                                .AddRate("table.datashard.scan.bytes", 481792)
                                .AddRate("table.datashard.scan.rows", 451792)
                                .AddGauge("table.datashard.size_bytes", 61792)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {80, 1},
                                        {90, 1},
                                        {100, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 181792)
                                .AddRate("table.datashard.write.rows", 151792)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 1261911)
                        .AddRate("table.datashard.bulk_upsert.rows", 1171911)
                        .AddRate("table.datashard.cache_hit.bytes", 271911)
                        .AddRate("table.datashard.cache_miss.bytes", 361911)
                        .AddRate("table.datashard.consumed_cpu_us", 4501911)
                        .AddRate("table.datashard.erase.bytes", 1081911)
                        .AddRate("table.datashard.erase.rows", 991911)
                        .AddRate("table.datashard.read.bytes", 631713822)
                        .AddRate("table.datashard.read.rows", 271353822)
                        .AddGauge("table.datashard.row_count", 91911)
                        .AddRate("table.datashard.scan.bytes", 1441911)
                        .AddRate("table.datashard.scan.rows", 1351911)
                        .AddGauge("table.datashard.size_bytes", 181911)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {20, 1},
                                {30, 1},
                                {40, 1},
                                {50, 1},
                                {60, 1},
                                {70, 1},
                                {80, 1},
                                {90, 1},
                                {100, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 541911)
                        .AddRate("table.datashard.write.rows", 451911)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 2: Update metrics for the second follower for each partition for each table
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101, 0, &tableMetricsConfig1, 0x0008);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201, 0, &tableMetricsConfig1, 0x0080);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301, 0, &tableMetricsConfig1, 0x0800);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102, 0, &tableMetricsConfig2, 0x0001);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202, 0, &tableMetricsConfig2, 0x0010);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302, 0, &tableMetricsConfig2, 0x0100);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 100002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "20102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280005)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260005)
                                        .AddRate("table.datashard.cache_hit.bytes", 60005)
                                        .AddRate("table.datashard.cache_miss.bytes", 80005)
                                        .AddRate("table.datashard.consumed_cpu_us", 200005)
                                        .AddRate("table.datashard.erase.bytes", 240005)
                                        .AddRate("table.datashard.erase.rows", 220005)
                                        .AddRate("table.datashard.read.bytes", 140380010)
                                        .AddRate("table.datashard.read.rows", 60300010)
                                        .AddGauge("table.datashard.row_count", 10001)
                                        .AddRate("table.datashard.scan.bytes", 320005)
                                        .AddRate("table.datashard.scan.rows", 300005)
                                        .AddGauge("table.datashard.size_bytes", 20001)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 120005)
                                        .AddRate("table.datashard.write.rows", 100005)
                                    .EndNested()
                                    .StartNested("follower_id", "30102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140008)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130008)
                                        .AddRate("table.datashard.cache_hit.bytes", 30008)
                                        .AddRate("table.datashard.cache_miss.bytes", 40008)
                                        .AddRate("table.datashard.consumed_cpu_us", 300008)
                                        .AddRate("table.datashard.erase.bytes", 120008)
                                        .AddRate("table.datashard.erase.rows", 110008)
                                        .AddRate("table.datashard.read.bytes", 70190016)
                                        .AddRate("table.datashard.read.rows", 30150016)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 160008)
                                        .AddRate("table.datashard.scan.rows", 150008)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60008)
                                        .AddRate("table.datashard.write.rows", 50008)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 560015)
                                        .AddRate("table.datashard.bulk_upsert.rows", 520015)
                                        .AddRate("table.datashard.cache_hit.bytes", 120015)
                                        .AddRate("table.datashard.cache_miss.bytes", 160015)
                                        .AddRate("table.datashard.consumed_cpu_us", 600015)
                                        .AddRate("table.datashard.erase.bytes", 480015)
                                        .AddRate("table.datashard.erase.rows", 440015)
                                        .AddRate("table.datashard.read.bytes", 280760030)
                                        .AddRate("table.datashard.read.rows", 120600030)
                                        .AddGauge("table.datashard.row_count", 30011)
                                        .AddRate("table.datashard.scan.bytes", 640015)
                                        .AddRate("table.datashard.scan.rows", 600015)
                                        .AddGauge("table.datashard.size_bytes", 60011)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {10, 1},
                                                {20, 1},
                                                {40, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 240015)
                                        .AddRate("table.datashard.write.rows", 200015)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 560015)
                                    .AddRate("table.datashard.bulk_upsert.rows", 520015)
                                    .AddRate("table.datashard.cache_hit.bytes", 120015)
                                    .AddRate("table.datashard.cache_miss.bytes", 160015)
                                    .AddRate("table.datashard.consumed_cpu_us", 600015)
                                    .AddRate("table.datashard.erase.bytes", 480015)
                                    .AddRate("table.datashard.erase.rows", 440015)
                                    .AddRate("table.datashard.read.bytes", 280760030)
                                    .AddRate("table.datashard.read.rows", 120600030)
                                    .AddGauge("table.datashard.row_count", 30011)
                                    .AddRate("table.datashard.scan.bytes", 640015)
                                    .AddRate("table.datashard.scan.rows", 600015)
                                    .AddGauge("table.datashard.size_bytes", 60011)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {20, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 240015)
                                    .AddRate("table.datashard.write.rows", 200015)
                                .EndNested()
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "10202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 400032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "20202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280080)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260080)
                                        .AddRate("table.datashard.cache_hit.bytes", 60080)
                                        .AddRate("table.datashard.cache_miss.bytes", 80080)
                                        .AddRate("table.datashard.consumed_cpu_us", 500080)
                                        .AddRate("table.datashard.erase.bytes", 240080)
                                        .AddRate("table.datashard.erase.rows", 220080)
                                        .AddRate("table.datashard.read.bytes", 140380160)
                                        .AddRate("table.datashard.read.rows", 60300160)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320080)
                                        .AddRate("table.datashard.scan.rows", 300080)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 120080)
                                        .AddRate("table.datashard.write.rows", 100080)
                                    .EndNested()
                                    .StartNested("follower_id", "30202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140128)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130128)
                                        .AddRate("table.datashard.cache_hit.bytes", 30128)
                                        .AddRate("table.datashard.cache_miss.bytes", 40128)
                                        .AddRate("table.datashard.consumed_cpu_us", 600128)
                                        .AddRate("table.datashard.erase.bytes", 120128)
                                        .AddRate("table.datashard.erase.rows", 110128)
                                        .AddRate("table.datashard.read.bytes", 70190256)
                                        .AddRate("table.datashard.read.rows", 30150256)
                                        .AddGauge("table.datashard.row_count", 10128)
                                        .AddRate("table.datashard.scan.bytes", 160128)
                                        .AddRate("table.datashard.scan.rows", 150128)
                                        .AddGauge("table.datashard.size_bytes", 20128)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                        .AddRate("table.datashard.write.bytes", 60128)
                                        .AddRate("table.datashard.write.rows", 50128)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 560240)
                                        .AddRate("table.datashard.bulk_upsert.rows", 520240)
                                        .AddRate("table.datashard.cache_hit.bytes", 120240)
                                        .AddRate("table.datashard.cache_miss.bytes", 160240)
                                        .AddRate("table.datashard.consumed_cpu_us", 1500240)
                                        .AddRate("table.datashard.erase.bytes", 480240)
                                        .AddRate("table.datashard.erase.rows", 440240)
                                        .AddRate("table.datashard.read.bytes", 280760480)
                                        .AddRate("table.datashard.read.rows", 120600480)
                                        .AddGauge("table.datashard.row_count", 30176)
                                        .AddRate("table.datashard.scan.bytes", 640240)
                                        .AddRate("table.datashard.scan.rows", 600240)
                                        .AddGauge("table.datashard.size_bytes", 60176)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {10, 1},
                                                {50, 1},
                                                {70, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 240240)
                                        .AddRate("table.datashard.write.rows", 200240)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 560240)
                                    .AddRate("table.datashard.bulk_upsert.rows", 520240)
                                    .AddRate("table.datashard.cache_hit.bytes", 120240)
                                    .AddRate("table.datashard.cache_miss.bytes", 160240)
                                    .AddRate("table.datashard.consumed_cpu_us", 1500240)
                                    .AddRate("table.datashard.erase.bytes", 480240)
                                    .AddRate("table.datashard.erase.rows", 440240)
                                    .AddRate("table.datashard.read.bytes", 280760480)
                                    .AddRate("table.datashard.read.rows", 120600480)
                                    .AddGauge("table.datashard.row_count", 30176)
                                    .AddRate("table.datashard.scan.bytes", 640240)
                                    .AddRate("table.datashard.scan.rows", 600240)
                                    .AddGauge("table.datashard.size_bytes", 60176)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {50, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 240240)
                                    .AddRate("table.datashard.write.rows", 200240)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 700512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "20302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 281280)
                                        .AddRate("table.datashard.bulk_upsert.rows", 261280)
                                        .AddRate("table.datashard.cache_hit.bytes", 61280)
                                        .AddRate("table.datashard.cache_miss.bytes", 81280)
                                        .AddRate("table.datashard.consumed_cpu_us", 801280)
                                        .AddRate("table.datashard.erase.bytes", 241280)
                                        .AddRate("table.datashard.erase.rows", 221280)
                                        .AddRate("table.datashard.read.bytes", 140382560)
                                        .AddRate("table.datashard.read.rows", 60302560)
                                        .AddGauge("table.datashard.row_count", 10256)
                                        .AddRate("table.datashard.scan.bytes", 321280)
                                        .AddRate("table.datashard.scan.rows", 301280)
                                        .AddGauge("table.datashard.size_bytes", 20256)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 121280)
                                        .AddRate("table.datashard.write.rows", 101280)
                                    .EndNested()
                                    .StartNested("follower_id", "30302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 142048)
                                        .AddRate("table.datashard.bulk_upsert.rows", 132048)
                                        .AddRate("table.datashard.cache_hit.bytes", 32048)
                                        .AddRate("table.datashard.cache_miss.bytes", 42048)
                                        .AddRate("table.datashard.consumed_cpu_us", 902048)
                                        .AddRate("table.datashard.erase.bytes", 122048)
                                        .AddRate("table.datashard.erase.rows", 112048)
                                        .AddRate("table.datashard.read.bytes", 70194096)
                                        .AddRate("table.datashard.read.rows", 30154096)
                                        .AddGauge("table.datashard.row_count", 12048)
                                        .AddRate("table.datashard.scan.bytes", 162048)
                                        .AddRate("table.datashard.scan.rows", 152048)
                                        .AddGauge("table.datashard.size_bytes", 22048)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 62048)
                                        .AddRate("table.datashard.write.rows", 52048)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 563840)
                                        .AddRate("table.datashard.bulk_upsert.rows", 523840)
                                        .AddRate("table.datashard.cache_hit.bytes", 123840)
                                        .AddRate("table.datashard.cache_miss.bytes", 163840)
                                        .AddRate("table.datashard.consumed_cpu_us", 2403840)
                                        .AddRate("table.datashard.erase.bytes", 483840)
                                        .AddRate("table.datashard.erase.rows", 443840)
                                        .AddRate("table.datashard.read.bytes", 280767680)
                                        .AddRate("table.datashard.read.rows", 120607680)
                                        .AddGauge("table.datashard.row_count", 32816)
                                        .AddRate("table.datashard.scan.bytes", 643840)
                                        .AddRate("table.datashard.scan.rows", 603840)
                                        .AddGauge("table.datashard.size_bytes", 62816)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {10, 1},
                                                {80, 1},
                                                {100, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 243840)
                                        .AddRate("table.datashard.write.rows", 203840)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 563840)
                                    .AddRate("table.datashard.bulk_upsert.rows", 523840)
                                    .AddRate("table.datashard.cache_hit.bytes", 123840)
                                    .AddRate("table.datashard.cache_miss.bytes", 163840)
                                    .AddRate("table.datashard.consumed_cpu_us", 2403840)
                                    .AddRate("table.datashard.erase.bytes", 483840)
                                    .AddRate("table.datashard.erase.rows", 443840)
                                    .AddRate("table.datashard.read.bytes", 280767680)
                                    .AddRate("table.datashard.read.rows", 120607680)
                                    .AddGauge("table.datashard.row_count", 32816)
                                    .AddRate("table.datashard.scan.bytes", 643840)
                                    .AddRate("table.datashard.scan.rows", 603840)
                                    .AddGauge("table.datashard.size_bytes", 62816)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {80, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 243840)
                                    .AddRate("table.datashard.write.rows", 203840)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 1684095)
                            .AddRate("table.datashard.bulk_upsert.rows", 1564095)
                            .AddRate("table.datashard.cache_hit.bytes", 364095)
                            .AddRate("table.datashard.cache_miss.bytes", 484095)
                            .AddRate("table.datashard.consumed_cpu_us", 4504095)
                            .AddRate("table.datashard.erase.bytes", 1444095)
                            .AddRate("table.datashard.erase.rows", 1324095)
                            .AddRate("table.datashard.read.bytes", 842288190)
                            .AddRate("table.datashard.read.rows", 361808190)
                            .AddGauge("table.datashard.row_count", 93003)
                            .AddRate("table.datashard.scan.bytes", 1924095)
                            .AddRate("table.datashard.scan.rows", 1804095)
                            .AddGauge("table.datashard.size_bytes", 183003)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {10, 3},
                                    {20, 1},
                                    {40, 1},
                                    {50, 1},
                                    {70, 1},
                                    {80, 1},
                                    {100, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 724095)
                            .AddRate("table.datashard.write.rows", 604095)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "20101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                    .AddRate("table.datashard.cache_hit.bytes", 60010)
                                    .AddRate("table.datashard.cache_miss.bytes", 80010)
                                    .AddRate("table.datashard.consumed_cpu_us", 200010)
                                    .AddRate("table.datashard.erase.bytes", 240010)
                                    .AddRate("table.datashard.erase.rows", 220010)
                                    .AddRate("table.datashard.read.bytes", 140380020)
                                    .AddRate("table.datashard.read.rows", 60300020)
                                    .AddGauge("table.datashard.row_count", 10008)
                                    .AddRate("table.datashard.scan.bytes", 320010)
                                    .AddRate("table.datashard.scan.rows", 300010)
                                    .AddGauge("table.datashard.size_bytes", 20008)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                    .AddRate("table.datashard.write.bytes", 120010)
                                    .AddRate("table.datashard.write.rows", 100010)
                                .EndNested()
                                .StartNested("follower_id", "30101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 300004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 560015)
                                    .AddRate("table.datashard.bulk_upsert.rows", 520015)
                                    .AddRate("table.datashard.cache_hit.bytes", 120015)
                                    .AddRate("table.datashard.cache_miss.bytes", 160015)
                                    .AddRate("table.datashard.consumed_cpu_us", 600015)
                                    .AddRate("table.datashard.erase.bytes", 480015)
                                    .AddRate("table.datashard.erase.rows", 440015)
                                    .AddRate("table.datashard.read.bytes", 280760030)
                                    .AddRate("table.datashard.read.rows", 120600030)
                                    .AddGauge("table.datashard.row_count", 30013)
                                    .AddRate("table.datashard.scan.bytes", 640015)
                                    .AddRate("table.datashard.scan.rows", 600015)
                                    .AddGauge("table.datashard.size_bytes", 60013)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {20, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 240015)
                                    .AddRate("table.datashard.write.rows", 200015)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 560015)
                                .AddRate("table.datashard.bulk_upsert.rows", 520015)
                                .AddRate("table.datashard.cache_hit.bytes", 120015)
                                .AddRate("table.datashard.cache_miss.bytes", 160015)
                                .AddRate("table.datashard.consumed_cpu_us", 600015)
                                .AddRate("table.datashard.erase.bytes", 480015)
                                .AddRate("table.datashard.erase.rows", 440015)
                                .AddRate("table.datashard.read.bytes", 280760030)
                                .AddRate("table.datashard.read.rows", 120600030)
                                .AddGauge("table.datashard.row_count", 30013)
                                .AddRate("table.datashard.scan.bytes", 640015)
                                .AddRate("table.datashard.scan.rows", 600015)
                                .AddGauge("table.datashard.size_bytes", 60013)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {10, 1},
                                        {20, 1},
                                        {40, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 240015)
                                .AddRate("table.datashard.write.rows", 200015)
                            .EndNested()
                            .StartNested("tablet_id", "201")
                                .StartNested("follower_id", "10201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 400016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("follower_id", "20201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280160)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260160)
                                    .AddRate("table.datashard.cache_hit.bytes", 60160)
                                    .AddRate("table.datashard.cache_miss.bytes", 80160)
                                    .AddRate("table.datashard.consumed_cpu_us", 500160)
                                    .AddRate("table.datashard.erase.bytes", 240160)
                                    .AddRate("table.datashard.erase.rows", 220160)
                                    .AddRate("table.datashard.read.bytes", 140380320)
                                    .AddRate("table.datashard.read.rows", 60300320)
                                    .AddGauge("table.datashard.row_count", 10128)
                                    .AddRate("table.datashard.scan.bytes", 320160)
                                    .AddRate("table.datashard.scan.rows", 300160)
                                    .AddGauge("table.datashard.size_bytes", 20128)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                    .AddRate("table.datashard.write.bytes", 120160)
                                    .AddRate("table.datashard.write.rows", 100160)
                                .EndNested()
                                .StartNested("follower_id", "30201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 600064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 560240)
                                    .AddRate("table.datashard.bulk_upsert.rows", 520240)
                                    .AddRate("table.datashard.cache_hit.bytes", 120240)
                                    .AddRate("table.datashard.cache_miss.bytes", 160240)
                                    .AddRate("table.datashard.consumed_cpu_us", 1500240)
                                    .AddRate("table.datashard.erase.bytes", 480240)
                                    .AddRate("table.datashard.erase.rows", 440240)
                                    .AddRate("table.datashard.read.bytes", 280760480)
                                    .AddRate("table.datashard.read.rows", 120600480)
                                    .AddGauge("table.datashard.row_count", 30208)
                                    .AddRate("table.datashard.scan.bytes", 640240)
                                    .AddRate("table.datashard.scan.rows", 600240)
                                    .AddGauge("table.datashard.size_bytes", 60208)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {50, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 240240)
                                    .AddRate("table.datashard.write.rows", 200240)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 560240)
                                .AddRate("table.datashard.bulk_upsert.rows", 520240)
                                .AddRate("table.datashard.cache_hit.bytes", 120240)
                                .AddRate("table.datashard.cache_miss.bytes", 160240)
                                .AddRate("table.datashard.consumed_cpu_us", 1500240)
                                .AddRate("table.datashard.erase.bytes", 480240)
                                .AddRate("table.datashard.erase.rows", 440240)
                                .AddRate("table.datashard.read.bytes", 280760480)
                                .AddRate("table.datashard.read.rows", 120600480)
                                .AddGauge("table.datashard.row_count", 30208)
                                .AddRate("table.datashard.scan.bytes", 640240)
                                .AddRate("table.datashard.scan.rows", 600240)
                                .AddGauge("table.datashard.size_bytes", 60208)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {10, 1},
                                        {50, 1},
                                        {70, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 240240)
                                .AddRate("table.datashard.write.rows", 200240)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                    .AddRate("table.datashard.cache_hit.bytes", 30256)
                                    .AddRate("table.datashard.cache_miss.bytes", 40256)
                                    .AddRate("table.datashard.consumed_cpu_us", 700256)
                                    .AddRate("table.datashard.erase.bytes", 120256)
                                    .AddRate("table.datashard.erase.rows", 110256)
                                    .AddRate("table.datashard.read.bytes", 70190512)
                                    .AddRate("table.datashard.read.rows", 30150512)
                                    .AddGauge("table.datashard.row_count", 10256)
                                    .AddRate("table.datashard.scan.bytes", 160256)
                                    .AddRate("table.datashard.scan.rows", 150256)
                                    .AddGauge("table.datashard.size_bytes", 20256)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 60256)
                                    .AddRate("table.datashard.write.rows", 50256)
                                .EndNested()
                                .StartNested("follower_id", "20301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 282560)
                                    .AddRate("table.datashard.bulk_upsert.rows", 262560)
                                    .AddRate("table.datashard.cache_hit.bytes", 62560)
                                    .AddRate("table.datashard.cache_miss.bytes", 82560)
                                    .AddRate("table.datashard.consumed_cpu_us", 802560)
                                    .AddRate("table.datashard.erase.bytes", 242560)
                                    .AddRate("table.datashard.erase.rows", 222560)
                                    .AddRate("table.datashard.read.bytes", 140385120)
                                    .AddRate("table.datashard.read.rows", 60305120)
                                    .AddGauge("table.datashard.row_count", 12048)
                                    .AddRate("table.datashard.scan.bytes", 322560)
                                    .AddRate("table.datashard.scan.rows", 302560)
                                    .AddGauge("table.datashard.size_bytes", 22048)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                    .AddRate("table.datashard.write.bytes", 122560)
                                    .AddRate("table.datashard.write.rows", 102560)
                                .EndNested()
                                .StartNested("follower_id", "30301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 141024)
                                    .AddRate("table.datashard.bulk_upsert.rows", 131024)
                                    .AddRate("table.datashard.cache_hit.bytes", 31024)
                                    .AddRate("table.datashard.cache_miss.bytes", 41024)
                                    .AddRate("table.datashard.consumed_cpu_us", 901024)
                                    .AddRate("table.datashard.erase.bytes", 121024)
                                    .AddRate("table.datashard.erase.rows", 111024)
                                    .AddRate("table.datashard.read.bytes", 70192048)
                                    .AddRate("table.datashard.read.rows", 30152048)
                                    .AddGauge("table.datashard.row_count", 11024)
                                    .AddRate("table.datashard.scan.bytes", 161024)
                                    .AddRate("table.datashard.scan.rows", 151024)
                                    .AddGauge("table.datashard.size_bytes", 21024)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                    .AddRate("table.datashard.write.bytes", 61024)
                                    .AddRate("table.datashard.write.rows", 51024)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 563840)
                                    .AddRate("table.datashard.bulk_upsert.rows", 523840)
                                    .AddRate("table.datashard.cache_hit.bytes", 123840)
                                    .AddRate("table.datashard.cache_miss.bytes", 163840)
                                    .AddRate("table.datashard.consumed_cpu_us", 2403840)
                                    .AddRate("table.datashard.erase.bytes", 483840)
                                    .AddRate("table.datashard.erase.rows", 443840)
                                    .AddRate("table.datashard.read.bytes", 280767680)
                                    .AddRate("table.datashard.read.rows", 120607680)
                                    .AddGauge("table.datashard.row_count", 33328)
                                    .AddRate("table.datashard.scan.bytes", 643840)
                                    .AddRate("table.datashard.scan.rows", 603840)
                                    .AddGauge("table.datashard.size_bytes", 63328)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {80, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 243840)
                                    .AddRate("table.datashard.write.rows", 203840)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 563840)
                                .AddRate("table.datashard.bulk_upsert.rows", 523840)
                                .AddRate("table.datashard.cache_hit.bytes", 123840)
                                .AddRate("table.datashard.cache_miss.bytes", 163840)
                                .AddRate("table.datashard.consumed_cpu_us", 2403840)
                                .AddRate("table.datashard.erase.bytes", 483840)
                                .AddRate("table.datashard.erase.rows", 443840)
                                .AddRate("table.datashard.read.bytes", 280767680)
                                .AddRate("table.datashard.read.rows", 120607680)
                                .AddGauge("table.datashard.row_count", 33328)
                                .AddRate("table.datashard.scan.bytes", 643840)
                                .AddRate("table.datashard.scan.rows", 603840)
                                .AddGauge("table.datashard.size_bytes", 63328)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {10, 1},
                                        {80, 1},
                                        {100, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 243840)
                                .AddRate("table.datashard.write.rows", 203840)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 1684095)
                        .AddRate("table.datashard.bulk_upsert.rows", 1564095)
                        .AddRate("table.datashard.cache_hit.bytes", 364095)
                        .AddRate("table.datashard.cache_miss.bytes", 484095)
                        .AddRate("table.datashard.consumed_cpu_us", 4504095)
                        .AddRate("table.datashard.erase.bytes", 1444095)
                        .AddRate("table.datashard.erase.rows", 1324095)
                        .AddRate("table.datashard.read.bytes", 842288190)
                        .AddRate("table.datashard.read.rows", 361808190)
                        .AddGauge("table.datashard.row_count", 93549)
                        .AddRate("table.datashard.scan.bytes", 1924095)
                        .AddRate("table.datashard.scan.rows", 1804095)
                        .AddGauge("table.datashard.size_bytes", 183549)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {10, 3},
                                {20, 1},
                                {40, 1},
                                {50, 1},
                                {70, 1},
                                {80, 1},
                                {100, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 724095)
                        .AddRate("table.datashard.write.rows", 604095)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

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

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting followers):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 100002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "30102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140008)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130008)
                                        .AddRate("table.datashard.cache_hit.bytes", 30008)
                                        .AddRate("table.datashard.cache_miss.bytes", 40008)
                                        .AddRate("table.datashard.consumed_cpu_us", 300008)
                                        .AddRate("table.datashard.erase.bytes", 120008)
                                        .AddRate("table.datashard.erase.rows", 110008)
                                        .AddRate("table.datashard.read.bytes", 70190016)
                                        .AddRate("table.datashard.read.rows", 30150016)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 160008)
                                        .AddRate("table.datashard.scan.rows", 150008)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60008)
                                        .AddRate("table.datashard.write.rows", 50008)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 400010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 20010)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 40010)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {20, 1},
                                                {40, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                    .AddRate("table.datashard.cache_hit.bytes", 60010)
                                    .AddRate("table.datashard.cache_miss.bytes", 80010)
                                    .AddRate("table.datashard.consumed_cpu_us", 400010)
                                    .AddRate("table.datashard.erase.bytes", 240010)
                                    .AddRate("table.datashard.erase.rows", 220010)
                                    .AddRate("table.datashard.read.bytes", 140380020)
                                    .AddRate("table.datashard.read.rows", 60300020)
                                    .AddGauge("table.datashard.row_count", 20010)
                                    .AddRate("table.datashard.scan.bytes", 320010)
                                    .AddRate("table.datashard.scan.rows", 300010)
                                    .AddGauge("table.datashard.size_bytes", 40010)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120010)
                                    .AddRate("table.datashard.write.rows", 100010)
                                .EndNested()
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "10202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 400032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "30202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140128)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130128)
                                        .AddRate("table.datashard.cache_hit.bytes", 30128)
                                        .AddRate("table.datashard.cache_miss.bytes", 40128)
                                        .AddRate("table.datashard.consumed_cpu_us", 600128)
                                        .AddRate("table.datashard.erase.bytes", 120128)
                                        .AddRate("table.datashard.erase.rows", 110128)
                                        .AddRate("table.datashard.read.bytes", 70190256)
                                        .AddRate("table.datashard.read.rows", 30150256)
                                        .AddGauge("table.datashard.row_count", 10128)
                                        .AddRate("table.datashard.scan.bytes", 160128)
                                        .AddRate("table.datashard.scan.rows", 150128)
                                        .AddGauge("table.datashard.size_bytes", 20128)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                        .AddRate("table.datashard.write.bytes", 60128)
                                        .AddRate("table.datashard.write.rows", 50128)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280160)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260160)
                                        .AddRate("table.datashard.cache_hit.bytes", 60160)
                                        .AddRate("table.datashard.cache_miss.bytes", 80160)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000160)
                                        .AddRate("table.datashard.erase.bytes", 240160)
                                        .AddRate("table.datashard.erase.rows", 220160)
                                        .AddRate("table.datashard.read.bytes", 140380320)
                                        .AddRate("table.datashard.read.rows", 60300320)
                                        .AddGauge("table.datashard.row_count", 20160)
                                        .AddRate("table.datashard.scan.bytes", 320160)
                                        .AddRate("table.datashard.scan.rows", 300160)
                                        .AddGauge("table.datashard.size_bytes", 40160)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {50, 1},
                                                {70, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 120160)
                                        .AddRate("table.datashard.write.rows", 100160)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280160)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260160)
                                    .AddRate("table.datashard.cache_hit.bytes", 60160)
                                    .AddRate("table.datashard.cache_miss.bytes", 80160)
                                    .AddRate("table.datashard.consumed_cpu_us", 1000160)
                                    .AddRate("table.datashard.erase.bytes", 240160)
                                    .AddRate("table.datashard.erase.rows", 220160)
                                    .AddRate("table.datashard.read.bytes", 140380320)
                                    .AddRate("table.datashard.read.rows", 60300320)
                                    .AddGauge("table.datashard.row_count", 20160)
                                    .AddRate("table.datashard.scan.bytes", 320160)
                                    .AddRate("table.datashard.scan.rows", 300160)
                                    .AddGauge("table.datashard.size_bytes", 40160)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {50, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120160)
                                    .AddRate("table.datashard.write.rows", 100160)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 700512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "30302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 142048)
                                        .AddRate("table.datashard.bulk_upsert.rows", 132048)
                                        .AddRate("table.datashard.cache_hit.bytes", 32048)
                                        .AddRate("table.datashard.cache_miss.bytes", 42048)
                                        .AddRate("table.datashard.consumed_cpu_us", 902048)
                                        .AddRate("table.datashard.erase.bytes", 122048)
                                        .AddRate("table.datashard.erase.rows", 112048)
                                        .AddRate("table.datashard.read.bytes", 70194096)
                                        .AddRate("table.datashard.read.rows", 30154096)
                                        .AddGauge("table.datashard.row_count", 12048)
                                        .AddRate("table.datashard.scan.bytes", 162048)
                                        .AddRate("table.datashard.scan.rows", 152048)
                                        .AddGauge("table.datashard.size_bytes", 22048)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 62048)
                                        .AddRate("table.datashard.write.rows", 52048)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 282560)
                                        .AddRate("table.datashard.bulk_upsert.rows", 262560)
                                        .AddRate("table.datashard.cache_hit.bytes", 62560)
                                        .AddRate("table.datashard.cache_miss.bytes", 82560)
                                        .AddRate("table.datashard.consumed_cpu_us", 1602560)
                                        .AddRate("table.datashard.erase.bytes", 242560)
                                        .AddRate("table.datashard.erase.rows", 222560)
                                        .AddRate("table.datashard.read.bytes", 140385120)
                                        .AddRate("table.datashard.read.rows", 60305120)
                                        .AddGauge("table.datashard.row_count", 22560)
                                        .AddRate("table.datashard.scan.bytes", 322560)
                                        .AddRate("table.datashard.scan.rows", 302560)
                                        .AddGauge("table.datashard.size_bytes", 42560)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {80, 1},
                                                {100, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 122560)
                                        .AddRate("table.datashard.write.rows", 102560)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 282560)
                                    .AddRate("table.datashard.bulk_upsert.rows", 262560)
                                    .AddRate("table.datashard.cache_hit.bytes", 62560)
                                    .AddRate("table.datashard.cache_miss.bytes", 82560)
                                    .AddRate("table.datashard.consumed_cpu_us", 1602560)
                                    .AddRate("table.datashard.erase.bytes", 242560)
                                    .AddRate("table.datashard.erase.rows", 222560)
                                    .AddRate("table.datashard.read.bytes", 140385120)
                                    .AddRate("table.datashard.read.rows", 60305120)
                                    .AddGauge("table.datashard.row_count", 22560)
                                    .AddRate("table.datashard.scan.bytes", 322560)
                                    .AddRate("table.datashard.scan.rows", 302560)
                                    .AddGauge("table.datashard.size_bytes", 42560)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 122560)
                                    .AddRate("table.datashard.write.rows", 102560)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 842730)
                            .AddRate("table.datashard.bulk_upsert.rows", 782730)
                            .AddRate("table.datashard.cache_hit.bytes", 182730)
                            .AddRate("table.datashard.cache_miss.bytes", 242730)
                            .AddRate("table.datashard.consumed_cpu_us", 3002730)
                            .AddRate("table.datashard.erase.bytes", 722730)
                            .AddRate("table.datashard.erase.rows", 662730)
                            .AddRate("table.datashard.read.bytes", 421145460)
                            .AddRate("table.datashard.read.rows", 180905460)
                            .AddGauge("table.datashard.row_count", 62730)
                            .AddRate("table.datashard.scan.bytes", 962730)
                            .AddRate("table.datashard.scan.rows", 902730)
                            .AddGauge("table.datashard.size_bytes", 122730)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {40, 1},
                                    {50, 1},
                                    {70, 1},
                                    {80, 1},
                                    {100, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 362730)
                            .AddRate("table.datashard.write.rows", 302730)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "30101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 300004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280005)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260005)
                                    .AddRate("table.datashard.cache_hit.bytes", 60005)
                                    .AddRate("table.datashard.cache_miss.bytes", 80005)
                                    .AddRate("table.datashard.consumed_cpu_us", 400005)
                                    .AddRate("table.datashard.erase.bytes", 240005)
                                    .AddRate("table.datashard.erase.rows", 220005)
                                    .AddRate("table.datashard.read.bytes", 140380010)
                                    .AddRate("table.datashard.read.rows", 60300010)
                                    .AddGauge("table.datashard.row_count", 20005)
                                    .AddRate("table.datashard.scan.bytes", 320005)
                                    .AddRate("table.datashard.scan.rows", 300005)
                                    .AddGauge("table.datashard.size_bytes", 40005)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120005)
                                    .AddRate("table.datashard.write.rows", 100005)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280005)
                                .AddRate("table.datashard.bulk_upsert.rows", 260005)
                                .AddRate("table.datashard.cache_hit.bytes", 60005)
                                .AddRate("table.datashard.cache_miss.bytes", 80005)
                                .AddRate("table.datashard.consumed_cpu_us", 400005)
                                .AddRate("table.datashard.erase.bytes", 240005)
                                .AddRate("table.datashard.erase.rows", 220005)
                                .AddRate("table.datashard.read.bytes", 140380010)
                                .AddRate("table.datashard.read.rows", 60300010)
                                .AddGauge("table.datashard.row_count", 20005)
                                .AddRate("table.datashard.scan.bytes", 320005)
                                .AddRate("table.datashard.scan.rows", 300005)
                                .AddGauge("table.datashard.size_bytes", 40005)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {20, 1},
                                        {40, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 120005)
                                .AddRate("table.datashard.write.rows", 100005)
                            .EndNested()
                            .StartNested("tablet_id", "201")
                                .StartNested("follower_id", "10201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 400016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("follower_id", "30201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 600064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280080)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260080)
                                    .AddRate("table.datashard.cache_hit.bytes", 60080)
                                    .AddRate("table.datashard.cache_miss.bytes", 80080)
                                    .AddRate("table.datashard.consumed_cpu_us", 1000080)
                                    .AddRate("table.datashard.erase.bytes", 240080)
                                    .AddRate("table.datashard.erase.rows", 220080)
                                    .AddRate("table.datashard.read.bytes", 140380160)
                                    .AddRate("table.datashard.read.rows", 60300160)
                                    .AddGauge("table.datashard.row_count", 20080)
                                    .AddRate("table.datashard.scan.bytes", 320080)
                                    .AddRate("table.datashard.scan.rows", 300080)
                                    .AddGauge("table.datashard.size_bytes", 40080)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {50, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120080)
                                    .AddRate("table.datashard.write.rows", 100080)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280080)
                                .AddRate("table.datashard.bulk_upsert.rows", 260080)
                                .AddRate("table.datashard.cache_hit.bytes", 60080)
                                .AddRate("table.datashard.cache_miss.bytes", 80080)
                                .AddRate("table.datashard.consumed_cpu_us", 1000080)
                                .AddRate("table.datashard.erase.bytes", 240080)
                                .AddRate("table.datashard.erase.rows", 220080)
                                .AddRate("table.datashard.read.bytes", 140380160)
                                .AddRate("table.datashard.read.rows", 60300160)
                                .AddGauge("table.datashard.row_count", 20080)
                                .AddRate("table.datashard.scan.bytes", 320080)
                                .AddRate("table.datashard.scan.rows", 300080)
                                .AddGauge("table.datashard.size_bytes", 40080)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {50, 1},
                                        {70, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 120080)
                                .AddRate("table.datashard.write.rows", 100080)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                    .AddRate("table.datashard.cache_hit.bytes", 30256)
                                    .AddRate("table.datashard.cache_miss.bytes", 40256)
                                    .AddRate("table.datashard.consumed_cpu_us", 700256)
                                    .AddRate("table.datashard.erase.bytes", 120256)
                                    .AddRate("table.datashard.erase.rows", 110256)
                                    .AddRate("table.datashard.read.bytes", 70190512)
                                    .AddRate("table.datashard.read.rows", 30150512)
                                    .AddGauge("table.datashard.row_count", 10256)
                                    .AddRate("table.datashard.scan.bytes", 160256)
                                    .AddRate("table.datashard.scan.rows", 150256)
                                    .AddGauge("table.datashard.size_bytes", 20256)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 60256)
                                    .AddRate("table.datashard.write.rows", 50256)
                                .EndNested()
                                .StartNested("follower_id", "30301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 141024)
                                    .AddRate("table.datashard.bulk_upsert.rows", 131024)
                                    .AddRate("table.datashard.cache_hit.bytes", 31024)
                                    .AddRate("table.datashard.cache_miss.bytes", 41024)
                                    .AddRate("table.datashard.consumed_cpu_us", 901024)
                                    .AddRate("table.datashard.erase.bytes", 121024)
                                    .AddRate("table.datashard.erase.rows", 111024)
                                    .AddRate("table.datashard.read.bytes", 70192048)
                                    .AddRate("table.datashard.read.rows", 30152048)
                                    .AddGauge("table.datashard.row_count", 11024)
                                    .AddRate("table.datashard.scan.bytes", 161024)
                                    .AddRate("table.datashard.scan.rows", 151024)
                                    .AddGauge("table.datashard.size_bytes", 21024)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                    .AddRate("table.datashard.write.bytes", 61024)
                                    .AddRate("table.datashard.write.rows", 51024)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 281280)
                                    .AddRate("table.datashard.bulk_upsert.rows", 261280)
                                    .AddRate("table.datashard.cache_hit.bytes", 61280)
                                    .AddRate("table.datashard.cache_miss.bytes", 81280)
                                    .AddRate("table.datashard.consumed_cpu_us", 1601280)
                                    .AddRate("table.datashard.erase.bytes", 241280)
                                    .AddRate("table.datashard.erase.rows", 221280)
                                    .AddRate("table.datashard.read.bytes", 140382560)
                                    .AddRate("table.datashard.read.rows", 60302560)
                                    .AddGauge("table.datashard.row_count", 21280)
                                    .AddRate("table.datashard.scan.bytes", 321280)
                                    .AddRate("table.datashard.scan.rows", 301280)
                                    .AddGauge("table.datashard.size_bytes", 41280)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 121280)
                                    .AddRate("table.datashard.write.rows", 101280)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 281280)
                                .AddRate("table.datashard.bulk_upsert.rows", 261280)
                                .AddRate("table.datashard.cache_hit.bytes", 61280)
                                .AddRate("table.datashard.cache_miss.bytes", 81280)
                                .AddRate("table.datashard.consumed_cpu_us", 1601280)
                                .AddRate("table.datashard.erase.bytes", 241280)
                                .AddRate("table.datashard.erase.rows", 221280)
                                .AddRate("table.datashard.read.bytes", 140382560)
                                .AddRate("table.datashard.read.rows", 60302560)
                                .AddGauge("table.datashard.row_count", 21280)
                                .AddRate("table.datashard.scan.bytes", 321280)
                                .AddRate("table.datashard.scan.rows", 301280)
                                .AddGauge("table.datashard.size_bytes", 41280)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {80, 1},
                                        {100, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 121280)
                                .AddRate("table.datashard.write.rows", 101280)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 841365)
                        .AddRate("table.datashard.bulk_upsert.rows", 781365)
                        .AddRate("table.datashard.cache_hit.bytes", 181365)
                        .AddRate("table.datashard.cache_miss.bytes", 241365)
                        .AddRate("table.datashard.consumed_cpu_us", 3001365)
                        .AddRate("table.datashard.erase.bytes", 721365)
                        .AddRate("table.datashard.erase.rows", 661365)
                        .AddRate("table.datashard.read.bytes", 421142730)
                        .AddRate("table.datashard.read.rows", 180902730)
                        .AddGauge("table.datashard.row_count", 61365)
                        .AddRate("table.datashard.scan.bytes", 961365)
                        .AddRate("table.datashard.scan.rows", 901365)
                        .AddGauge("table.datashard.size_bytes", 121365)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {20, 1},
                                {40, 1},
                                {50, 1},
                                {70, 1},
                                {80, 1},
                                {100, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 361365)
                        .AddRate("table.datashard.write.rows", 301365)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

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

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting partitions):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 100002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "30102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140008)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130008)
                                        .AddRate("table.datashard.cache_hit.bytes", 30008)
                                        .AddRate("table.datashard.cache_miss.bytes", 40008)
                                        .AddRate("table.datashard.consumed_cpu_us", 300008)
                                        .AddRate("table.datashard.erase.bytes", 120008)
                                        .AddRate("table.datashard.erase.rows", 110008)
                                        .AddRate("table.datashard.read.bytes", 70190016)
                                        .AddRate("table.datashard.read.rows", 30150016)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 160008)
                                        .AddRate("table.datashard.scan.rows", 150008)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60008)
                                        .AddRate("table.datashard.write.rows", 50008)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 400010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 20010)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 40010)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {20, 1},
                                                {40, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                    .AddRate("table.datashard.cache_hit.bytes", 60010)
                                    .AddRate("table.datashard.cache_miss.bytes", 80010)
                                    .AddRate("table.datashard.consumed_cpu_us", 400010)
                                    .AddRate("table.datashard.erase.bytes", 240010)
                                    .AddRate("table.datashard.erase.rows", 220010)
                                    .AddRate("table.datashard.read.bytes", 140380020)
                                    .AddRate("table.datashard.read.rows", 60300020)
                                    .AddGauge("table.datashard.row_count", 20010)
                                    .AddRate("table.datashard.scan.bytes", 320010)
                                    .AddRate("table.datashard.scan.rows", 300010)
                                    .AddGauge("table.datashard.size_bytes", 40010)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120010)
                                    .AddRate("table.datashard.write.rows", 100010)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 700512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "30302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 142048)
                                        .AddRate("table.datashard.bulk_upsert.rows", 132048)
                                        .AddRate("table.datashard.cache_hit.bytes", 32048)
                                        .AddRate("table.datashard.cache_miss.bytes", 42048)
                                        .AddRate("table.datashard.consumed_cpu_us", 902048)
                                        .AddRate("table.datashard.erase.bytes", 122048)
                                        .AddRate("table.datashard.erase.rows", 112048)
                                        .AddRate("table.datashard.read.bytes", 70194096)
                                        .AddRate("table.datashard.read.rows", 30154096)
                                        .AddGauge("table.datashard.row_count", 12048)
                                        .AddRate("table.datashard.scan.bytes", 162048)
                                        .AddRate("table.datashard.scan.rows", 152048)
                                        .AddGauge("table.datashard.size_bytes", 22048)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 62048)
                                        .AddRate("table.datashard.write.rows", 52048)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 282560)
                                        .AddRate("table.datashard.bulk_upsert.rows", 262560)
                                        .AddRate("table.datashard.cache_hit.bytes", 62560)
                                        .AddRate("table.datashard.cache_miss.bytes", 82560)
                                        .AddRate("table.datashard.consumed_cpu_us", 1602560)
                                        .AddRate("table.datashard.erase.bytes", 242560)
                                        .AddRate("table.datashard.erase.rows", 222560)
                                        .AddRate("table.datashard.read.bytes", 140385120)
                                        .AddRate("table.datashard.read.rows", 60305120)
                                        .AddGauge("table.datashard.row_count", 22560)
                                        .AddRate("table.datashard.scan.bytes", 322560)
                                        .AddRate("table.datashard.scan.rows", 302560)
                                        .AddGauge("table.datashard.size_bytes", 42560)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {80, 1},
                                                {100, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 122560)
                                        .AddRate("table.datashard.write.rows", 102560)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 282560)
                                    .AddRate("table.datashard.bulk_upsert.rows", 262560)
                                    .AddRate("table.datashard.cache_hit.bytes", 62560)
                                    .AddRate("table.datashard.cache_miss.bytes", 82560)
                                    .AddRate("table.datashard.consumed_cpu_us", 1602560)
                                    .AddRate("table.datashard.erase.bytes", 242560)
                                    .AddRate("table.datashard.erase.rows", 222560)
                                    .AddRate("table.datashard.read.bytes", 140385120)
                                    .AddRate("table.datashard.read.rows", 60305120)
                                    .AddGauge("table.datashard.row_count", 22560)
                                    .AddRate("table.datashard.scan.bytes", 322560)
                                    .AddRate("table.datashard.scan.rows", 302560)
                                    .AddGauge("table.datashard.size_bytes", 42560)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 122560)
                                    .AddRate("table.datashard.write.rows", 102560)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 562570)
                            .AddRate("table.datashard.bulk_upsert.rows", 522570)
                            .AddRate("table.datashard.cache_hit.bytes", 122570)
                            .AddRate("table.datashard.cache_miss.bytes", 162570)
                            .AddRate("table.datashard.consumed_cpu_us", 2002570)
                            .AddRate("table.datashard.erase.bytes", 482570)
                            .AddRate("table.datashard.erase.rows", 442570)
                            .AddRate("table.datashard.read.bytes", 280765140)
                            .AddRate("table.datashard.read.rows", 120605140)
                            .AddGauge("table.datashard.row_count", 42570)
                            .AddRate("table.datashard.scan.bytes", 642570)
                            .AddRate("table.datashard.scan.rows", 602570)
                            .AddGauge("table.datashard.size_bytes", 82570)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {40, 1},
                                    {80, 1},
                                    {100, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 242570)
                            .AddRate("table.datashard.write.rows", 202570)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "30101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 300004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280005)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260005)
                                    .AddRate("table.datashard.cache_hit.bytes", 60005)
                                    .AddRate("table.datashard.cache_miss.bytes", 80005)
                                    .AddRate("table.datashard.consumed_cpu_us", 400005)
                                    .AddRate("table.datashard.erase.bytes", 240005)
                                    .AddRate("table.datashard.erase.rows", 220005)
                                    .AddRate("table.datashard.read.bytes", 140380010)
                                    .AddRate("table.datashard.read.rows", 60300010)
                                    .AddGauge("table.datashard.row_count", 20005)
                                    .AddRate("table.datashard.scan.bytes", 320005)
                                    .AddRate("table.datashard.scan.rows", 300005)
                                    .AddGauge("table.datashard.size_bytes", 40005)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120005)
                                    .AddRate("table.datashard.write.rows", 100005)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280005)
                                .AddRate("table.datashard.bulk_upsert.rows", 260005)
                                .AddRate("table.datashard.cache_hit.bytes", 60005)
                                .AddRate("table.datashard.cache_miss.bytes", 80005)
                                .AddRate("table.datashard.consumed_cpu_us", 400005)
                                .AddRate("table.datashard.erase.bytes", 240005)
                                .AddRate("table.datashard.erase.rows", 220005)
                                .AddRate("table.datashard.read.bytes", 140380010)
                                .AddRate("table.datashard.read.rows", 60300010)
                                .AddGauge("table.datashard.row_count", 20005)
                                .AddRate("table.datashard.scan.bytes", 320005)
                                .AddRate("table.datashard.scan.rows", 300005)
                                .AddGauge("table.datashard.size_bytes", 40005)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {20, 1},
                                        {40, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 120005)
                                .AddRate("table.datashard.write.rows", 100005)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                    .AddRate("table.datashard.cache_hit.bytes", 30256)
                                    .AddRate("table.datashard.cache_miss.bytes", 40256)
                                    .AddRate("table.datashard.consumed_cpu_us", 700256)
                                    .AddRate("table.datashard.erase.bytes", 120256)
                                    .AddRate("table.datashard.erase.rows", 110256)
                                    .AddRate("table.datashard.read.bytes", 70190512)
                                    .AddRate("table.datashard.read.rows", 30150512)
                                    .AddGauge("table.datashard.row_count", 10256)
                                    .AddRate("table.datashard.scan.bytes", 160256)
                                    .AddRate("table.datashard.scan.rows", 150256)
                                    .AddGauge("table.datashard.size_bytes", 20256)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 60256)
                                    .AddRate("table.datashard.write.rows", 50256)
                                .EndNested()
                                .StartNested("follower_id", "30301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 141024)
                                    .AddRate("table.datashard.bulk_upsert.rows", 131024)
                                    .AddRate("table.datashard.cache_hit.bytes", 31024)
                                    .AddRate("table.datashard.cache_miss.bytes", 41024)
                                    .AddRate("table.datashard.consumed_cpu_us", 901024)
                                    .AddRate("table.datashard.erase.bytes", 121024)
                                    .AddRate("table.datashard.erase.rows", 111024)
                                    .AddRate("table.datashard.read.bytes", 70192048)
                                    .AddRate("table.datashard.read.rows", 30152048)
                                    .AddGauge("table.datashard.row_count", 11024)
                                    .AddRate("table.datashard.scan.bytes", 161024)
                                    .AddRate("table.datashard.scan.rows", 151024)
                                    .AddGauge("table.datashard.size_bytes", 21024)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                    .AddRate("table.datashard.write.bytes", 61024)
                                    .AddRate("table.datashard.write.rows", 51024)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 281280)
                                    .AddRate("table.datashard.bulk_upsert.rows", 261280)
                                    .AddRate("table.datashard.cache_hit.bytes", 61280)
                                    .AddRate("table.datashard.cache_miss.bytes", 81280)
                                    .AddRate("table.datashard.consumed_cpu_us", 1601280)
                                    .AddRate("table.datashard.erase.bytes", 241280)
                                    .AddRate("table.datashard.erase.rows", 221280)
                                    .AddRate("table.datashard.read.bytes", 140382560)
                                    .AddRate("table.datashard.read.rows", 60302560)
                                    .AddGauge("table.datashard.row_count", 21280)
                                    .AddRate("table.datashard.scan.bytes", 321280)
                                    .AddRate("table.datashard.scan.rows", 301280)
                                    .AddGauge("table.datashard.size_bytes", 41280)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 121280)
                                    .AddRate("table.datashard.write.rows", 101280)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 281280)
                                .AddRate("table.datashard.bulk_upsert.rows", 261280)
                                .AddRate("table.datashard.cache_hit.bytes", 61280)
                                .AddRate("table.datashard.cache_miss.bytes", 81280)
                                .AddRate("table.datashard.consumed_cpu_us", 1601280)
                                .AddRate("table.datashard.erase.bytes", 241280)
                                .AddRate("table.datashard.erase.rows", 221280)
                                .AddRate("table.datashard.read.bytes", 140382560)
                                .AddRate("table.datashard.read.rows", 60302560)
                                .AddGauge("table.datashard.row_count", 21280)
                                .AddRate("table.datashard.scan.bytes", 321280)
                                .AddRate("table.datashard.scan.rows", 301280)
                                .AddGauge("table.datashard.size_bytes", 41280)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {80, 1},
                                        {100, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 121280)
                                .AddRate("table.datashard.write.rows", 101280)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 561285)
                        .AddRate("table.datashard.bulk_upsert.rows", 521285)
                        .AddRate("table.datashard.cache_hit.bytes", 121285)
                        .AddRate("table.datashard.cache_miss.bytes", 161285)
                        .AddRate("table.datashard.consumed_cpu_us", 2001285)
                        .AddRate("table.datashard.erase.bytes", 481285)
                        .AddRate("table.datashard.erase.rows", 441285)
                        .AddRate("table.datashard.read.bytes", 280762570)
                        .AddRate("table.datashard.read.rows", 120602570)
                        .AddGauge("table.datashard.row_count", 41285)
                        .AddRate("table.datashard.scan.bytes", 641285)
                        .AddRate("table.datashard.scan.rows", 601285)
                        .AddGauge("table.datashard.size_bytes", 81285)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {20, 1},
                                {40, 1},
                                {80, 1},
                                {100, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 241285)
                        .AddRate("table.datashard.write.rows", 201285)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

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

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting table 1):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 100002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "30102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140008)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130008)
                                        .AddRate("table.datashard.cache_hit.bytes", 30008)
                                        .AddRate("table.datashard.cache_miss.bytes", 40008)
                                        .AddRate("table.datashard.consumed_cpu_us", 300008)
                                        .AddRate("table.datashard.erase.bytes", 120008)
                                        .AddRate("table.datashard.erase.rows", 110008)
                                        .AddRate("table.datashard.read.bytes", 70190016)
                                        .AddRate("table.datashard.read.rows", 30150016)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 160008)
                                        .AddRate("table.datashard.scan.rows", 150008)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60008)
                                        .AddRate("table.datashard.write.rows", 50008)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 400010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 20010)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 40010)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {20, 1},
                                                {40, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                    .AddRate("table.datashard.cache_hit.bytes", 60010)
                                    .AddRate("table.datashard.cache_miss.bytes", 80010)
                                    .AddRate("table.datashard.consumed_cpu_us", 400010)
                                    .AddRate("table.datashard.erase.bytes", 240010)
                                    .AddRate("table.datashard.erase.rows", 220010)
                                    .AddRate("table.datashard.read.bytes", 140380020)
                                    .AddRate("table.datashard.read.rows", 60300020)
                                    .AddGauge("table.datashard.row_count", 20010)
                                    .AddRate("table.datashard.scan.bytes", 320010)
                                    .AddRate("table.datashard.scan.rows", 300010)
                                    .AddGauge("table.datashard.size_bytes", 40010)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120010)
                                    .AddRate("table.datashard.write.rows", 100010)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 700512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "30302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 142048)
                                        .AddRate("table.datashard.bulk_upsert.rows", 132048)
                                        .AddRate("table.datashard.cache_hit.bytes", 32048)
                                        .AddRate("table.datashard.cache_miss.bytes", 42048)
                                        .AddRate("table.datashard.consumed_cpu_us", 902048)
                                        .AddRate("table.datashard.erase.bytes", 122048)
                                        .AddRate("table.datashard.erase.rows", 112048)
                                        .AddRate("table.datashard.read.bytes", 70194096)
                                        .AddRate("table.datashard.read.rows", 30154096)
                                        .AddGauge("table.datashard.row_count", 12048)
                                        .AddRate("table.datashard.scan.bytes", 162048)
                                        .AddRate("table.datashard.scan.rows", 152048)
                                        .AddGauge("table.datashard.size_bytes", 22048)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 62048)
                                        .AddRate("table.datashard.write.rows", 52048)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 282560)
                                        .AddRate("table.datashard.bulk_upsert.rows", 262560)
                                        .AddRate("table.datashard.cache_hit.bytes", 62560)
                                        .AddRate("table.datashard.cache_miss.bytes", 82560)
                                        .AddRate("table.datashard.consumed_cpu_us", 1602560)
                                        .AddRate("table.datashard.erase.bytes", 242560)
                                        .AddRate("table.datashard.erase.rows", 222560)
                                        .AddRate("table.datashard.read.bytes", 140385120)
                                        .AddRate("table.datashard.read.rows", 60305120)
                                        .AddGauge("table.datashard.row_count", 22560)
                                        .AddRate("table.datashard.scan.bytes", 322560)
                                        .AddRate("table.datashard.scan.rows", 302560)
                                        .AddGauge("table.datashard.size_bytes", 42560)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {80, 1},
                                                {100, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 122560)
                                        .AddRate("table.datashard.write.rows", 102560)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 282560)
                                    .AddRate("table.datashard.bulk_upsert.rows", 262560)
                                    .AddRate("table.datashard.cache_hit.bytes", 62560)
                                    .AddRate("table.datashard.cache_miss.bytes", 82560)
                                    .AddRate("table.datashard.consumed_cpu_us", 1602560)
                                    .AddRate("table.datashard.erase.bytes", 242560)
                                    .AddRate("table.datashard.erase.rows", 222560)
                                    .AddRate("table.datashard.read.bytes", 140385120)
                                    .AddRate("table.datashard.read.rows", 60305120)
                                    .AddGauge("table.datashard.row_count", 22560)
                                    .AddRate("table.datashard.scan.bytes", 322560)
                                    .AddRate("table.datashard.scan.rows", 302560)
                                    .AddGauge("table.datashard.size_bytes", 42560)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 122560)
                                    .AddRate("table.datashard.write.rows", 102560)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 562570)
                            .AddRate("table.datashard.bulk_upsert.rows", 522570)
                            .AddRate("table.datashard.cache_hit.bytes", 122570)
                            .AddRate("table.datashard.cache_miss.bytes", 162570)
                            .AddRate("table.datashard.consumed_cpu_us", 2002570)
                            .AddRate("table.datashard.erase.bytes", 482570)
                            .AddRate("table.datashard.erase.rows", 442570)
                            .AddRate("table.datashard.read.bytes", 280765140)
                            .AddRate("table.datashard.read.rows", 120605140)
                            .AddGauge("table.datashard.row_count", 42570)
                            .AddRate("table.datashard.scan.bytes", 642570)
                            .AddRate("table.datashard.scan.rows", 602570)
                            .AddGauge("table.datashard.size_bytes", 82570)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {40, 1},
                                    {80, 1},
                                    {100, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 242570)
                            .AddRate("table.datashard.write.rows", 202570)
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

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101,     0,  10, &tableMetricsConfig1, 0x0001);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 10101,  20, &tableMetricsConfig1, 0x0002);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101,  30, &tableMetricsConfig1, 0x0004);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201,     0,  40, &tableMetricsConfig1, 0x0010);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 10201,  50, &tableMetricsConfig1, 0x0020);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201,  60, &tableMetricsConfig1, 0x0040);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301,     0,  70, &tableMetricsConfig1, 0x0100);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 10301,  80, &tableMetricsConfig1, 0x0200);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301,  90, &tableMetricsConfig1, 0x0400);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 4321,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable,
            .MonitoringProjectId = "fake-monitoring-project-id"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102,     0,  10, &tableMetricsConfig2, 0x0001);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 10102,  20, &tableMetricsConfig2, 0x0002);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102,  30, &tableMetricsConfig2, 0x0004);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202,     0,  40, &tableMetricsConfig2, 0x0010);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 10202,  50, &tableMetricsConfig2, 0x0020);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202,  60, &tableMetricsConfig2, 0x0040);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302,     0,  70, &tableMetricsConfig2, 0x0100);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 10302,  80, &tableMetricsConfig2, 0x0200);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302,  90, &tableMetricsConfig2, 0x0400);

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
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                        .AddRate("table.datashard.cache_hit.bytes", 30001)
                                        .AddRate("table.datashard.cache_miss.bytes", 40001)
                                        .AddRate("table.datashard.consumed_cpu_us", 100001)
                                        .AddRate("table.datashard.erase.bytes", 120001)
                                        .AddRate("table.datashard.erase.rows", 110001)
                                        .AddRate("table.datashard.read.bytes", 70190002)
                                        .AddRate("table.datashard.read.rows", 30150002)
                                        .AddGauge("table.datashard.row_count", 10001)
                                        .AddRate("table.datashard.scan.bytes", 160001)
                                        .AddRate("table.datashard.scan.rows", 150001)
                                        .AddGauge("table.datashard.size_bytes", 20001)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60001)
                                        .AddRate("table.datashard.write.rows", 50001)
                                    .EndNested()
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "20102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                        .AddRate("table.datashard.cache_hit.bytes", 30004)
                                        .AddRate("table.datashard.cache_miss.bytes", 40004)
                                        .AddRate("table.datashard.consumed_cpu_us", 300004)
                                        .AddRate("table.datashard.erase.bytes", 120004)
                                        .AddRate("table.datashard.erase.rows", 110004)
                                        .AddRate("table.datashard.read.bytes", 70190008)
                                        .AddRate("table.datashard.read.rows", 30150008)
                                        .AddGauge("table.datashard.row_count", 10004)
                                        .AddRate("table.datashard.scan.bytes", 160004)
                                        .AddRate("table.datashard.scan.rows", 150004)
                                        .AddGauge("table.datashard.size_bytes", 20004)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60004)
                                        .AddRate("table.datashard.write.rows", 50004)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280006)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260006)
                                        .AddRate("table.datashard.cache_hit.bytes", 60006)
                                        .AddRate("table.datashard.cache_miss.bytes", 80006)
                                        .AddRate("table.datashard.consumed_cpu_us", 500006)
                                        .AddRate("table.datashard.erase.bytes", 240006)
                                        .AddRate("table.datashard.erase.rows", 220006)
                                        .AddRate("table.datashard.read.bytes", 140380012)
                                        .AddRate("table.datashard.read.rows", 60300012)
                                        .AddGauge("table.datashard.row_count", 20006)
                                        .AddRate("table.datashard.scan.bytes", 320006)
                                        .AddRate("table.datashard.scan.rows", 300006)
                                        .AddGauge("table.datashard.size_bytes", 40006)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {30, 1},
                                                {40, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 120006)
                                        .AddRate("table.datashard.write.rows", 100006)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420007)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390007)
                                    .AddRate("table.datashard.cache_hit.bytes", 90007)
                                    .AddRate("table.datashard.cache_miss.bytes", 120007)
                                    .AddRate("table.datashard.consumed_cpu_us", 600007)
                                    .AddRate("table.datashard.erase.bytes", 360007)
                                    .AddRate("table.datashard.erase.rows", 330007)
                                    .AddRate("table.datashard.read.bytes", 210570014)
                                    .AddRate("table.datashard.read.rows", 90450014)
                                    .AddGauge("table.datashard.row_count", 30007)
                                    .AddRate("table.datashard.scan.bytes", 480007)
                                    .AddRate("table.datashard.scan.rows", 450007)
                                    .AddGauge("table.datashard.size_bytes", 60007)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {30, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180007)
                                    .AddRate("table.datashard.write.rows", 150007)
                                .EndNested()
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                        .AddRate("table.datashard.cache_hit.bytes", 30016)
                                        .AddRate("table.datashard.cache_miss.bytes", 40016)
                                        .AddRate("table.datashard.consumed_cpu_us", 400016)
                                        .AddRate("table.datashard.erase.bytes", 120016)
                                        .AddRate("table.datashard.erase.rows", 110016)
                                        .AddRate("table.datashard.read.bytes", 70190032)
                                        .AddRate("table.datashard.read.rows", 30150032)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 160016)
                                        .AddRate("table.datashard.scan.rows", 150016)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                        .AddRate("table.datashard.write.bytes", 60016)
                                        .AddRate("table.datashard.write.rows", 50016)
                                    .EndNested()
                                    .StartNested("follower_id", "10202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 500032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "20202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                        .AddRate("table.datashard.cache_hit.bytes", 30064)
                                        .AddRate("table.datashard.cache_miss.bytes", 40064)
                                        .AddRate("table.datashard.consumed_cpu_us", 600064)
                                        .AddRate("table.datashard.erase.bytes", 120064)
                                        .AddRate("table.datashard.erase.rows", 110064)
                                        .AddRate("table.datashard.read.bytes", 70190128)
                                        .AddRate("table.datashard.read.rows", 30150128)
                                        .AddGauge("table.datashard.row_count", 10064)
                                        .AddRate("table.datashard.scan.bytes", 160064)
                                        .AddRate("table.datashard.scan.rows", 150064)
                                        .AddGauge("table.datashard.size_bytes", 20064)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                        .AddRate("table.datashard.write.bytes", 60064)
                                        .AddRate("table.datashard.write.rows", 50064)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280096)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260096)
                                        .AddRate("table.datashard.cache_hit.bytes", 60096)
                                        .AddRate("table.datashard.cache_miss.bytes", 80096)
                                        .AddRate("table.datashard.consumed_cpu_us", 1100096)
                                        .AddRate("table.datashard.erase.bytes", 240096)
                                        .AddRate("table.datashard.erase.rows", 220096)
                                        .AddRate("table.datashard.read.bytes", 140380192)
                                        .AddRate("table.datashard.read.rows", 60300192)
                                        .AddGauge("table.datashard.row_count", 20096)
                                        .AddRate("table.datashard.scan.bytes", 320096)
                                        .AddRate("table.datashard.scan.rows", 300096)
                                        .AddGauge("table.datashard.size_bytes", 40096)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {60, 1},
                                                {70, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 120096)
                                        .AddRate("table.datashard.write.rows", 100096)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420112)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390112)
                                    .AddRate("table.datashard.cache_hit.bytes", 90112)
                                    .AddRate("table.datashard.cache_miss.bytes", 120112)
                                    .AddRate("table.datashard.consumed_cpu_us", 1500112)
                                    .AddRate("table.datashard.erase.bytes", 360112)
                                    .AddRate("table.datashard.erase.rows", 330112)
                                    .AddRate("table.datashard.read.bytes", 210570224)
                                    .AddRate("table.datashard.read.rows", 90450224)
                                    .AddGauge("table.datashard.row_count", 30112)
                                    .AddRate("table.datashard.scan.bytes", 480112)
                                    .AddRate("table.datashard.scan.rows", 450112)
                                    .AddGauge("table.datashard.size_bytes", 60112)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {50, 1},
                                            {60, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180112)
                                    .AddRate("table.datashard.write.rows", 150112)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                        .AddRate("table.datashard.cache_hit.bytes", 30256)
                                        .AddRate("table.datashard.cache_miss.bytes", 40256)
                                        .AddRate("table.datashard.consumed_cpu_us", 700256)
                                        .AddRate("table.datashard.erase.bytes", 120256)
                                        .AddRate("table.datashard.erase.rows", 110256)
                                        .AddRate("table.datashard.read.bytes", 70190512)
                                        .AddRate("table.datashard.read.rows", 30150512)
                                        .AddGauge("table.datashard.row_count", 10256)
                                        .AddRate("table.datashard.scan.bytes", 160256)
                                        .AddRate("table.datashard.scan.rows", 150256)
                                        .AddGauge("table.datashard.size_bytes", 20256)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60256)
                                        .AddRate("table.datashard.write.rows", 50256)
                                    .EndNested()
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "20302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 141024)
                                        .AddRate("table.datashard.bulk_upsert.rows", 131024)
                                        .AddRate("table.datashard.cache_hit.bytes", 31024)
                                        .AddRate("table.datashard.cache_miss.bytes", 41024)
                                        .AddRate("table.datashard.consumed_cpu_us", 901024)
                                        .AddRate("table.datashard.erase.bytes", 121024)
                                        .AddRate("table.datashard.erase.rows", 111024)
                                        .AddRate("table.datashard.read.bytes", 70192048)
                                        .AddRate("table.datashard.read.rows", 30152048)
                                        .AddGauge("table.datashard.row_count", 11024)
                                        .AddRate("table.datashard.scan.bytes", 161024)
                                        .AddRate("table.datashard.scan.rows", 151024)
                                        .AddGauge("table.datashard.size_bytes", 21024)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 61024)
                                        .AddRate("table.datashard.write.rows", 51024)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 281536)
                                        .AddRate("table.datashard.bulk_upsert.rows", 261536)
                                        .AddRate("table.datashard.cache_hit.bytes", 61536)
                                        .AddRate("table.datashard.cache_miss.bytes", 81536)
                                        .AddRate("table.datashard.consumed_cpu_us", 1701536)
                                        .AddRate("table.datashard.erase.bytes", 241536)
                                        .AddRate("table.datashard.erase.rows", 221536)
                                        .AddRate("table.datashard.read.bytes", 140383072)
                                        .AddRate("table.datashard.read.rows", 60303072)
                                        .AddGauge("table.datashard.row_count", 21536)
                                        .AddRate("table.datashard.scan.bytes", 321536)
                                        .AddRate("table.datashard.scan.rows", 301536)
                                        .AddGauge("table.datashard.size_bytes", 41536)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {90, 1},
                                                {100, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 121536)
                                        .AddRate("table.datashard.write.rows", 101536)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 421792)
                                    .AddRate("table.datashard.bulk_upsert.rows", 391792)
                                    .AddRate("table.datashard.cache_hit.bytes", 91792)
                                    .AddRate("table.datashard.cache_miss.bytes", 121792)
                                    .AddRate("table.datashard.consumed_cpu_us", 2401792)
                                    .AddRate("table.datashard.erase.bytes", 361792)
                                    .AddRate("table.datashard.erase.rows", 331792)
                                    .AddRate("table.datashard.read.bytes", 210573584)
                                    .AddRate("table.datashard.read.rows", 90453584)
                                    .AddGauge("table.datashard.row_count", 31792)
                                    .AddRate("table.datashard.scan.bytes", 481792)
                                    .AddRate("table.datashard.scan.rows", 451792)
                                    .AddGauge("table.datashard.size_bytes", 61792)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {90, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 181792)
                                    .AddRate("table.datashard.write.rows", 151792)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 1261911)
                            .AddRate("table.datashard.bulk_upsert.rows", 1171911)
                            .AddRate("table.datashard.cache_hit.bytes", 271911)
                            .AddRate("table.datashard.cache_miss.bytes", 361911)
                            .AddRate("table.datashard.consumed_cpu_us", 4501911)
                            .AddRate("table.datashard.erase.bytes", 1081911)
                            .AddRate("table.datashard.erase.rows", 991911)
                            .AddRate("table.datashard.read.bytes", 631713822)
                            .AddRate("table.datashard.read.rows", 271353822)
                            .AddGauge("table.datashard.row_count", 91911)
                            .AddRate("table.datashard.scan.bytes", 1441911)
                            .AddRate("table.datashard.scan.rows", 1351911)
                            .AddGauge("table.datashard.size_bytes", 181911)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {30, 1},
                                    {40, 1},
                                    {50, 1},
                                    {60, 1},
                                    {70, 1},
                                    {80, 1},
                                    {90, 1},
                                    {100, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 541911)
                            .AddRate("table.datashard.write.rows", 451911)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("follower_id", "20101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 300004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280006)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260006)
                                    .AddRate("table.datashard.cache_hit.bytes", 60006)
                                    .AddRate("table.datashard.cache_miss.bytes", 80006)
                                    .AddRate("table.datashard.consumed_cpu_us", 500006)
                                    .AddRate("table.datashard.erase.bytes", 240006)
                                    .AddRate("table.datashard.erase.rows", 220006)
                                    .AddRate("table.datashard.read.bytes", 140380012)
                                    .AddRate("table.datashard.read.rows", 60300012)
                                    .AddGauge("table.datashard.row_count", 20006)
                                    .AddRate("table.datashard.scan.bytes", 320006)
                                    .AddRate("table.datashard.scan.rows", 300006)
                                    .AddGauge("table.datashard.size_bytes", 40006)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {30, 1},
                                            {40, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120006)
                                    .AddRate("table.datashard.write.rows", 100006)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 420007)
                                .AddRate("table.datashard.bulk_upsert.rows", 390007)
                                .AddRate("table.datashard.cache_hit.bytes", 90007)
                                .AddRate("table.datashard.cache_miss.bytes", 120007)
                                .AddRate("table.datashard.consumed_cpu_us", 600007)
                                .AddRate("table.datashard.erase.bytes", 360007)
                                .AddRate("table.datashard.erase.rows", 330007)
                                .AddRate("table.datashard.read.bytes", 210570014)
                                .AddRate("table.datashard.read.rows", 90450014)
                                .AddGauge("table.datashard.row_count", 30007)
                                .AddRate("table.datashard.scan.bytes", 480007)
                                .AddRate("table.datashard.scan.rows", 450007)
                                .AddGauge("table.datashard.size_bytes", 60007)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {20, 1},
                                        {30, 1},
                                        {40, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 180007)
                                .AddRate("table.datashard.write.rows", 150007)
                            .EndNested()
                            .StartNested("tablet_id", "201")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 400016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("follower_id", "10201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .StartNested("follower_id", "20201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140064)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130064)
                                    .AddRate("table.datashard.cache_hit.bytes", 30064)
                                    .AddRate("table.datashard.cache_miss.bytes", 40064)
                                    .AddRate("table.datashard.consumed_cpu_us", 600064)
                                    .AddRate("table.datashard.erase.bytes", 120064)
                                    .AddRate("table.datashard.erase.rows", 110064)
                                    .AddRate("table.datashard.read.bytes", 70190128)
                                    .AddRate("table.datashard.read.rows", 30150128)
                                    .AddGauge("table.datashard.row_count", 10064)
                                    .AddRate("table.datashard.scan.bytes", 160064)
                                    .AddRate("table.datashard.scan.rows", 150064)
                                    .AddGauge("table.datashard.size_bytes", 20064)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{70, 1}})
                                    .AddRate("table.datashard.write.bytes", 60064)
                                    .AddRate("table.datashard.write.rows", 50064)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280096)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260096)
                                    .AddRate("table.datashard.cache_hit.bytes", 60096)
                                    .AddRate("table.datashard.cache_miss.bytes", 80096)
                                    .AddRate("table.datashard.consumed_cpu_us", 1100096)
                                    .AddRate("table.datashard.erase.bytes", 240096)
                                    .AddRate("table.datashard.erase.rows", 220096)
                                    .AddRate("table.datashard.read.bytes", 140380192)
                                    .AddRate("table.datashard.read.rows", 60300192)
                                    .AddGauge("table.datashard.row_count", 20096)
                                    .AddRate("table.datashard.scan.bytes", 320096)
                                    .AddRate("table.datashard.scan.rows", 300096)
                                    .AddGauge("table.datashard.size_bytes", 40096)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {60, 1},
                                            {70, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120096)
                                    .AddRate("table.datashard.write.rows", 100096)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 420112)
                                .AddRate("table.datashard.bulk_upsert.rows", 390112)
                                .AddRate("table.datashard.cache_hit.bytes", 90112)
                                .AddRate("table.datashard.cache_miss.bytes", 120112)
                                .AddRate("table.datashard.consumed_cpu_us", 1500112)
                                .AddRate("table.datashard.erase.bytes", 360112)
                                .AddRate("table.datashard.erase.rows", 330112)
                                .AddRate("table.datashard.read.bytes", 210570224)
                                .AddRate("table.datashard.read.rows", 90450224)
                                .AddGauge("table.datashard.row_count", 30112)
                                .AddRate("table.datashard.scan.bytes", 480112)
                                .AddRate("table.datashard.scan.rows", 450112)
                                .AddGauge("table.datashard.size_bytes", 60112)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {50, 1},
                                        {60, 1},
                                        {70, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 180112)
                                .AddRate("table.datashard.write.rows", 150112)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                    .AddRate("table.datashard.cache_hit.bytes", 30256)
                                    .AddRate("table.datashard.cache_miss.bytes", 40256)
                                    .AddRate("table.datashard.consumed_cpu_us", 700256)
                                    .AddRate("table.datashard.erase.bytes", 120256)
                                    .AddRate("table.datashard.erase.rows", 110256)
                                    .AddRate("table.datashard.read.bytes", 70190512)
                                    .AddRate("table.datashard.read.rows", 30150512)
                                    .AddGauge("table.datashard.row_count", 10256)
                                    .AddRate("table.datashard.scan.bytes", 160256)
                                    .AddRate("table.datashard.scan.rows", 150256)
                                    .AddGauge("table.datashard.size_bytes", 20256)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 60256)
                                    .AddRate("table.datashard.write.rows", 50256)
                                .EndNested()
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .StartNested("follower_id", "20301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 141024)
                                    .AddRate("table.datashard.bulk_upsert.rows", 131024)
                                    .AddRate("table.datashard.cache_hit.bytes", 31024)
                                    .AddRate("table.datashard.cache_miss.bytes", 41024)
                                    .AddRate("table.datashard.consumed_cpu_us", 901024)
                                    .AddRate("table.datashard.erase.bytes", 121024)
                                    .AddRate("table.datashard.erase.rows", 111024)
                                    .AddRate("table.datashard.read.bytes", 70192048)
                                    .AddRate("table.datashard.read.rows", 30152048)
                                    .AddGauge("table.datashard.row_count", 11024)
                                    .AddRate("table.datashard.scan.bytes", 161024)
                                    .AddRate("table.datashard.scan.rows", 151024)
                                    .AddGauge("table.datashard.size_bytes", 21024)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                    .AddRate("table.datashard.write.bytes", 61024)
                                    .AddRate("table.datashard.write.rows", 51024)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 281536)
                                    .AddRate("table.datashard.bulk_upsert.rows", 261536)
                                    .AddRate("table.datashard.cache_hit.bytes", 61536)
                                    .AddRate("table.datashard.cache_miss.bytes", 81536)
                                    .AddRate("table.datashard.consumed_cpu_us", 1701536)
                                    .AddRate("table.datashard.erase.bytes", 241536)
                                    .AddRate("table.datashard.erase.rows", 221536)
                                    .AddRate("table.datashard.read.bytes", 140383072)
                                    .AddRate("table.datashard.read.rows", 60303072)
                                    .AddGauge("table.datashard.row_count", 21536)
                                    .AddRate("table.datashard.scan.bytes", 321536)
                                    .AddRate("table.datashard.scan.rows", 301536)
                                    .AddGauge("table.datashard.size_bytes", 41536)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {90, 1},
                                            {100, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 121536)
                                    .AddRate("table.datashard.write.rows", 101536)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 421792)
                                .AddRate("table.datashard.bulk_upsert.rows", 391792)
                                .AddRate("table.datashard.cache_hit.bytes", 91792)
                                .AddRate("table.datashard.cache_miss.bytes", 121792)
                                .AddRate("table.datashard.consumed_cpu_us", 2401792)
                                .AddRate("table.datashard.erase.bytes", 361792)
                                .AddRate("table.datashard.erase.rows", 331792)
                                .AddRate("table.datashard.read.bytes", 210573584)
                                .AddRate("table.datashard.read.rows", 90453584)
                                .AddGauge("table.datashard.row_count", 31792)
                                .AddRate("table.datashard.scan.bytes", 481792)
                                .AddRate("table.datashard.scan.rows", 451792)
                                .AddGauge("table.datashard.size_bytes", 61792)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {80, 1},
                                        {90, 1},
                                        {100, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 181792)
                                .AddRate("table.datashard.write.rows", 151792)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 1261911)
                        .AddRate("table.datashard.bulk_upsert.rows", 1171911)
                        .AddRate("table.datashard.cache_hit.bytes", 271911)
                        .AddRate("table.datashard.cache_miss.bytes", 361911)
                        .AddRate("table.datashard.consumed_cpu_us", 4501911)
                        .AddRate("table.datashard.erase.bytes", 1081911)
                        .AddRate("table.datashard.erase.rows", 991911)
                        .AddRate("table.datashard.read.bytes", 631713822)
                        .AddRate("table.datashard.read.rows", 271353822)
                        .AddGauge("table.datashard.row_count", 91911)
                        .AddRate("table.datashard.scan.bytes", 1441911)
                        .AddRate("table.datashard.scan.rows", 1351911)
                        .AddGauge("table.datashard.size_bytes", 181911)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {20, 1},
                                {30, 1},
                                {40, 1},
                                {50, 1},
                                {60, 1},
                                {70, 1},
                                {80, 1},
                                {90, 1},
                                {100, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 541911)
                        .AddRate("table.datashard.write.rows", 451911)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 2: Update metrics for the second follower for each partition for each table
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 20101, 0, &tableMetricsConfig1, 0x0008);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 201, 20201, 0, &tableMetricsConfig1, 0x0080);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 301, 20301, 0, &tableMetricsConfig1, 0x0800);

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 102, 20102, 0, &tableMetricsConfig2, 0x0008);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 20202, 0, &tableMetricsConfig2, 0x0080);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 302, 20302, 0, &tableMetricsConfig2, 0x0800);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after the update):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                        .AddRate("table.datashard.cache_hit.bytes", 30001)
                                        .AddRate("table.datashard.cache_miss.bytes", 40001)
                                        .AddRate("table.datashard.consumed_cpu_us", 100001)
                                        .AddRate("table.datashard.erase.bytes", 120001)
                                        .AddRate("table.datashard.erase.rows", 110001)
                                        .AddRate("table.datashard.read.bytes", 70190002)
                                        .AddRate("table.datashard.read.rows", 30150002)
                                        .AddGauge("table.datashard.row_count", 10001)
                                        .AddRate("table.datashard.scan.bytes", 160001)
                                        .AddRate("table.datashard.scan.rows", 150001)
                                        .AddGauge("table.datashard.size_bytes", 20001)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60001)
                                        .AddRate("table.datashard.write.rows", 50001)
                                    .EndNested()
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "20102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280012)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260012)
                                        .AddRate("table.datashard.cache_hit.bytes", 60012)
                                        .AddRate("table.datashard.cache_miss.bytes", 80012)
                                        .AddRate("table.datashard.consumed_cpu_us", 300012)
                                        .AddRate("table.datashard.erase.bytes", 240012)
                                        .AddRate("table.datashard.erase.rows", 220012)
                                        .AddRate("table.datashard.read.bytes", 140380024)
                                        .AddRate("table.datashard.read.rows", 60300024)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320012)
                                        .AddRate("table.datashard.scan.rows", 300012)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 120012)
                                        .AddRate("table.datashard.write.rows", 100012)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 420014)
                                        .AddRate("table.datashard.bulk_upsert.rows", 390014)
                                        .AddRate("table.datashard.cache_hit.bytes", 90014)
                                        .AddRate("table.datashard.cache_miss.bytes", 120014)
                                        .AddRate("table.datashard.consumed_cpu_us", 500014)
                                        .AddRate("table.datashard.erase.bytes", 360014)
                                        .AddRate("table.datashard.erase.rows", 330014)
                                        .AddRate("table.datashard.read.bytes", 210570028)
                                        .AddRate("table.datashard.read.rows", 90450028)
                                        .AddGauge("table.datashard.row_count", 20010)
                                        .AddRate("table.datashard.scan.bytes", 480014)
                                        .AddRate("table.datashard.scan.rows", 450014)
                                        .AddGauge("table.datashard.size_bytes", 40010)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {10, 1},
                                                {30, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 180014)
                                        .AddRate("table.datashard.write.rows", 150014)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 560015)
                                    .AddRate("table.datashard.bulk_upsert.rows", 520015)
                                    .AddRate("table.datashard.cache_hit.bytes", 120015)
                                    .AddRate("table.datashard.cache_miss.bytes", 160015)
                                    .AddRate("table.datashard.consumed_cpu_us", 600015)
                                    .AddRate("table.datashard.erase.bytes", 480015)
                                    .AddRate("table.datashard.erase.rows", 440015)
                                    .AddRate("table.datashard.read.bytes", 280760030)
                                    .AddRate("table.datashard.read.rows", 120600030)
                                    .AddGauge("table.datashard.row_count", 30011)
                                    .AddRate("table.datashard.scan.bytes", 640015)
                                    .AddRate("table.datashard.scan.rows", 600015)
                                    .AddGauge("table.datashard.size_bytes", 60011)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {20, 1},
                                            {30, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 240015)
                                    .AddRate("table.datashard.write.rows", 200015)
                                .EndNested()
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                        .AddRate("table.datashard.cache_hit.bytes", 30016)
                                        .AddRate("table.datashard.cache_miss.bytes", 40016)
                                        .AddRate("table.datashard.consumed_cpu_us", 400016)
                                        .AddRate("table.datashard.erase.bytes", 120016)
                                        .AddRate("table.datashard.erase.rows", 110016)
                                        .AddRate("table.datashard.read.bytes", 70190032)
                                        .AddRate("table.datashard.read.rows", 30150032)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 160016)
                                        .AddRate("table.datashard.scan.rows", 150016)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                        .AddRate("table.datashard.write.bytes", 60016)
                                        .AddRate("table.datashard.write.rows", 50016)
                                    .EndNested()
                                    .StartNested("follower_id", "10202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 500032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "20202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280192)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260192)
                                        .AddRate("table.datashard.cache_hit.bytes", 60192)
                                        .AddRate("table.datashard.cache_miss.bytes", 80192)
                                        .AddRate("table.datashard.consumed_cpu_us", 600192)
                                        .AddRate("table.datashard.erase.bytes", 240192)
                                        .AddRate("table.datashard.erase.rows", 220192)
                                        .AddRate("table.datashard.read.bytes", 140380384)
                                        .AddRate("table.datashard.read.rows", 60300384)
                                        .AddGauge("table.datashard.row_count", 10128)
                                        .AddRate("table.datashard.scan.bytes", 320192)
                                        .AddRate("table.datashard.scan.rows", 300192)
                                        .AddGauge("table.datashard.size_bytes", 20128)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 120192)
                                        .AddRate("table.datashard.write.rows", 100192)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 420224)
                                        .AddRate("table.datashard.bulk_upsert.rows", 390224)
                                        .AddRate("table.datashard.cache_hit.bytes", 90224)
                                        .AddRate("table.datashard.cache_miss.bytes", 120224)
                                        .AddRate("table.datashard.consumed_cpu_us", 1100224)
                                        .AddRate("table.datashard.erase.bytes", 360224)
                                        .AddRate("table.datashard.erase.rows", 330224)
                                        .AddRate("table.datashard.read.bytes", 210570448)
                                        .AddRate("table.datashard.read.rows", 90450448)
                                        .AddGauge("table.datashard.row_count", 20160)
                                        .AddRate("table.datashard.scan.bytes", 480224)
                                        .AddRate("table.datashard.scan.rows", 450224)
                                        .AddGauge("table.datashard.size_bytes", 40160)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {10, 1},
                                                {60, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 180224)
                                        .AddRate("table.datashard.write.rows", 150224)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 560240)
                                    .AddRate("table.datashard.bulk_upsert.rows", 520240)
                                    .AddRate("table.datashard.cache_hit.bytes", 120240)
                                    .AddRate("table.datashard.cache_miss.bytes", 160240)
                                    .AddRate("table.datashard.consumed_cpu_us", 1500240)
                                    .AddRate("table.datashard.erase.bytes", 480240)
                                    .AddRate("table.datashard.erase.rows", 440240)
                                    .AddRate("table.datashard.read.bytes", 280760480)
                                    .AddRate("table.datashard.read.rows", 120600480)
                                    .AddGauge("table.datashard.row_count", 30176)
                                    .AddRate("table.datashard.scan.bytes", 640240)
                                    .AddRate("table.datashard.scan.rows", 600240)
                                    .AddGauge("table.datashard.size_bytes", 60176)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {50, 1},
                                            {60, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 240240)
                                    .AddRate("table.datashard.write.rows", 200240)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                        .AddRate("table.datashard.cache_hit.bytes", 30256)
                                        .AddRate("table.datashard.cache_miss.bytes", 40256)
                                        .AddRate("table.datashard.consumed_cpu_us", 700256)
                                        .AddRate("table.datashard.erase.bytes", 120256)
                                        .AddRate("table.datashard.erase.rows", 110256)
                                        .AddRate("table.datashard.read.bytes", 70190512)
                                        .AddRate("table.datashard.read.rows", 30150512)
                                        .AddGauge("table.datashard.row_count", 10256)
                                        .AddRate("table.datashard.scan.bytes", 160256)
                                        .AddRate("table.datashard.scan.rows", 150256)
                                        .AddGauge("table.datashard.size_bytes", 20256)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60256)
                                        .AddRate("table.datashard.write.rows", 50256)
                                    .EndNested()
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "20302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 283072)
                                        .AddRate("table.datashard.bulk_upsert.rows", 263072)
                                        .AddRate("table.datashard.cache_hit.bytes", 63072)
                                        .AddRate("table.datashard.cache_miss.bytes", 83072)
                                        .AddRate("table.datashard.consumed_cpu_us", 903072)
                                        .AddRate("table.datashard.erase.bytes", 243072)
                                        .AddRate("table.datashard.erase.rows", 223072)
                                        .AddRate("table.datashard.read.bytes", 140386144)
                                        .AddRate("table.datashard.read.rows", 60306144)
                                        .AddGauge("table.datashard.row_count", 12048)
                                        .AddRate("table.datashard.scan.bytes", 323072)
                                        .AddRate("table.datashard.scan.rows", 303072)
                                        .AddGauge("table.datashard.size_bytes", 22048)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                        .AddRate("table.datashard.write.bytes", 123072)
                                        .AddRate("table.datashard.write.rows", 103072)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 423584)
                                        .AddRate("table.datashard.bulk_upsert.rows", 393584)
                                        .AddRate("table.datashard.cache_hit.bytes", 93584)
                                        .AddRate("table.datashard.cache_miss.bytes", 123584)
                                        .AddRate("table.datashard.consumed_cpu_us", 1703584)
                                        .AddRate("table.datashard.erase.bytes", 363584)
                                        .AddRate("table.datashard.erase.rows", 333584)
                                        .AddRate("table.datashard.read.bytes", 210577168)
                                        .AddRate("table.datashard.read.rows", 90457168)
                                        .AddGauge("table.datashard.row_count", 22560)
                                        .AddRate("table.datashard.scan.bytes", 483584)
                                        .AddRate("table.datashard.scan.rows", 453584)
                                        .AddGauge("table.datashard.size_bytes", 42560)
                                        .AddCpuHistogram(
                                            "table.datashard.used_core_percents",
                                            {
                                                {10, 1},
                                                {90, 1},
                                            }
                                        )
                                        .AddRate("table.datashard.write.bytes", 183584)
                                        .AddRate("table.datashard.write.rows", 153584)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 563840)
                                    .AddRate("table.datashard.bulk_upsert.rows", 523840)
                                    .AddRate("table.datashard.cache_hit.bytes", 123840)
                                    .AddRate("table.datashard.cache_miss.bytes", 163840)
                                    .AddRate("table.datashard.consumed_cpu_us", 2403840)
                                    .AddRate("table.datashard.erase.bytes", 483840)
                                    .AddRate("table.datashard.erase.rows", 443840)
                                    .AddRate("table.datashard.read.bytes", 280767680)
                                    .AddRate("table.datashard.read.rows", 120607680)
                                    .AddGauge("table.datashard.row_count", 32816)
                                    .AddRate("table.datashard.scan.bytes", 643840)
                                    .AddRate("table.datashard.scan.rows", 603840)
                                    .AddGauge("table.datashard.size_bytes", 62816)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {80, 1},
                                            {90, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 243840)
                                    .AddRate("table.datashard.write.rows", 203840)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 1684095)
                            .AddRate("table.datashard.bulk_upsert.rows", 1564095)
                            .AddRate("table.datashard.cache_hit.bytes", 364095)
                            .AddRate("table.datashard.cache_miss.bytes", 484095)
                            .AddRate("table.datashard.consumed_cpu_us", 4504095)
                            .AddRate("table.datashard.erase.bytes", 1444095)
                            .AddRate("table.datashard.erase.rows", 1324095)
                            .AddRate("table.datashard.read.bytes", 842288190)
                            .AddRate("table.datashard.read.rows", 361808190)
                            .AddGauge("table.datashard.row_count", 93003)
                            .AddRate("table.datashard.scan.bytes", 1924095)
                            .AddRate("table.datashard.scan.rows", 1804095)
                            .AddGauge("table.datashard.size_bytes", 183003)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {10, 3},
                                    {20, 1},
                                    {30, 1},
                                    {50, 1},
                                    {60, 1},
                                    {80, 1},
                                    {90, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 724095)
                            .AddRate("table.datashard.write.rows", 604095)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("follower_id", "20101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280012)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260012)
                                    .AddRate("table.datashard.cache_hit.bytes", 60012)
                                    .AddRate("table.datashard.cache_miss.bytes", 80012)
                                    .AddRate("table.datashard.consumed_cpu_us", 300012)
                                    .AddRate("table.datashard.erase.bytes", 240012)
                                    .AddRate("table.datashard.erase.rows", 220012)
                                    .AddRate("table.datashard.read.bytes", 140380024)
                                    .AddRate("table.datashard.read.rows", 60300024)
                                    .AddGauge("table.datashard.row_count", 10008)
                                    .AddRate("table.datashard.scan.bytes", 320012)
                                    .AddRate("table.datashard.scan.rows", 300012)
                                    .AddGauge("table.datashard.size_bytes", 20008)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                    .AddRate("table.datashard.write.bytes", 120012)
                                    .AddRate("table.datashard.write.rows", 100012)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420014)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390014)
                                    .AddRate("table.datashard.cache_hit.bytes", 90014)
                                    .AddRate("table.datashard.cache_miss.bytes", 120014)
                                    .AddRate("table.datashard.consumed_cpu_us", 500014)
                                    .AddRate("table.datashard.erase.bytes", 360014)
                                    .AddRate("table.datashard.erase.rows", 330014)
                                    .AddRate("table.datashard.read.bytes", 210570028)
                                    .AddRate("table.datashard.read.rows", 90450028)
                                    .AddGauge("table.datashard.row_count", 20010)
                                    .AddRate("table.datashard.scan.bytes", 480014)
                                    .AddRate("table.datashard.scan.rows", 450014)
                                    .AddGauge("table.datashard.size_bytes", 40010)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {30, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180014)
                                    .AddRate("table.datashard.write.rows", 150014)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 560015)
                                .AddRate("table.datashard.bulk_upsert.rows", 520015)
                                .AddRate("table.datashard.cache_hit.bytes", 120015)
                                .AddRate("table.datashard.cache_miss.bytes", 160015)
                                .AddRate("table.datashard.consumed_cpu_us", 600015)
                                .AddRate("table.datashard.erase.bytes", 480015)
                                .AddRate("table.datashard.erase.rows", 440015)
                                .AddRate("table.datashard.read.bytes", 280760030)
                                .AddRate("table.datashard.read.rows", 120600030)
                                .AddGauge("table.datashard.row_count", 30011)
                                .AddRate("table.datashard.scan.bytes", 640015)
                                .AddRate("table.datashard.scan.rows", 600015)
                                .AddGauge("table.datashard.size_bytes", 60011)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {10, 1},
                                        {20, 1},
                                        {30, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 240015)
                                .AddRate("table.datashard.write.rows", 200015)
                            .EndNested()
                            .StartNested("tablet_id", "201")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 400016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("follower_id", "10201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .StartNested("follower_id", "20201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280192)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260192)
                                    .AddRate("table.datashard.cache_hit.bytes", 60192)
                                    .AddRate("table.datashard.cache_miss.bytes", 80192)
                                    .AddRate("table.datashard.consumed_cpu_us", 600192)
                                    .AddRate("table.datashard.erase.bytes", 240192)
                                    .AddRate("table.datashard.erase.rows", 220192)
                                    .AddRate("table.datashard.read.bytes", 140380384)
                                    .AddRate("table.datashard.read.rows", 60300384)
                                    .AddGauge("table.datashard.row_count", 10128)
                                    .AddRate("table.datashard.scan.bytes", 320192)
                                    .AddRate("table.datashard.scan.rows", 300192)
                                    .AddGauge("table.datashard.size_bytes", 20128)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                    .AddRate("table.datashard.write.bytes", 120192)
                                    .AddRate("table.datashard.write.rows", 100192)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 420224)
                                    .AddRate("table.datashard.bulk_upsert.rows", 390224)
                                    .AddRate("table.datashard.cache_hit.bytes", 90224)
                                    .AddRate("table.datashard.cache_miss.bytes", 120224)
                                    .AddRate("table.datashard.consumed_cpu_us", 1100224)
                                    .AddRate("table.datashard.erase.bytes", 360224)
                                    .AddRate("table.datashard.erase.rows", 330224)
                                    .AddRate("table.datashard.read.bytes", 210570448)
                                    .AddRate("table.datashard.read.rows", 90450448)
                                    .AddGauge("table.datashard.row_count", 20160)
                                    .AddRate("table.datashard.scan.bytes", 480224)
                                    .AddRate("table.datashard.scan.rows", 450224)
                                    .AddGauge("table.datashard.size_bytes", 40160)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {60, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 180224)
                                    .AddRate("table.datashard.write.rows", 150224)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 560240)
                                .AddRate("table.datashard.bulk_upsert.rows", 520240)
                                .AddRate("table.datashard.cache_hit.bytes", 120240)
                                .AddRate("table.datashard.cache_miss.bytes", 160240)
                                .AddRate("table.datashard.consumed_cpu_us", 1500240)
                                .AddRate("table.datashard.erase.bytes", 480240)
                                .AddRate("table.datashard.erase.rows", 440240)
                                .AddRate("table.datashard.read.bytes", 280760480)
                                .AddRate("table.datashard.read.rows", 120600480)
                                .AddGauge("table.datashard.row_count", 30176)
                                .AddRate("table.datashard.scan.bytes", 640240)
                                .AddRate("table.datashard.scan.rows", 600240)
                                .AddGauge("table.datashard.size_bytes", 60176)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {10, 1},
                                        {50, 1},
                                        {60, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 240240)
                                .AddRate("table.datashard.write.rows", 200240)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                    .AddRate("table.datashard.cache_hit.bytes", 30256)
                                    .AddRate("table.datashard.cache_miss.bytes", 40256)
                                    .AddRate("table.datashard.consumed_cpu_us", 700256)
                                    .AddRate("table.datashard.erase.bytes", 120256)
                                    .AddRate("table.datashard.erase.rows", 110256)
                                    .AddRate("table.datashard.read.bytes", 70190512)
                                    .AddRate("table.datashard.read.rows", 30150512)
                                    .AddGauge("table.datashard.row_count", 10256)
                                    .AddRate("table.datashard.scan.bytes", 160256)
                                    .AddRate("table.datashard.scan.rows", 150256)
                                    .AddGauge("table.datashard.size_bytes", 20256)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 60256)
                                    .AddRate("table.datashard.write.rows", 50256)
                                .EndNested()
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .StartNested("follower_id", "20301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 283072)
                                    .AddRate("table.datashard.bulk_upsert.rows", 263072)
                                    .AddRate("table.datashard.cache_hit.bytes", 63072)
                                    .AddRate("table.datashard.cache_miss.bytes", 83072)
                                    .AddRate("table.datashard.consumed_cpu_us", 903072)
                                    .AddRate("table.datashard.erase.bytes", 243072)
                                    .AddRate("table.datashard.erase.rows", 223072)
                                    .AddRate("table.datashard.read.bytes", 140386144)
                                    .AddRate("table.datashard.read.rows", 60306144)
                                    .AddGauge("table.datashard.row_count", 12048)
                                    .AddRate("table.datashard.scan.bytes", 323072)
                                    .AddRate("table.datashard.scan.rows", 303072)
                                    .AddGauge("table.datashard.size_bytes", 22048)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{10, 1}})
                                    .AddRate("table.datashard.write.bytes", 123072)
                                    .AddRate("table.datashard.write.rows", 103072)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 423584)
                                    .AddRate("table.datashard.bulk_upsert.rows", 393584)
                                    .AddRate("table.datashard.cache_hit.bytes", 93584)
                                    .AddRate("table.datashard.cache_miss.bytes", 123584)
                                    .AddRate("table.datashard.consumed_cpu_us", 1703584)
                                    .AddRate("table.datashard.erase.bytes", 363584)
                                    .AddRate("table.datashard.erase.rows", 333584)
                                    .AddRate("table.datashard.read.bytes", 210577168)
                                    .AddRate("table.datashard.read.rows", 90457168)
                                    .AddGauge("table.datashard.row_count", 22560)
                                    .AddRate("table.datashard.scan.bytes", 483584)
                                    .AddRate("table.datashard.scan.rows", 453584)
                                    .AddGauge("table.datashard.size_bytes", 42560)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {10, 1},
                                            {90, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 183584)
                                    .AddRate("table.datashard.write.rows", 153584)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 563840)
                                .AddRate("table.datashard.bulk_upsert.rows", 523840)
                                .AddRate("table.datashard.cache_hit.bytes", 123840)
                                .AddRate("table.datashard.cache_miss.bytes", 163840)
                                .AddRate("table.datashard.consumed_cpu_us", 2403840)
                                .AddRate("table.datashard.erase.bytes", 483840)
                                .AddRate("table.datashard.erase.rows", 443840)
                                .AddRate("table.datashard.read.bytes", 280767680)
                                .AddRate("table.datashard.read.rows", 120607680)
                                .AddGauge("table.datashard.row_count", 32816)
                                .AddRate("table.datashard.scan.bytes", 643840)
                                .AddRate("table.datashard.scan.rows", 603840)
                                .AddGauge("table.datashard.size_bytes", 62816)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {10, 1},
                                        {80, 1},
                                        {90, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 243840)
                                .AddRate("table.datashard.write.rows", 203840)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 1684095)
                        .AddRate("table.datashard.bulk_upsert.rows", 1564095)
                        .AddRate("table.datashard.cache_hit.bytes", 364095)
                        .AddRate("table.datashard.cache_miss.bytes", 484095)
                        .AddRate("table.datashard.consumed_cpu_us", 4504095)
                        .AddRate("table.datashard.erase.bytes", 1444095)
                        .AddRate("table.datashard.erase.rows", 1324095)
                        .AddRate("table.datashard.read.bytes", 842288190)
                        .AddRate("table.datashard.read.rows", 361808190)
                        .AddGauge("table.datashard.row_count", 93003)
                        .AddRate("table.datashard.scan.bytes", 1924095)
                        .AddRate("table.datashard.scan.rows", 1804095)
                        .AddGauge("table.datashard.size_bytes", 183003)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {10, 3},
                                {20, 1},
                                {30, 1},
                                {50, 1},
                                {60, 1},
                                {80, 1},
                                {90, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 724095)
                        .AddRate("table.datashard.write.rows", 604095)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

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

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting followers):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                        .AddRate("table.datashard.cache_hit.bytes", 30001)
                                        .AddRate("table.datashard.cache_miss.bytes", 40001)
                                        .AddRate("table.datashard.consumed_cpu_us", 100001)
                                        .AddRate("table.datashard.erase.bytes", 120001)
                                        .AddRate("table.datashard.erase.rows", 110001)
                                        .AddRate("table.datashard.read.bytes", 70190002)
                                        .AddRate("table.datashard.read.rows", 30150002)
                                        .AddGauge("table.datashard.row_count", 10001)
                                        .AddRate("table.datashard.scan.bytes", 160001)
                                        .AddRate("table.datashard.scan.rows", 150001)
                                        .AddGauge("table.datashard.size_bytes", 20001)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                        .AddRate("table.datashard.write.bytes", 60001)
                                        .AddRate("table.datashard.write.rows", 50001)
                                    .EndNested()
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280003)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260003)
                                    .AddRate("table.datashard.cache_hit.bytes", 60003)
                                    .AddRate("table.datashard.cache_miss.bytes", 80003)
                                    .AddRate("table.datashard.consumed_cpu_us", 300003)
                                    .AddRate("table.datashard.erase.bytes", 240003)
                                    .AddRate("table.datashard.erase.rows", 220003)
                                    .AddRate("table.datashard.read.bytes", 140380006)
                                    .AddRate("table.datashard.read.rows", 60300006)
                                    .AddGauge("table.datashard.row_count", 20003)
                                    .AddRate("table.datashard.scan.bytes", 320003)
                                    .AddRate("table.datashard.scan.rows", 300003)
                                    .AddGauge("table.datashard.size_bytes", 40003)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {20, 1},
                                            {30, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120003)
                                    .AddRate("table.datashard.write.rows", 100003)
                                .EndNested()
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                        .AddRate("table.datashard.cache_hit.bytes", 30016)
                                        .AddRate("table.datashard.cache_miss.bytes", 40016)
                                        .AddRate("table.datashard.consumed_cpu_us", 400016)
                                        .AddRate("table.datashard.erase.bytes", 120016)
                                        .AddRate("table.datashard.erase.rows", 110016)
                                        .AddRate("table.datashard.read.bytes", 70190032)
                                        .AddRate("table.datashard.read.rows", 30150032)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 160016)
                                        .AddRate("table.datashard.scan.rows", 150016)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                        .AddRate("table.datashard.write.bytes", 60016)
                                        .AddRate("table.datashard.write.rows", 50016)
                                    .EndNested()
                                    .StartNested("follower_id", "10202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 500032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 500032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280048)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260048)
                                    .AddRate("table.datashard.cache_hit.bytes", 60048)
                                    .AddRate("table.datashard.cache_miss.bytes", 80048)
                                    .AddRate("table.datashard.consumed_cpu_us", 900048)
                                    .AddRate("table.datashard.erase.bytes", 240048)
                                    .AddRate("table.datashard.erase.rows", 220048)
                                    .AddRate("table.datashard.read.bytes", 140380096)
                                    .AddRate("table.datashard.read.rows", 60300096)
                                    .AddGauge("table.datashard.row_count", 20048)
                                    .AddRate("table.datashard.scan.bytes", 320048)
                                    .AddRate("table.datashard.scan.rows", 300048)
                                    .AddGauge("table.datashard.size_bytes", 40048)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {50, 1},
                                            {60, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120048)
                                    .AddRate("table.datashard.write.rows", 100048)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                        .AddRate("table.datashard.cache_hit.bytes", 30256)
                                        .AddRate("table.datashard.cache_miss.bytes", 40256)
                                        .AddRate("table.datashard.consumed_cpu_us", 700256)
                                        .AddRate("table.datashard.erase.bytes", 120256)
                                        .AddRate("table.datashard.erase.rows", 110256)
                                        .AddRate("table.datashard.read.bytes", 70190512)
                                        .AddRate("table.datashard.read.rows", 30150512)
                                        .AddGauge("table.datashard.row_count", 10256)
                                        .AddRate("table.datashard.scan.bytes", 160256)
                                        .AddRate("table.datashard.scan.rows", 150256)
                                        .AddGauge("table.datashard.size_bytes", 20256)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 60256)
                                        .AddRate("table.datashard.write.rows", 50256)
                                    .EndNested()
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280768)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260768)
                                    .AddRate("table.datashard.cache_hit.bytes", 60768)
                                    .AddRate("table.datashard.cache_miss.bytes", 80768)
                                    .AddRate("table.datashard.consumed_cpu_us", 1500768)
                                    .AddRate("table.datashard.erase.bytes", 240768)
                                    .AddRate("table.datashard.erase.rows", 220768)
                                    .AddRate("table.datashard.read.bytes", 140381536)
                                    .AddRate("table.datashard.read.rows", 60301536)
                                    .AddGauge("table.datashard.row_count", 20768)
                                    .AddRate("table.datashard.scan.bytes", 320768)
                                    .AddRate("table.datashard.scan.rows", 300768)
                                    .AddGauge("table.datashard.size_bytes", 40768)
                                    .AddCpuHistogram(
                                        "table.datashard.used_core_percents",
                                        {
                                            {80, 1},
                                            {90, 1},
                                        }
                                    )
                                    .AddRate("table.datashard.write.bytes", 120768)
                                    .AddRate("table.datashard.write.rows", 100768)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 840819)
                            .AddRate("table.datashard.bulk_upsert.rows", 780819)
                            .AddRate("table.datashard.cache_hit.bytes", 180819)
                            .AddRate("table.datashard.cache_miss.bytes", 240819)
                            .AddRate("table.datashard.consumed_cpu_us", 2700819)
                            .AddRate("table.datashard.erase.bytes", 720819)
                            .AddRate("table.datashard.erase.rows", 660819)
                            .AddRate("table.datashard.read.bytes", 421141638)
                            .AddRate("table.datashard.read.rows", 180901638)
                            .AddGauge("table.datashard.row_count", 60819)
                            .AddRate("table.datashard.scan.bytes", 960819)
                            .AddRate("table.datashard.scan.rows", 900819)
                            .AddGauge("table.datashard.size_bytes", 120819)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {20, 1},
                                    {30, 1},
                                    {50, 1},
                                    {60, 1},
                                    {80, 1},
                                    {90, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 360819)
                            .AddRate("table.datashard.write.rows", 300819)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280003)
                                .AddRate("table.datashard.bulk_upsert.rows", 260003)
                                .AddRate("table.datashard.cache_hit.bytes", 60003)
                                .AddRate("table.datashard.cache_miss.bytes", 80003)
                                .AddRate("table.datashard.consumed_cpu_us", 300003)
                                .AddRate("table.datashard.erase.bytes", 240003)
                                .AddRate("table.datashard.erase.rows", 220003)
                                .AddRate("table.datashard.read.bytes", 140380006)
                                .AddRate("table.datashard.read.rows", 60300006)
                                .AddGauge("table.datashard.row_count", 20003)
                                .AddRate("table.datashard.scan.bytes", 320003)
                                .AddRate("table.datashard.scan.rows", 300003)
                                .AddGauge("table.datashard.size_bytes", 40003)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {20, 1},
                                        {30, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 120003)
                                .AddRate("table.datashard.write.rows", 100003)
                            .EndNested()
                            .StartNested("tablet_id", "201")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140016)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130016)
                                    .AddRate("table.datashard.cache_hit.bytes", 30016)
                                    .AddRate("table.datashard.cache_miss.bytes", 40016)
                                    .AddRate("table.datashard.consumed_cpu_us", 400016)
                                    .AddRate("table.datashard.erase.bytes", 120016)
                                    .AddRate("table.datashard.erase.rows", 110016)
                                    .AddRate("table.datashard.read.bytes", 70190032)
                                    .AddRate("table.datashard.read.rows", 30150032)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 160016)
                                    .AddRate("table.datashard.scan.rows", 150016)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{50, 1}})
                                    .AddRate("table.datashard.write.bytes", 60016)
                                    .AddRate("table.datashard.write.rows", 50016)
                                .EndNested()
                                .StartNested("follower_id", "10201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280048)
                                .AddRate("table.datashard.bulk_upsert.rows", 260048)
                                .AddRate("table.datashard.cache_hit.bytes", 60048)
                                .AddRate("table.datashard.cache_miss.bytes", 80048)
                                .AddRate("table.datashard.consumed_cpu_us", 900048)
                                .AddRate("table.datashard.erase.bytes", 240048)
                                .AddRate("table.datashard.erase.rows", 220048)
                                .AddRate("table.datashard.read.bytes", 140380096)
                                .AddRate("table.datashard.read.rows", 60300096)
                                .AddGauge("table.datashard.row_count", 20048)
                                .AddRate("table.datashard.scan.bytes", 320048)
                                .AddRate("table.datashard.scan.rows", 300048)
                                .AddGauge("table.datashard.size_bytes", 40048)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {50, 1},
                                        {60, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 120048)
                                .AddRate("table.datashard.write.rows", 100048)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140256)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130256)
                                    .AddRate("table.datashard.cache_hit.bytes", 30256)
                                    .AddRate("table.datashard.cache_miss.bytes", 40256)
                                    .AddRate("table.datashard.consumed_cpu_us", 700256)
                                    .AddRate("table.datashard.erase.bytes", 120256)
                                    .AddRate("table.datashard.erase.rows", 110256)
                                    .AddRate("table.datashard.read.bytes", 70190512)
                                    .AddRate("table.datashard.read.rows", 30150512)
                                    .AddGauge("table.datashard.row_count", 10256)
                                    .AddRate("table.datashard.scan.bytes", 160256)
                                    .AddRate("table.datashard.scan.rows", 150256)
                                    .AddGauge("table.datashard.size_bytes", 20256)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 60256)
                                    .AddRate("table.datashard.write.rows", 50256)
                                .EndNested()
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280768)
                                .AddRate("table.datashard.bulk_upsert.rows", 260768)
                                .AddRate("table.datashard.cache_hit.bytes", 60768)
                                .AddRate("table.datashard.cache_miss.bytes", 80768)
                                .AddRate("table.datashard.consumed_cpu_us", 1500768)
                                .AddRate("table.datashard.erase.bytes", 240768)
                                .AddRate("table.datashard.erase.rows", 220768)
                                .AddRate("table.datashard.read.bytes", 140381536)
                                .AddRate("table.datashard.read.rows", 60301536)
                                .AddGauge("table.datashard.row_count", 20768)
                                .AddRate("table.datashard.scan.bytes", 320768)
                                .AddRate("table.datashard.scan.rows", 300768)
                                .AddGauge("table.datashard.size_bytes", 40768)
                                .AddCpuHistogram(
                                    "table.datashard.used_core_percents",
                                    {
                                        {80, 1},
                                        {90, 1},
                                    }
                                )
                                .AddRate("table.datashard.write.bytes", 120768)
                                .AddRate("table.datashard.write.rows", 100768)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 840819)
                        .AddRate("table.datashard.bulk_upsert.rows", 780819)
                        .AddRate("table.datashard.cache_hit.bytes", 180819)
                        .AddRate("table.datashard.cache_miss.bytes", 240819)
                        .AddRate("table.datashard.consumed_cpu_us", 2700819)
                        .AddRate("table.datashard.erase.bytes", 720819)
                        .AddRate("table.datashard.erase.rows", 660819)
                        .AddRate("table.datashard.read.bytes", 421141638)
                        .AddRate("table.datashard.read.rows", 180901638)
                        .AddGauge("table.datashard.row_count", 60819)
                        .AddRate("table.datashard.scan.bytes", 960819)
                        .AddRate("table.datashard.scan.rows", 900819)
                        .AddGauge("table.datashard.size_bytes", 120819)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {20, 1},
                                {30, 1},
                                {50, 1},
                                {60, 1},
                                {80, 1},
                                {90, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 360819)
                        .AddRate("table.datashard.write.rows", 300819)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

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

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting leaders):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "10202")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 500032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                        .AddRate("table.datashard.cache_hit.bytes", 30032)
                                        .AddRate("table.datashard.cache_miss.bytes", 40032)
                                        .AddRate("table.datashard.consumed_cpu_us", 500032)
                                        .AddRate("table.datashard.erase.bytes", 120032)
                                        .AddRate("table.datashard.erase.rows", 110032)
                                        .AddRate("table.datashard.read.bytes", 70190064)
                                        .AddRate("table.datashard.read.rows", 30150064)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 160032)
                                        .AddRate("table.datashard.scan.rows", 150032)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                        .AddRate("table.datashard.write.bytes", 60032)
                                        .AddRate("table.datashard.write.rows", 50032)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 420546)
                            .AddRate("table.datashard.bulk_upsert.rows", 390546)
                            .AddRate("table.datashard.cache_hit.bytes", 90546)
                            .AddRate("table.datashard.cache_miss.bytes", 120546)
                            .AddRate("table.datashard.consumed_cpu_us", 1500546)
                            .AddRate("table.datashard.erase.bytes", 360546)
                            .AddRate("table.datashard.erase.rows", 330546)
                            .AddRate("table.datashard.read.bytes", 210571092)
                            .AddRate("table.datashard.read.rows", 90451092)
                            .AddGauge("table.datashard.row_count", 30546)
                            .AddRate("table.datashard.scan.bytes", 480546)
                            .AddRate("table.datashard.scan.rows", 450546)
                            .AddGauge("table.datashard.size_bytes", 60546)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {30, 1},
                                    {60, 1},
                                    {90, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 180546)
                            .AddRate("table.datashard.write.rows", 150546)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                .AddRate("table.datashard.cache_hit.bytes", 30002)
                                .AddRate("table.datashard.cache_miss.bytes", 40002)
                                .AddRate("table.datashard.consumed_cpu_us", 200002)
                                .AddRate("table.datashard.erase.bytes", 120002)
                                .AddRate("table.datashard.erase.rows", 110002)
                                .AddRate("table.datashard.read.bytes", 70190004)
                                .AddRate("table.datashard.read.rows", 30150004)
                                .AddGauge("table.datashard.row_count", 10002)
                                .AddRate("table.datashard.scan.bytes", 160002)
                                .AddRate("table.datashard.scan.rows", 150002)
                                .AddGauge("table.datashard.size_bytes", 20002)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                .AddRate("table.datashard.write.bytes", 60002)
                                .AddRate("table.datashard.write.rows", 50002)
                            .EndNested()
                            .StartNested("tablet_id", "201")
                                .StartNested("follower_id", "10201")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                    .AddRate("table.datashard.cache_hit.bytes", 30032)
                                    .AddRate("table.datashard.cache_miss.bytes", 40032)
                                    .AddRate("table.datashard.consumed_cpu_us", 500032)
                                    .AddRate("table.datashard.erase.bytes", 120032)
                                    .AddRate("table.datashard.erase.rows", 110032)
                                    .AddRate("table.datashard.read.bytes", 70190064)
                                    .AddRate("table.datashard.read.rows", 30150064)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 160032)
                                    .AddRate("table.datashard.scan.rows", 150032)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                    .AddRate("table.datashard.write.bytes", 60032)
                                    .AddRate("table.datashard.write.rows", 50032)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140032)
                                .AddRate("table.datashard.bulk_upsert.rows", 130032)
                                .AddRate("table.datashard.cache_hit.bytes", 30032)
                                .AddRate("table.datashard.cache_miss.bytes", 40032)
                                .AddRate("table.datashard.consumed_cpu_us", 500032)
                                .AddRate("table.datashard.erase.bytes", 120032)
                                .AddRate("table.datashard.erase.rows", 110032)
                                .AddRate("table.datashard.read.bytes", 70190064)
                                .AddRate("table.datashard.read.rows", 30150064)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 160032)
                                .AddRate("table.datashard.scan.rows", 150032)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{60, 1}})
                                .AddRate("table.datashard.write.bytes", 60032)
                                .AddRate("table.datashard.write.rows", 50032)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                .AddRate("table.datashard.cache_hit.bytes", 30512)
                                .AddRate("table.datashard.cache_miss.bytes", 40512)
                                .AddRate("table.datashard.consumed_cpu_us", 800512)
                                .AddRate("table.datashard.erase.bytes", 120512)
                                .AddRate("table.datashard.erase.rows", 110512)
                                .AddRate("table.datashard.read.bytes", 70191024)
                                .AddRate("table.datashard.read.rows", 30151024)
                                .AddGauge("table.datashard.row_count", 10512)
                                .AddRate("table.datashard.scan.bytes", 160512)
                                .AddRate("table.datashard.scan.rows", 150512)
                                .AddGauge("table.datashard.size_bytes", 20512)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 60512)
                                .AddRate("table.datashard.write.rows", 50512)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 420546)
                        .AddRate("table.datashard.bulk_upsert.rows", 390546)
                        .AddRate("table.datashard.cache_hit.bytes", 90546)
                        .AddRate("table.datashard.cache_miss.bytes", 120546)
                        .AddRate("table.datashard.consumed_cpu_us", 1500546)
                        .AddRate("table.datashard.erase.bytes", 360546)
                        .AddRate("table.datashard.erase.rows", 330546)
                        .AddRate("table.datashard.read.bytes", 210571092)
                        .AddRate("table.datashard.read.rows", 90451092)
                        .AddGauge("table.datashard.row_count", 30546)
                        .AddRate("table.datashard.scan.bytes", 480546)
                        .AddRate("table.datashard.scan.rows", 450546)
                        .AddGauge("table.datashard.size_bytes", 60546)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {30, 1},
                                {60, 1},
                                {90, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 180546)
                        .AddRate("table.datashard.write.rows", 150546)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 5: Remove metrics for the second partition in each table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 201, 10201);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 202, 10202);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting partitions):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280514)
                            .AddRate("table.datashard.bulk_upsert.rows", 260514)
                            .AddRate("table.datashard.cache_hit.bytes", 60514)
                            .AddRate("table.datashard.cache_miss.bytes", 80514)
                            .AddRate("table.datashard.consumed_cpu_us", 1000514)
                            .AddRate("table.datashard.erase.bytes", 240514)
                            .AddRate("table.datashard.erase.rows", 220514)
                            .AddRate("table.datashard.read.bytes", 140381028)
                            .AddRate("table.datashard.read.rows", 60301028)
                            .AddGauge("table.datashard.row_count", 20514)
                            .AddRate("table.datashard.scan.bytes", 320514)
                            .AddRate("table.datashard.scan.rows", 300514)
                            .AddGauge("table.datashard.size_bytes", 40514)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {30, 1},
                                    {90, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 120514)
                            .AddRate("table.datashard.write.rows", 100514)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "10101")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                .AddRate("table.datashard.cache_hit.bytes", 30002)
                                .AddRate("table.datashard.cache_miss.bytes", 40002)
                                .AddRate("table.datashard.consumed_cpu_us", 200002)
                                .AddRate("table.datashard.erase.bytes", 120002)
                                .AddRate("table.datashard.erase.rows", 110002)
                                .AddRate("table.datashard.read.bytes", 70190004)
                                .AddRate("table.datashard.read.rows", 30150004)
                                .AddGauge("table.datashard.row_count", 10002)
                                .AddRate("table.datashard.scan.bytes", 160002)
                                .AddRate("table.datashard.scan.rows", 150002)
                                .AddGauge("table.datashard.size_bytes", 20002)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                .AddRate("table.datashard.write.bytes", 60002)
                                .AddRate("table.datashard.write.rows", 50002)
                            .EndNested()
                            .StartNested("tablet_id", "301")
                                .StartNested("follower_id", "10301")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                .AddRate("table.datashard.cache_hit.bytes", 30512)
                                .AddRate("table.datashard.cache_miss.bytes", 40512)
                                .AddRate("table.datashard.consumed_cpu_us", 800512)
                                .AddRate("table.datashard.erase.bytes", 120512)
                                .AddRate("table.datashard.erase.rows", 110512)
                                .AddRate("table.datashard.read.bytes", 70191024)
                                .AddRate("table.datashard.read.rows", 30151024)
                                .AddGauge("table.datashard.row_count", 10512)
                                .AddRate("table.datashard.scan.bytes", 160512)
                                .AddRate("table.datashard.scan.rows", 150512)
                                .AddGauge("table.datashard.size_bytes", 20512)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 60512)
                                .AddRate("table.datashard.write.rows", 50512)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 280514)
                        .AddRate("table.datashard.bulk_upsert.rows", 260514)
                        .AddRate("table.datashard.cache_hit.bytes", 60514)
                        .AddRate("table.datashard.cache_miss.bytes", 80514)
                        .AddRate("table.datashard.consumed_cpu_us", 1000514)
                        .AddRate("table.datashard.erase.bytes", 240514)
                        .AddRate("table.datashard.erase.rows", 220514)
                        .AddRate("table.datashard.read.bytes", 140381028)
                        .AddRate("table.datashard.read.rows", 60301028)
                        .AddGauge("table.datashard.row_count", 20514)
                        .AddRate("table.datashard.scan.bytes", 320514)
                        .AddRate("table.datashard.scan.rows", 300514)
                        .AddGauge("table.datashard.size_bytes", 40514)
                        .AddCpuHistogram(
                            "table.datashard.used_core_percents",
                            {
                                {30, 1},
                                {90, 1},
                            }
                        )
                        .AddRate("table.datashard.write.bytes", 120514)
                        .AddRate("table.datashard.write.rows", 100514)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );

        // TEST 6: Remove metrics for all remaining partitions in the first table
        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 101, 10101);

        SendForgetDataShardTablet(runtime, aggregatorId, edgeActorId, 301, 10301);

        countersJson = NormalizeJson(
            GetPrivateJsonForCounters(*detailedCounters),
            {
                // Drop all low-level sensors to make asserts easier
                {"type", {"DataShard"}},
            }
        );

        Cerr << "TEST Current counters (after deleting table 1):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "102")
                                    .StartNested("follower_id", "10102")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                                .StartNested("tablet_id", "302")
                                    .StartNested("follower_id", "10302")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                        .AddRate("table.datashard.cache_hit.bytes", 30512)
                                        .AddRate("table.datashard.cache_miss.bytes", 40512)
                                        .AddRate("table.datashard.consumed_cpu_us", 800512)
                                        .AddRate("table.datashard.erase.bytes", 120512)
                                        .AddRate("table.datashard.erase.rows", 110512)
                                        .AddRate("table.datashard.read.bytes", 70191024)
                                        .AddRate("table.datashard.read.rows", 30151024)
                                        .AddGauge("table.datashard.row_count", 10512)
                                        .AddRate("table.datashard.scan.bytes", 160512)
                                        .AddRate("table.datashard.scan.rows", 150512)
                                        .AddGauge("table.datashard.size_bytes", 20512)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 60512)
                                        .AddRate("table.datashard.write.rows", 50512)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140512)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130512)
                                    .AddRate("table.datashard.cache_hit.bytes", 30512)
                                    .AddRate("table.datashard.cache_miss.bytes", 40512)
                                    .AddRate("table.datashard.consumed_cpu_us", 800512)
                                    .AddRate("table.datashard.erase.bytes", 120512)
                                    .AddRate("table.datashard.erase.rows", 110512)
                                    .AddRate("table.datashard.read.bytes", 70191024)
                                    .AddRate("table.datashard.read.rows", 30151024)
                                    .AddGauge("table.datashard.row_count", 10512)
                                    .AddRate("table.datashard.scan.bytes", 160512)
                                    .AddRate("table.datashard.scan.rows", 150512)
                                    .AddGauge("table.datashard.size_bytes", 20512)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 60512)
                                    .AddRate("table.datashard.write.rows", 50512)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280514)
                            .AddRate("table.datashard.bulk_upsert.rows", 260514)
                            .AddRate("table.datashard.cache_hit.bytes", 60514)
                            .AddRate("table.datashard.cache_miss.bytes", 80514)
                            .AddRate("table.datashard.consumed_cpu_us", 1000514)
                            .AddRate("table.datashard.erase.bytes", 240514)
                            .AddRate("table.datashard.erase.rows", 220514)
                            .AddRate("table.datashard.read.bytes", 140381028)
                            .AddRate("table.datashard.read.rows", 60301028)
                            .AddGauge("table.datashard.row_count", 20514)
                            .AddRate("table.datashard.scan.bytes", 320514)
                            .AddRate("table.datashard.scan.rows", 300514)
                            .AddGauge("table.datashard.size_bytes", 40514)
                            .AddCpuHistogram(
                                "table.datashard.used_core_percents",
                                {
                                    {30, 1},
                                    {90, 1},
                                }
                            )
                            .AddRate("table.datashard.write.bytes", 120514)
                            .AddRate("table.datashard.write.rows", 100514)
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

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 10, &tableMetricsConfig1, 0x01);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 12340002,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-2"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 20, &tableMetricsConfig2, 0x02);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig3 = {
            .TableId = 12340003,
            .TablePath = "/Root/fake-db/fake-table3",
            .TableSchemaVersion = 1000333,
            .TenantDbSchemaVersion = 2000333,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-3"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 30, &tableMetricsConfig3, 0x04);

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
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                            .AddRate("table.datashard.bulk_upsert.rows", 130002)
                            .AddRate("table.datashard.cache_hit.bytes", 30002)
                            .AddRate("table.datashard.cache_miss.bytes", 40002)
                            .AddRate("table.datashard.consumed_cpu_us", 200002)
                            .AddRate("table.datashard.erase.bytes", 120002)
                            .AddRate("table.datashard.erase.rows", 110002)
                            .AddRate("table.datashard.read.bytes", 70190004)
                            .AddRate("table.datashard.read.rows", 30150004)
                            .AddGauge("table.datashard.row_count", 10002)
                            .AddRate("table.datashard.scan.bytes", 160002)
                            .AddRate("table.datashard.scan.rows", 150002)
                            .AddGauge("table.datashard.size_bytes", 20002)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                            .AddRate("table.datashard.write.bytes", 60002)
                            .AddRate("table.datashard.write.rows", 50002)
                        .EndNested()
                    .EndNested()
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                        .StartNested("table", "/Root/fake-db/fake-table3")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "303")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                        .AddRate("table.datashard.cache_hit.bytes", 30004)
                                        .AddRate("table.datashard.cache_miss.bytes", 40004)
                                        .AddRate("table.datashard.consumed_cpu_us", 300004)
                                        .AddRate("table.datashard.erase.bytes", 120004)
                                        .AddRate("table.datashard.erase.rows", 110004)
                                        .AddRate("table.datashard.read.bytes", 70190008)
                                        .AddRate("table.datashard.read.rows", 30150008)
                                        .AddGauge("table.datashard.row_count", 10004)
                                        .AddRate("table.datashard.scan.bytes", 160004)
                                        .AddRate("table.datashard.scan.rows", 150004)
                                        .AddGauge("table.datashard.size_bytes", 20004)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60004)
                                        .AddRate("table.datashard.write.rows", 50004)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 300004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                            .AddRate("table.datashard.bulk_upsert.rows", 130004)
                            .AddRate("table.datashard.cache_hit.bytes", 30004)
                            .AddRate("table.datashard.cache_miss.bytes", 40004)
                            .AddRate("table.datashard.consumed_cpu_us", 300004)
                            .AddRate("table.datashard.erase.bytes", 120004)
                            .AddRate("table.datashard.erase.rows", 110004)
                            .AddRate("table.datashard.read.bytes", 70190008)
                            .AddRate("table.datashard.read.rows", 30150008)
                            .AddGauge("table.datashard.row_count", 10004)
                            .AddRate("table.datashard.scan.bytes", 160004)
                            .AddRate("table.datashard.scan.rows", 150004)
                            .AddGauge("table.datashard.size_bytes", 20004)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                            .AddRate("table.datashard.write.bytes", 60004)
                            .AddRate("table.datashard.write.rows", 50004)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                .AddRate("table.datashard.cache_hit.bytes", 30001)
                                .AddRate("table.datashard.cache_miss.bytes", 40001)
                                .AddRate("table.datashard.consumed_cpu_us", 100001)
                                .AddRate("table.datashard.erase.bytes", 120001)
                                .AddRate("table.datashard.erase.rows", 110001)
                                .AddRate("table.datashard.read.bytes", 70190002)
                                .AddRate("table.datashard.read.rows", 30150002)
                                .AddGauge("table.datashard.row_count", 10001)
                                .AddRate("table.datashard.scan.bytes", 160001)
                                .AddRate("table.datashard.scan.rows", 150001)
                                .AddGauge("table.datashard.size_bytes", 20001)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                .AddRate("table.datashard.write.bytes", 60001)
                                .AddRate("table.datashard.write.rows", 50001)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                        .AddRate("table.datashard.bulk_upsert.rows", 130001)
                        .AddRate("table.datashard.cache_hit.bytes", 30001)
                        .AddRate("table.datashard.cache_miss.bytes", 40001)
                        .AddRate("table.datashard.consumed_cpu_us", 100001)
                        .AddRate("table.datashard.erase.bytes", 120001)
                        .AddRate("table.datashard.erase.rows", 110001)
                        .AddRate("table.datashard.read.bytes", 70190002)
                        .AddRate("table.datashard.read.rows", 30150002)
                        .AddGauge("table.datashard.row_count", 10001)
                        .AddRate("table.datashard.scan.bytes", 160001)
                        .AddRate("table.datashard.scan.rows", 150001)
                        .AddGauge("table.datashard.size_bytes", 20001)
                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                        .AddRate("table.datashard.write.bytes", 60001)
                        .AddRate("table.datashard.write.rows", 50001)
                    .EndNested()
                .EndNested()
                .BuildJson()
        );

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

            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &invalidTableMetricsConfig2, 0x08);

            // Update the other two tables to make sure the regular updates work
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &tableMetricsConfig1, 0x10);
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
        } else {
            TEvTabletCounters::TTableMetricsConfig invalidTableMetricsConfig1 = tableMetricsConfig1;

            invalidTableMetricsConfig1.TenantDbSchemaVersion = 2000110;
            invalidTableMetricsConfig1.MonitoringProjectId = newMonitoringProjectId;

            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &invalidTableMetricsConfig1, 0x10);

            // Update the other two tables to make sure the regular updates work
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &tableMetricsConfig2, 0x08);
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
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

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .StartNested("database", "1113-1001")
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                    .AddRate("table.datashard.cache_hit.bytes", 60010)
                                    .AddRate("table.datashard.cache_miss.bytes", 80010)
                                    .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                    .AddRate("table.datashard.erase.bytes", 240010)
                                    .AddRate("table.datashard.erase.rows", 220010)
                                    .AddRate("table.datashard.read.bytes", 140380020)
                                    .AddRate("table.datashard.read.rows", 60300020)
                                    .AddGauge("table.datashard.row_count", 10008)
                                    .AddRate("table.datashard.scan.bytes", 320010)
                                    .AddRate("table.datashard.scan.rows", 300010)
                                    .AddGauge("table.datashard.size_bytes", 20008)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 120010)
                                    .AddRate("table.datashard.write.rows", 100010)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                            .AddRate("table.datashard.erase.bytes", 240010)
                            .AddRate("table.datashard.erase.rows", 220010)
                            .AddRate("table.datashard.read.bytes", 140380020)
                            .AddRate("table.datashard.read.rows", 60300020)
                            .AddGauge("table.datashard.row_count", 10008)
                            .AddRate("table.datashard.scan.bytes", 320010)
                            .AddRate("table.datashard.scan.rows", 300010)
                            .AddGauge("table.datashard.size_bytes", 20008)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                            .AddRate("table.datashard.write.bytes", 120010)
                            .AddRate("table.datashard.write.rows", 100010)
                        .EndNested()
                    .EndNested()
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                        .StartNested("table", "/Root/fake-db/fake-table3")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "303")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                    .AddRate("table.datashard.cache_hit.bytes", 60036)
                                    .AddRate("table.datashard.cache_miss.bytes", 80036)
                                    .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                    .AddRate("table.datashard.erase.bytes", 240036)
                                    .AddRate("table.datashard.erase.rows", 220036)
                                    .AddRate("table.datashard.read.bytes", 140380072)
                                    .AddRate("table.datashard.read.rows", 60300072)
                                    .AddGauge("table.datashard.row_count", 10032)
                                    .AddRate("table.datashard.scan.bytes", 320036)
                                    .AddRate("table.datashard.scan.rows", 300036)
                                    .AddGauge("table.datashard.size_bytes", 20032)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                    .AddRate("table.datashard.write.bytes", 120036)
                                    .AddRate("table.datashard.write.rows", 100036)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                            .AddRate("table.datashard.erase.bytes", 240036)
                            .AddRate("table.datashard.erase.rows", 220036)
                            .AddRate("table.datashard.read.bytes", 140380072)
                            .AddRate("table.datashard.read.rows", 60300072)
                            .AddGauge("table.datashard.row_count", 10032)
                            .AddRate("table.datashard.scan.bytes", 320036)
                            .AddRate("table.datashard.scan.rows", 300036)
                            .AddGauge("table.datashard.size_bytes", 20032)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                            .AddRate("table.datashard.write.bytes", 120036)
                            .AddRate("table.datashard.write.rows", 100036)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                    .AddRate("table.datashard.cache_hit.bytes", 60017)
                                    .AddRate("table.datashard.cache_miss.bytes", 80017)
                                    .AddRate("table.datashard.consumed_cpu_us", 800017)
                                    .AddRate("table.datashard.erase.bytes", 240017)
                                    .AddRate("table.datashard.erase.rows", 220017)
                                    .AddRate("table.datashard.read.bytes", 140380034)
                                    .AddRate("table.datashard.read.rows", 60300034)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 320017)
                                    .AddRate("table.datashard.scan.rows", 300017)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 120017)
                                    .AddRate("table.datashard.write.rows", 100017)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                .AddRate("table.datashard.cache_hit.bytes", 60017)
                                .AddRate("table.datashard.cache_miss.bytes", 80017)
                                .AddRate("table.datashard.consumed_cpu_us", 800017)
                                .AddRate("table.datashard.erase.bytes", 240017)
                                .AddRate("table.datashard.erase.rows", 220017)
                                .AddRate("table.datashard.read.bytes", 140380034)
                                .AddRate("table.datashard.read.rows", 60300034)
                                .AddGauge("table.datashard.row_count", 10016)
                                .AddRate("table.datashard.scan.bytes", 320017)
                                .AddRate("table.datashard.scan.rows", 300017)
                                .AddGauge("table.datashard.size_bytes", 20016)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                .AddRate("table.datashard.write.bytes", 120017)
                                .AddRate("table.datashard.write.rows", 100017)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                        .AddRate("table.datashard.erase.bytes", 240017)
                        .AddRate("table.datashard.erase.rows", 220017)
                        .AddRate("table.datashard.read.bytes", 140380034)
                        .AddRate("table.datashard.read.rows", 60300034)
                        .AddGauge("table.datashard.row_count", 10016)
                        .AddRate("table.datashard.scan.bytes", 320017)
                        .AddRate("table.datashard.scan.rows", 300017)
                        .AddGauge("table.datashard.size_bytes", 20016)
                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                        .AddRate("table.datashard.write.bytes", 120017)
                        .AddRate("table.datashard.write.rows", 100017)
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
        const TString& finalExpectedJson
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

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 10, &tableMetricsConfig1, 0x01);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig2 = {
            .TableId = 12340002,
            .TablePath = "/Root/fake-db/fake-table2",
            .TableSchemaVersion = 1000222,
            .TenantDbSchemaVersion = 2000222,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-2"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 20, &tableMetricsConfig2, 0x02);

        TEvTabletCounters::TTableMetricsConfig tableMetricsConfig3 = {
            .TableId = 12340003,
            .TablePath = "/Root/fake-db/fake-table3",
            .TableSchemaVersion = 1000333,
            .TenantDbSchemaVersion = 2000333,
            .MetricsLevel = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition,
            .MonitoringProjectId = "fake-monitoring-project-id-3"
        };

        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 30, &tableMetricsConfig3, 0x04);

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
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                        .AddRate("table.datashard.cache_hit.bytes", 30002)
                                        .AddRate("table.datashard.cache_miss.bytes", 40002)
                                        .AddRate("table.datashard.consumed_cpu_us", 200002)
                                        .AddRate("table.datashard.erase.bytes", 120002)
                                        .AddRate("table.datashard.erase.rows", 110002)
                                        .AddRate("table.datashard.read.bytes", 70190004)
                                        .AddRate("table.datashard.read.rows", 30150004)
                                        .AddGauge("table.datashard.row_count", 10002)
                                        .AddRate("table.datashard.scan.bytes", 160002)
                                        .AddRate("table.datashard.scan.rows", 150002)
                                        .AddGauge("table.datashard.size_bytes", 20002)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                        .AddRate("table.datashard.write.bytes", 60002)
                                        .AddRate("table.datashard.write.rows", 50002)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130002)
                                    .AddRate("table.datashard.cache_hit.bytes", 30002)
                                    .AddRate("table.datashard.cache_miss.bytes", 40002)
                                    .AddRate("table.datashard.consumed_cpu_us", 200002)
                                    .AddRate("table.datashard.erase.bytes", 120002)
                                    .AddRate("table.datashard.erase.rows", 110002)
                                    .AddRate("table.datashard.read.bytes", 70190004)
                                    .AddRate("table.datashard.read.rows", 30150004)
                                    .AddGauge("table.datashard.row_count", 10002)
                                    .AddRate("table.datashard.scan.bytes", 160002)
                                    .AddRate("table.datashard.scan.rows", 150002)
                                    .AddGauge("table.datashard.size_bytes", 20002)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                                    .AddRate("table.datashard.write.bytes", 60002)
                                    .AddRate("table.datashard.write.rows", 50002)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 140002)
                            .AddRate("table.datashard.bulk_upsert.rows", 130002)
                            .AddRate("table.datashard.cache_hit.bytes", 30002)
                            .AddRate("table.datashard.cache_miss.bytes", 40002)
                            .AddRate("table.datashard.consumed_cpu_us", 200002)
                            .AddRate("table.datashard.erase.bytes", 120002)
                            .AddRate("table.datashard.erase.rows", 110002)
                            .AddRate("table.datashard.read.bytes", 70190004)
                            .AddRate("table.datashard.read.rows", 30150004)
                            .AddGauge("table.datashard.row_count", 10002)
                            .AddRate("table.datashard.scan.bytes", 160002)
                            .AddRate("table.datashard.scan.rows", 150002)
                            .AddGauge("table.datashard.size_bytes", 20002)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}})
                            .AddRate("table.datashard.write.bytes", 60002)
                            .AddRate("table.datashard.write.rows", 50002)
                        .EndNested()
                    .EndNested()
                    .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                        .StartNested("table", "/Root/fake-db/fake-table3")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "303")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                        .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                        .AddRate("table.datashard.cache_hit.bytes", 30004)
                                        .AddRate("table.datashard.cache_miss.bytes", 40004)
                                        .AddRate("table.datashard.consumed_cpu_us", 300004)
                                        .AddRate("table.datashard.erase.bytes", 120004)
                                        .AddRate("table.datashard.erase.rows", 110004)
                                        .AddRate("table.datashard.read.bytes", 70190008)
                                        .AddRate("table.datashard.read.rows", 30150008)
                                        .AddGauge("table.datashard.row_count", 10004)
                                        .AddRate("table.datashard.scan.bytes", 160004)
                                        .AddRate("table.datashard.scan.rows", 150004)
                                        .AddGauge("table.datashard.size_bytes", 20004)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                        .AddRate("table.datashard.write.bytes", 60004)
                                        .AddRate("table.datashard.write.rows", 50004)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130004)
                                    .AddRate("table.datashard.cache_hit.bytes", 30004)
                                    .AddRate("table.datashard.cache_miss.bytes", 40004)
                                    .AddRate("table.datashard.consumed_cpu_us", 300004)
                                    .AddRate("table.datashard.erase.bytes", 120004)
                                    .AddRate("table.datashard.erase.rows", 110004)
                                    .AddRate("table.datashard.read.bytes", 70190008)
                                    .AddRate("table.datashard.read.rows", 30150008)
                                    .AddGauge("table.datashard.row_count", 10004)
                                    .AddRate("table.datashard.scan.bytes", 160004)
                                    .AddRate("table.datashard.scan.rows", 150004)
                                    .AddGauge("table.datashard.size_bytes", 20004)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                                    .AddRate("table.datashard.write.bytes", 60004)
                                    .AddRate("table.datashard.write.rows", 50004)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 140004)
                            .AddRate("table.datashard.bulk_upsert.rows", 130004)
                            .AddRate("table.datashard.cache_hit.bytes", 30004)
                            .AddRate("table.datashard.cache_miss.bytes", 40004)
                            .AddRate("table.datashard.consumed_cpu_us", 300004)
                            .AddRate("table.datashard.erase.bytes", 120004)
                            .AddRate("table.datashard.erase.rows", 110004)
                            .AddRate("table.datashard.read.bytes", 70190008)
                            .AddRate("table.datashard.read.rows", 30150008)
                            .AddGauge("table.datashard.row_count", 10004)
                            .AddRate("table.datashard.scan.bytes", 160004)
                            .AddRate("table.datashard.scan.rows", 150004)
                            .AddGauge("table.datashard.size_bytes", 20004)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{40, 1}})
                            .AddRate("table.datashard.write.bytes", 60004)
                            .AddRate("table.datashard.write.rows", 50004)
                        .EndNested()
                    .EndNested()
                    .StartNested("table", "/Root/fake-db/fake-table1")
                        .StartNested("detailed_metrics", "per_partition")
                            .StartNested("tablet_id", "101")
                                .StartNested("follower_id", "0")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                    .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                    .AddRate("table.datashard.cache_hit.bytes", 30001)
                                    .AddRate("table.datashard.cache_miss.bytes", 40001)
                                    .AddRate("table.datashard.consumed_cpu_us", 100001)
                                    .AddRate("table.datashard.erase.bytes", 120001)
                                    .AddRate("table.datashard.erase.rows", 110001)
                                    .AddRate("table.datashard.read.bytes", 70190002)
                                    .AddRate("table.datashard.read.rows", 30150002)
                                    .AddGauge("table.datashard.row_count", 10001)
                                    .AddRate("table.datashard.scan.bytes", 160001)
                                    .AddRate("table.datashard.scan.rows", 150001)
                                    .AddGauge("table.datashard.size_bytes", 20001)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                    .AddRate("table.datashard.write.bytes", 60001)
                                    .AddRate("table.datashard.write.rows", 50001)
                                .EndNested()
                                .StartNested("follower_id", "replicas_only")
                                    .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                    .AddRate("table.datashard.bulk_upsert.rows", 0)
                                    .AddRate("table.datashard.cache_hit.bytes", 0)
                                    .AddRate("table.datashard.cache_miss.bytes", 0)
                                    .AddRate("table.datashard.consumed_cpu_us", 0)
                                    .AddRate("table.datashard.erase.bytes", 0)
                                    .AddRate("table.datashard.erase.rows", 0)
                                    .AddRate("table.datashard.read.bytes", 0)
                                    .AddRate("table.datashard.read.rows", 0)
                                    .AddGauge("table.datashard.row_count", 0)
                                    .AddRate("table.datashard.scan.bytes", 0)
                                    .AddRate("table.datashard.scan.rows", 0)
                                    .AddGauge("table.datashard.size_bytes", 0)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {})
                                    .AddRate("table.datashard.write.bytes", 0)
                                    .AddRate("table.datashard.write.rows", 0)
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                                .AddRate("table.datashard.bulk_upsert.rows", 130001)
                                .AddRate("table.datashard.cache_hit.bytes", 30001)
                                .AddRate("table.datashard.cache_miss.bytes", 40001)
                                .AddRate("table.datashard.consumed_cpu_us", 100001)
                                .AddRate("table.datashard.erase.bytes", 120001)
                                .AddRate("table.datashard.erase.rows", 110001)
                                .AddRate("table.datashard.read.bytes", 70190002)
                                .AddRate("table.datashard.read.rows", 30150002)
                                .AddGauge("table.datashard.row_count", 10001)
                                .AddRate("table.datashard.scan.bytes", 160001)
                                .AddRate("table.datashard.scan.rows", 150001)
                                .AddGauge("table.datashard.size_bytes", 20001)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                                .AddRate("table.datashard.write.bytes", 60001)
                                .AddRate("table.datashard.write.rows", 50001)
                            .EndNested()
                        .EndNested()
                        .AddRate("table.datashard.bulk_upsert.bytes", 140001)
                        .AddRate("table.datashard.bulk_upsert.rows", 130001)
                        .AddRate("table.datashard.cache_hit.bytes", 30001)
                        .AddRate("table.datashard.cache_miss.bytes", 40001)
                        .AddRate("table.datashard.consumed_cpu_us", 100001)
                        .AddRate("table.datashard.erase.bytes", 120001)
                        .AddRate("table.datashard.erase.rows", 110001)
                        .AddRate("table.datashard.read.bytes", 70190002)
                        .AddRate("table.datashard.read.rows", 30150002)
                        .AddGauge("table.datashard.row_count", 10001)
                        .AddRate("table.datashard.scan.bytes", 160001)
                        .AddRate("table.datashard.scan.rows", 150001)
                        .AddGauge("table.datashard.size_bytes", 20001)
                        .AddCpuHistogram("table.datashard.used_core_percents", {{20, 1}})
                        .AddRate("table.datashard.write.bytes", 60001)
                        .AddRate("table.datashard.write.rows", 50001)
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

            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &invalidTableMetricsConfig2, 0x08);

            // Update the other two tables to make sure the regular updates work
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &tableMetricsConfig1, 0x10);
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
        } else {
            TEvTabletCounters::TTableMetricsConfig invalidTableMetricsConfig1 = tableMetricsConfig1;

            invalidTableMetricsConfig1.TenantDbSchemaVersion = 2000112;
            invalidTableMetricsConfig1.MonitoringProjectId = newMonitoringProjectId;

            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 101, 0, 70, &invalidTableMetricsConfig1, 0x10);

            // Update the other two tables to make sure the regular updates work
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 202, 0, 80, &tableMetricsConfig2, 0x08);
            SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 303, 0, 90, &tableMetricsConfig3, 0x20);
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
            NormalizeJson(
                TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "303")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                            .AddRate("table.datashard.erase.bytes", 240036)
                                            .AddRate("table.datashard.erase.rows", 220036)
                                            .AddRate("table.datashard.read.bytes", 140380072)
                                            .AddRate("table.datashard.read.rows", 60300072)
                                            .AddGauge("table.datashard.row_count", 10032)
                                            .AddRate("table.datashard.scan.bytes", 320036)
                                            .AddRate("table.datashard.scan.rows", 300036)
                                            .AddGauge("table.datashard.size_bytes", 20032)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                            .AddRate("table.datashard.write.bytes", 120036)
                                            .AddRate("table.datashard.write.rows", 100036)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                .AddRate("table.datashard.cache_hit.bytes", 60036)
                                .AddRate("table.datashard.cache_miss.bytes", 80036)
                                .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                .AddRate("table.datashard.erase.bytes", 240036)
                                .AddRate("table.datashard.erase.rows", 220036)
                                .AddRate("table.datashard.read.bytes", 140380072)
                                .AddRate("table.datashard.read.rows", 60300072)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 320036)
                                .AddRate("table.datashard.scan.rows", 300036)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                .AddRate("table.datashard.write.bytes", 120036)
                                .AddRate("table.datashard.write.rows", 100036)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "101")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                                        .AddRate("table.datashard.erase.bytes", 240017)
                                        .AddRate("table.datashard.erase.rows", 220017)
                                        .AddRate("table.datashard.read.bytes", 140380034)
                                        .AddRate("table.datashard.read.rows", 60300034)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320017)
                                        .AddRate("table.datashard.scan.rows", 300017)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 120017)
                                        .AddRate("table.datashard.write.rows", 100017)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                    .AddRate("table.datashard.cache_hit.bytes", 60017)
                                    .AddRate("table.datashard.cache_miss.bytes", 80017)
                                    .AddRate("table.datashard.consumed_cpu_us", 800017)
                                    .AddRate("table.datashard.erase.bytes", 240017)
                                    .AddRate("table.datashard.erase.rows", 220017)
                                    .AddRate("table.datashard.read.bytes", 140380034)
                                    .AddRate("table.datashard.read.rows", 60300034)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 320017)
                                    .AddRate("table.datashard.scan.rows", 300017)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 120017)
                                    .AddRate("table.datashard.write.rows", 100017)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                            .AddRate("table.datashard.bulk_upsert.rows", 260017)
                            .AddRate("table.datashard.cache_hit.bytes", 60017)
                            .AddRate("table.datashard.cache_miss.bytes", 80017)
                            .AddRate("table.datashard.consumed_cpu_us", 800017)
                            .AddRate("table.datashard.erase.bytes", 240017)
                            .AddRate("table.datashard.erase.rows", 220017)
                            .AddRate("table.datashard.read.bytes", 140380034)
                            .AddRate("table.datashard.read.rows", 60300034)
                            .AddGauge("table.datashard.row_count", 10016)
                            .AddRate("table.datashard.scan.bytes", 320017)
                            .AddRate("table.datashard.scan.rows", 300017)
                            .AddGauge("table.datashard.size_bytes", 20016)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                            .AddRate("table.datashard.write.bytes", 120017)
                            .AddRate("table.datashard.write.rows", 100017)
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table2")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "202")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                    .AddRate("table.datashard.cache_hit.bytes", 60010)
                                    .AddRate("table.datashard.cache_miss.bytes", 80010)
                                    .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                    .AddRate("table.datashard.erase.bytes", 240010)
                                    .AddRate("table.datashard.erase.rows", 220010)
                                    .AddRate("table.datashard.read.bytes", 140380020)
                                    .AddRate("table.datashard.read.rows", 60300020)
                                    .AddGauge("table.datashard.row_count", 10008)
                                    .AddRate("table.datashard.scan.bytes", 320010)
                                    .AddRate("table.datashard.scan.rows", 300010)
                                    .AddGauge("table.datashard.size_bytes", 20008)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                    .AddRate("table.datashard.write.bytes", 120010)
                                    .AddRate("table.datashard.write.rows", 100010)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                            .AddRate("table.datashard.erase.bytes", 240010)
                            .AddRate("table.datashard.erase.rows", 220010)
                            .AddRate("table.datashard.read.bytes", 140380020)
                            .AddRate("table.datashard.read.rows", 60300020)
                            .AddGauge("table.datashard.row_count", 10008)
                            .AddRate("table.datashard.scan.bytes", 320010)
                            .AddRate("table.datashard.scan.rows", 300010)
                            .AddGauge("table.datashard.size_bytes", 20008)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                            .AddRate("table.datashard.write.bytes", 120010)
                            .AddRate("table.datashard.write.rows", 100010)
                        .EndNested()
                    .EndNested()
                    .BuildJson()
            )
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
            NormalizeJson(
                TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-1")
                            .StartNested("table", "/Root/fake-db/fake-table1")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "101")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                            .AddRate("table.datashard.cache_hit.bytes", 60017)
                                            .AddRate("table.datashard.cache_miss.bytes", 80017)
                                            .AddRate("table.datashard.consumed_cpu_us", 800017)
                                            .AddRate("table.datashard.erase.bytes", 240017)
                                            .AddRate("table.datashard.erase.rows", 220017)
                                            .AddRate("table.datashard.read.bytes", 140380034)
                                            .AddRate("table.datashard.read.rows", 60300034)
                                            .AddGauge("table.datashard.row_count", 10016)
                                            .AddRate("table.datashard.scan.bytes", 320017)
                                            .AddRate("table.datashard.scan.rows", 300017)
                                            .AddGauge("table.datashard.size_bytes", 20016)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                            .AddRate("table.datashard.write.bytes", 120017)
                                            .AddRate("table.datashard.write.rows", 100017)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                                        .AddRate("table.datashard.erase.bytes", 240017)
                                        .AddRate("table.datashard.erase.rows", 220017)
                                        .AddRate("table.datashard.read.bytes", 140380034)
                                        .AddRate("table.datashard.read.rows", 60300034)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320017)
                                        .AddRate("table.datashard.scan.rows", 300017)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 120017)
                                        .AddRate("table.datashard.write.rows", 100017)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                .AddRate("table.datashard.cache_hit.bytes", 60017)
                                .AddRate("table.datashard.cache_miss.bytes", 80017)
                                .AddRate("table.datashard.consumed_cpu_us", 800017)
                                .AddRate("table.datashard.erase.bytes", 240017)
                                .AddRate("table.datashard.erase.rows", 220017)
                                .AddRate("table.datashard.read.bytes", 140380034)
                                .AddRate("table.datashard.read.rows", 60300034)
                                .AddGauge("table.datashard.row_count", 10016)
                                .AddRate("table.datashard.scan.bytes", 320017)
                                .AddRate("table.datashard.scan.rows", 300017)
                                .AddGauge("table.datashard.size_bytes", 20016)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                .AddRate("table.datashard.write.bytes", 120017)
                                .AddRate("table.datashard.write.rows", 100017)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "202")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                            .AddRate("table.datashard.erase.bytes", 240010)
                                            .AddRate("table.datashard.erase.rows", 220010)
                                            .AddRate("table.datashard.read.bytes", 140380020)
                                            .AddRate("table.datashard.read.rows", 60300020)
                                            .AddGauge("table.datashard.row_count", 10008)
                                            .AddRate("table.datashard.scan.bytes", 320010)
                                            .AddRate("table.datashard.scan.rows", 300010)
                                            .AddGauge("table.datashard.size_bytes", 20008)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                            .AddRate("table.datashard.write.bytes", 120010)
                                            .AddRate("table.datashard.write.rows", 100010)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                .AddRate("table.datashard.cache_hit.bytes", 60010)
                                .AddRate("table.datashard.cache_miss.bytes", 80010)
                                .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                .AddRate("table.datashard.erase.bytes", 240010)
                                .AddRate("table.datashard.erase.rows", 220010)
                                .AddRate("table.datashard.read.bytes", 140380020)
                                .AddRate("table.datashard.read.rows", 60300020)
                                .AddGauge("table.datashard.row_count", 10008)
                                .AddRate("table.datashard.scan.bytes", 320010)
                                .AddRate("table.datashard.scan.rows", 300010)
                                .AddGauge("table.datashard.size_bytes", 20008)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 120010)
                                .AddRate("table.datashard.write.rows", 100010)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "303")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                            .AddRate("table.datashard.erase.bytes", 240036)
                                            .AddRate("table.datashard.erase.rows", 220036)
                                            .AddRate("table.datashard.read.bytes", 140380072)
                                            .AddRate("table.datashard.read.rows", 60300072)
                                            .AddGauge("table.datashard.row_count", 10032)
                                            .AddRate("table.datashard.scan.bytes", 320036)
                                            .AddRate("table.datashard.scan.rows", 300036)
                                            .AddGauge("table.datashard.size_bytes", 20032)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                            .AddRate("table.datashard.write.bytes", 120036)
                                            .AddRate("table.datashard.write.rows", 100036)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                .AddRate("table.datashard.cache_hit.bytes", 60036)
                                .AddRate("table.datashard.cache_miss.bytes", 80036)
                                .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                .AddRate("table.datashard.erase.bytes", 240036)
                                .AddRate("table.datashard.erase.rows", 220036)
                                .AddRate("table.datashard.read.bytes", 140380072)
                                .AddRate("table.datashard.read.rows", 60300072)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 320036)
                                .AddRate("table.datashard.scan.rows", 300036)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                .AddRate("table.datashard.write.bytes", 120036)
                                .AddRate("table.datashard.write.rows", 100036)
                            .EndNested()
                        .EndNested()
                    .EndNested()
                    .BuildJson()
            )
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
            NormalizeJson(
                TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "202")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                            .AddRate("table.datashard.erase.bytes", 240010)
                                            .AddRate("table.datashard.erase.rows", 220010)
                                            .AddRate("table.datashard.read.bytes", 140380020)
                                            .AddRate("table.datashard.read.rows", 60300020)
                                            .AddGauge("table.datashard.row_count", 10008)
                                            .AddRate("table.datashard.scan.bytes", 320010)
                                            .AddRate("table.datashard.scan.rows", 300010)
                                            .AddGauge("table.datashard.size_bytes", 20008)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                            .AddRate("table.datashard.write.bytes", 120010)
                                            .AddRate("table.datashard.write.rows", 100010)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                .AddRate("table.datashard.cache_hit.bytes", 60010)
                                .AddRate("table.datashard.cache_miss.bytes", 80010)
                                .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                .AddRate("table.datashard.erase.bytes", 240010)
                                .AddRate("table.datashard.erase.rows", 220010)
                                .AddRate("table.datashard.read.bytes", 140380020)
                                .AddRate("table.datashard.read.rows", 60300020)
                                .AddGauge("table.datashard.row_count", 10008)
                                .AddRate("table.datashard.scan.bytes", 320010)
                                .AddRate("table.datashard.scan.rows", 300010)
                                .AddGauge("table.datashard.size_bytes", 20008)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 120010)
                                .AddRate("table.datashard.write.rows", 100010)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table1")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "101")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                            .AddRate("table.datashard.cache_hit.bytes", 60017)
                                            .AddRate("table.datashard.cache_miss.bytes", 80017)
                                            .AddRate("table.datashard.consumed_cpu_us", 800017)
                                            .AddRate("table.datashard.erase.bytes", 240017)
                                            .AddRate("table.datashard.erase.rows", 220017)
                                            .AddRate("table.datashard.read.bytes", 140380034)
                                            .AddRate("table.datashard.read.rows", 60300034)
                                            .AddGauge("table.datashard.row_count", 10016)
                                            .AddRate("table.datashard.scan.bytes", 320017)
                                            .AddRate("table.datashard.scan.rows", 300017)
                                            .AddGauge("table.datashard.size_bytes", 20016)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                            .AddRate("table.datashard.write.bytes", 120017)
                                            .AddRate("table.datashard.write.rows", 100017)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                                        .AddRate("table.datashard.erase.bytes", 240017)
                                        .AddRate("table.datashard.erase.rows", 220017)
                                        .AddRate("table.datashard.read.bytes", 140380034)
                                        .AddRate("table.datashard.read.rows", 60300034)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320017)
                                        .AddRate("table.datashard.scan.rows", 300017)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 120017)
                                        .AddRate("table.datashard.write.rows", 100017)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                .AddRate("table.datashard.cache_hit.bytes", 60017)
                                .AddRate("table.datashard.cache_miss.bytes", 80017)
                                .AddRate("table.datashard.consumed_cpu_us", 800017)
                                .AddRate("table.datashard.erase.bytes", 240017)
                                .AddRate("table.datashard.erase.rows", 220017)
                                .AddRate("table.datashard.read.bytes", 140380034)
                                .AddRate("table.datashard.read.rows", 60300034)
                                .AddGauge("table.datashard.row_count", 10016)
                                .AddRate("table.datashard.scan.bytes", 320017)
                                .AddRate("table.datashard.scan.rows", 300017)
                                .AddGauge("table.datashard.size_bytes", 20016)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                .AddRate("table.datashard.write.bytes", 120017)
                                .AddRate("table.datashard.write.rows", 100017)
                            .EndNested()
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "303")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                            .AddRate("table.datashard.erase.bytes", 240036)
                                            .AddRate("table.datashard.erase.rows", 220036)
                                            .AddRate("table.datashard.read.bytes", 140380072)
                                            .AddRate("table.datashard.read.rows", 60300072)
                                            .AddGauge("table.datashard.row_count", 10032)
                                            .AddRate("table.datashard.scan.bytes", 320036)
                                            .AddRate("table.datashard.scan.rows", 300036)
                                            .AddGauge("table.datashard.size_bytes", 20032)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                            .AddRate("table.datashard.write.bytes", 120036)
                                            .AddRate("table.datashard.write.rows", 100036)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                .AddRate("table.datashard.cache_hit.bytes", 60036)
                                .AddRate("table.datashard.cache_miss.bytes", 80036)
                                .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                .AddRate("table.datashard.erase.bytes", 240036)
                                .AddRate("table.datashard.erase.rows", 220036)
                                .AddRate("table.datashard.read.bytes", 140380072)
                                .AddRate("table.datashard.read.rows", 60300072)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 320036)
                                .AddRate("table.datashard.scan.rows", 300036)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                .AddRate("table.datashard.write.bytes", 120036)
                                .AddRate("table.datashard.write.rows", 100036)
                            .EndNested()
                        .EndNested()
                    .EndNested()
                    .BuildJson()
            )
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
            NormalizeJson(
                TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-1")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "202")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                            .AddRate("table.datashard.erase.bytes", 240010)
                                            .AddRate("table.datashard.erase.rows", 220010)
                                            .AddRate("table.datashard.read.bytes", 140380020)
                                            .AddRate("table.datashard.read.rows", 60300020)
                                            .AddGauge("table.datashard.row_count", 10008)
                                            .AddRate("table.datashard.scan.bytes", 320010)
                                            .AddRate("table.datashard.scan.rows", 300010)
                                            .AddGauge("table.datashard.size_bytes", 20008)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                            .AddRate("table.datashard.write.bytes", 120010)
                                            .AddRate("table.datashard.write.rows", 100010)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                .AddRate("table.datashard.cache_hit.bytes", 60010)
                                .AddRate("table.datashard.cache_miss.bytes", 80010)
                                .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                .AddRate("table.datashard.erase.bytes", 240010)
                                .AddRate("table.datashard.erase.rows", 220010)
                                .AddRate("table.datashard.read.bytes", 140380020)
                                .AddRate("table.datashard.read.rows", 60300020)
                                .AddGauge("table.datashard.row_count", 10008)
                                .AddRate("table.datashard.scan.bytes", 320010)
                                .AddRate("table.datashard.scan.rows", 300010)
                                .AddGauge("table.datashard.size_bytes", 20008)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 120010)
                                .AddRate("table.datashard.write.rows", 100010)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "303")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                            .AddRate("table.datashard.erase.bytes", 240036)
                                            .AddRate("table.datashard.erase.rows", 220036)
                                            .AddRate("table.datashard.read.bytes", 140380072)
                                            .AddRate("table.datashard.read.rows", 60300072)
                                            .AddGauge("table.datashard.row_count", 10032)
                                            .AddRate("table.datashard.scan.bytes", 320036)
                                            .AddRate("table.datashard.scan.rows", 300036)
                                            .AddGauge("table.datashard.size_bytes", 20032)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                            .AddRate("table.datashard.write.bytes", 120036)
                                            .AddRate("table.datashard.write.rows", 100036)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                .AddRate("table.datashard.cache_hit.bytes", 60036)
                                .AddRate("table.datashard.cache_miss.bytes", 80036)
                                .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                .AddRate("table.datashard.erase.bytes", 240036)
                                .AddRate("table.datashard.erase.rows", 220036)
                                .AddRate("table.datashard.read.bytes", 140380072)
                                .AddRate("table.datashard.read.rows", 60300072)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 320036)
                                .AddRate("table.datashard.scan.rows", 300036)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                .AddRate("table.datashard.write.bytes", 120036)
                                .AddRate("table.datashard.write.rows", 100036)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "101")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                                        .AddRate("table.datashard.erase.bytes", 240017)
                                        .AddRate("table.datashard.erase.rows", 220017)
                                        .AddRate("table.datashard.read.bytes", 140380034)
                                        .AddRate("table.datashard.read.rows", 60300034)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320017)
                                        .AddRate("table.datashard.scan.rows", 300017)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 120017)
                                        .AddRate("table.datashard.write.rows", 100017)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                    .AddRate("table.datashard.cache_hit.bytes", 60017)
                                    .AddRate("table.datashard.cache_miss.bytes", 80017)
                                    .AddRate("table.datashard.consumed_cpu_us", 800017)
                                    .AddRate("table.datashard.erase.bytes", 240017)
                                    .AddRate("table.datashard.erase.rows", 220017)
                                    .AddRate("table.datashard.read.bytes", 140380034)
                                    .AddRate("table.datashard.read.rows", 60300034)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 320017)
                                    .AddRate("table.datashard.scan.rows", 300017)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 120017)
                                    .AddRate("table.datashard.write.rows", 100017)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                            .AddRate("table.datashard.bulk_upsert.rows", 260017)
                            .AddRate("table.datashard.cache_hit.bytes", 60017)
                            .AddRate("table.datashard.cache_miss.bytes", 80017)
                            .AddRate("table.datashard.consumed_cpu_us", 800017)
                            .AddRate("table.datashard.erase.bytes", 240017)
                            .AddRate("table.datashard.erase.rows", 220017)
                            .AddRate("table.datashard.read.bytes", 140380034)
                            .AddRate("table.datashard.read.rows", 60300034)
                            .AddGauge("table.datashard.row_count", 10016)
                            .AddRate("table.datashard.scan.bytes", 320017)
                            .AddRate("table.datashard.scan.rows", 300017)
                            .AddGauge("table.datashard.size_bytes", 20016)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                            .AddRate("table.datashard.write.bytes", 120017)
                            .AddRate("table.datashard.write.rows", 100017)
                        .EndNested()
                    .EndNested()
                    .BuildJson()
            )
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
            NormalizeJson(
                TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "202")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                            .AddRate("table.datashard.erase.bytes", 240010)
                                            .AddRate("table.datashard.erase.rows", 220010)
                                            .AddRate("table.datashard.read.bytes", 140380020)
                                            .AddRate("table.datashard.read.rows", 60300020)
                                            .AddGauge("table.datashard.row_count", 10008)
                                            .AddRate("table.datashard.scan.bytes", 320010)
                                            .AddRate("table.datashard.scan.rows", 300010)
                                            .AddGauge("table.datashard.size_bytes", 20008)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                            .AddRate("table.datashard.write.bytes", 120010)
                                            .AddRate("table.datashard.write.rows", 100010)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                .AddRate("table.datashard.cache_hit.bytes", 60010)
                                .AddRate("table.datashard.cache_miss.bytes", 80010)
                                .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                .AddRate("table.datashard.erase.bytes", 240010)
                                .AddRate("table.datashard.erase.rows", 220010)
                                .AddRate("table.datashard.read.bytes", 140380020)
                                .AddRate("table.datashard.read.rows", 60300020)
                                .AddGauge("table.datashard.row_count", 10008)
                                .AddRate("table.datashard.scan.bytes", 320010)
                                .AddRate("table.datashard.scan.rows", 300010)
                                .AddGauge("table.datashard.size_bytes", 20008)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 120010)
                                .AddRate("table.datashard.write.rows", 100010)
                            .EndNested()
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "303")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                            .AddRate("table.datashard.erase.bytes", 240036)
                                            .AddRate("table.datashard.erase.rows", 220036)
                                            .AddRate("table.datashard.read.bytes", 140380072)
                                            .AddRate("table.datashard.read.rows", 60300072)
                                            .AddGauge("table.datashard.row_count", 10032)
                                            .AddRate("table.datashard.scan.bytes", 320036)
                                            .AddRate("table.datashard.scan.rows", 300036)
                                            .AddGauge("table.datashard.size_bytes", 20032)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                            .AddRate("table.datashard.write.bytes", 120036)
                                            .AddRate("table.datashard.write.rows", 100036)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                .AddRate("table.datashard.cache_hit.bytes", 60036)
                                .AddRate("table.datashard.cache_miss.bytes", 80036)
                                .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                .AddRate("table.datashard.erase.bytes", 240036)
                                .AddRate("table.datashard.erase.rows", 220036)
                                .AddRate("table.datashard.read.bytes", 140380072)
                                .AddRate("table.datashard.read.rows", 60300072)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 320036)
                                .AddRate("table.datashard.scan.rows", 300036)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                .AddRate("table.datashard.write.bytes", 120036)
                                .AddRate("table.datashard.write.rows", 100036)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "101")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                                        .AddRate("table.datashard.erase.bytes", 240017)
                                        .AddRate("table.datashard.erase.rows", 220017)
                                        .AddRate("table.datashard.read.bytes", 140380034)
                                        .AddRate("table.datashard.read.rows", 60300034)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320017)
                                        .AddRate("table.datashard.scan.rows", 300017)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 120017)
                                        .AddRate("table.datashard.write.rows", 100017)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                    .AddRate("table.datashard.cache_hit.bytes", 60017)
                                    .AddRate("table.datashard.cache_miss.bytes", 80017)
                                    .AddRate("table.datashard.consumed_cpu_us", 800017)
                                    .AddRate("table.datashard.erase.bytes", 240017)
                                    .AddRate("table.datashard.erase.rows", 220017)
                                    .AddRate("table.datashard.read.bytes", 140380034)
                                    .AddRate("table.datashard.read.rows", 60300034)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 320017)
                                    .AddRate("table.datashard.scan.rows", 300017)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 120017)
                                    .AddRate("table.datashard.write.rows", 100017)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                            .AddRate("table.datashard.bulk_upsert.rows", 260017)
                            .AddRate("table.datashard.cache_hit.bytes", 60017)
                            .AddRate("table.datashard.cache_miss.bytes", 80017)
                            .AddRate("table.datashard.consumed_cpu_us", 800017)
                            .AddRate("table.datashard.erase.bytes", 240017)
                            .AddRate("table.datashard.erase.rows", 220017)
                            .AddRate("table.datashard.read.bytes", 140380034)
                            .AddRate("table.datashard.read.rows", 60300034)
                            .AddGauge("table.datashard.row_count", 10016)
                            .AddRate("table.datashard.scan.bytes", 320017)
                            .AddRate("table.datashard.scan.rows", 300017)
                            .AddGauge("table.datashard.size_bytes", 20016)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                            .AddRate("table.datashard.write.bytes", 120017)
                            .AddRate("table.datashard.write.rows", 100017)
                        .EndNested()
                    .EndNested()
                    .BuildJson()
            )
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
            NormalizeJson(
                TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "202")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                            .AddRate("table.datashard.erase.bytes", 240010)
                                            .AddRate("table.datashard.erase.rows", 220010)
                                            .AddRate("table.datashard.read.bytes", 140380020)
                                            .AddRate("table.datashard.read.rows", 60300020)
                                            .AddGauge("table.datashard.row_count", 10008)
                                            .AddRate("table.datashard.scan.bytes", 320010)
                                            .AddRate("table.datashard.scan.rows", 300010)
                                            .AddGauge("table.datashard.size_bytes", 20008)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                            .AddRate("table.datashard.write.bytes", 120010)
                                            .AddRate("table.datashard.write.rows", 100010)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                .AddRate("table.datashard.cache_hit.bytes", 60010)
                                .AddRate("table.datashard.cache_miss.bytes", 80010)
                                .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                .AddRate("table.datashard.erase.bytes", 240010)
                                .AddRate("table.datashard.erase.rows", 220010)
                                .AddRate("table.datashard.read.bytes", 140380020)
                                .AddRate("table.datashard.read.rows", 60300020)
                                .AddGauge("table.datashard.row_count", 10008)
                                .AddRate("table.datashard.scan.bytes", 320010)
                                .AddRate("table.datashard.scan.rows", 300010)
                                .AddGauge("table.datashard.size_bytes", 20008)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 120010)
                                .AddRate("table.datashard.write.rows", 100010)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "303")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                            .AddRate("table.datashard.erase.bytes", 240036)
                                            .AddRate("table.datashard.erase.rows", 220036)
                                            .AddRate("table.datashard.read.bytes", 140380072)
                                            .AddRate("table.datashard.read.rows", 60300072)
                                            .AddGauge("table.datashard.row_count", 10032)
                                            .AddRate("table.datashard.scan.bytes", 320036)
                                            .AddRate("table.datashard.scan.rows", 300036)
                                            .AddGauge("table.datashard.size_bytes", 20032)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                            .AddRate("table.datashard.write.bytes", 120036)
                                            .AddRate("table.datashard.write.rows", 100036)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                .AddRate("table.datashard.cache_hit.bytes", 60036)
                                .AddRate("table.datashard.cache_miss.bytes", 80036)
                                .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                .AddRate("table.datashard.erase.bytes", 240036)
                                .AddRate("table.datashard.erase.rows", 220036)
                                .AddRate("table.datashard.read.bytes", 140380072)
                                .AddRate("table.datashard.read.rows", 60300072)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 320036)
                                .AddRate("table.datashard.scan.rows", 300036)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                .AddRate("table.datashard.write.bytes", 120036)
                                .AddRate("table.datashard.write.rows", 100036)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "101")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                                        .AddRate("table.datashard.erase.bytes", 240017)
                                        .AddRate("table.datashard.erase.rows", 220017)
                                        .AddRate("table.datashard.read.bytes", 140380034)
                                        .AddRate("table.datashard.read.rows", 60300034)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320017)
                                        .AddRate("table.datashard.scan.rows", 300017)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 120017)
                                        .AddRate("table.datashard.write.rows", 100017)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                    .AddRate("table.datashard.cache_hit.bytes", 60017)
                                    .AddRate("table.datashard.cache_miss.bytes", 80017)
                                    .AddRate("table.datashard.consumed_cpu_us", 800017)
                                    .AddRate("table.datashard.erase.bytes", 240017)
                                    .AddRate("table.datashard.erase.rows", 220017)
                                    .AddRate("table.datashard.read.bytes", 140380034)
                                    .AddRate("table.datashard.read.rows", 60300034)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 320017)
                                    .AddRate("table.datashard.scan.rows", 300017)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 120017)
                                    .AddRate("table.datashard.write.rows", 100017)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                            .AddRate("table.datashard.bulk_upsert.rows", 260017)
                            .AddRate("table.datashard.cache_hit.bytes", 60017)
                            .AddRate("table.datashard.cache_miss.bytes", 80017)
                            .AddRate("table.datashard.consumed_cpu_us", 800017)
                            .AddRate("table.datashard.erase.bytes", 240017)
                            .AddRate("table.datashard.erase.rows", 220017)
                            .AddRate("table.datashard.read.bytes", 140380034)
                            .AddRate("table.datashard.read.rows", 60300034)
                            .AddGauge("table.datashard.row_count", 10016)
                            .AddRate("table.datashard.scan.bytes", 320017)
                            .AddRate("table.datashard.scan.rows", 300017)
                            .AddGauge("table.datashard.size_bytes", 20016)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                            .AddRate("table.datashard.write.bytes", 120017)
                            .AddRate("table.datashard.write.rows", 100017)
                        .EndNested()
                    .EndNested()
                    .BuildJson()
            )
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
            NormalizeJson(
                TSensorsJsonBuilder::Start()
                    .StartNested("database", "1113-1001")
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-2")
                            .StartNested("table", "/Root/fake-db/fake-table2")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "202")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                            .AddRate("table.datashard.cache_hit.bytes", 60010)
                                            .AddRate("table.datashard.cache_miss.bytes", 80010)
                                            .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                            .AddRate("table.datashard.erase.bytes", 240010)
                                            .AddRate("table.datashard.erase.rows", 220010)
                                            .AddRate("table.datashard.read.bytes", 140380020)
                                            .AddRate("table.datashard.read.rows", 60300020)
                                            .AddGauge("table.datashard.row_count", 10008)
                                            .AddRate("table.datashard.scan.bytes", 320010)
                                            .AddRate("table.datashard.scan.rows", 300010)
                                            .AddGauge("table.datashard.size_bytes", 20008)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                            .AddRate("table.datashard.write.bytes", 120010)
                                            .AddRate("table.datashard.write.rows", 100010)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                        .AddRate("table.datashard.cache_hit.bytes", 60010)
                                        .AddRate("table.datashard.cache_miss.bytes", 80010)
                                        .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                        .AddRate("table.datashard.erase.bytes", 240010)
                                        .AddRate("table.datashard.erase.rows", 220010)
                                        .AddRate("table.datashard.read.bytes", 140380020)
                                        .AddRate("table.datashard.read.rows", 60300020)
                                        .AddGauge("table.datashard.row_count", 10008)
                                        .AddRate("table.datashard.scan.bytes", 320010)
                                        .AddRate("table.datashard.scan.rows", 300010)
                                        .AddGauge("table.datashard.size_bytes", 20008)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                        .AddRate("table.datashard.write.bytes", 120010)
                                        .AddRate("table.datashard.write.rows", 100010)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280010)
                                .AddRate("table.datashard.bulk_upsert.rows", 260010)
                                .AddRate("table.datashard.cache_hit.bytes", 60010)
                                .AddRate("table.datashard.cache_miss.bytes", 80010)
                                .AddRate("table.datashard.consumed_cpu_us", 1000010)
                                .AddRate("table.datashard.erase.bytes", 240010)
                                .AddRate("table.datashard.erase.rows", 220010)
                                .AddRate("table.datashard.read.bytes", 140380020)
                                .AddRate("table.datashard.read.rows", 60300020)
                                .AddGauge("table.datashard.row_count", 10008)
                                .AddRate("table.datashard.scan.bytes", 320010)
                                .AddRate("table.datashard.scan.rows", 300010)
                                .AddGauge("table.datashard.size_bytes", 20008)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{90, 1}})
                                .AddRate("table.datashard.write.bytes", 120010)
                                .AddRate("table.datashard.write.rows", 100010)
                            .EndNested()
                        .EndNested()
                        .StartNested("monitoring_project_id", "fake-monitoring-project-id-3")
                            .StartNested("table", "/Root/fake-db/fake-table3")
                                .StartNested("detailed_metrics", "per_partition")
                                    .StartNested("tablet_id", "303")
                                        .StartNested("follower_id", "0")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                            .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                            .AddRate("table.datashard.cache_hit.bytes", 60036)
                                            .AddRate("table.datashard.cache_miss.bytes", 80036)
                                            .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                            .AddRate("table.datashard.erase.bytes", 240036)
                                            .AddRate("table.datashard.erase.rows", 220036)
                                            .AddRate("table.datashard.read.bytes", 140380072)
                                            .AddRate("table.datashard.read.rows", 60300072)
                                            .AddGauge("table.datashard.row_count", 10032)
                                            .AddRate("table.datashard.scan.bytes", 320036)
                                            .AddRate("table.datashard.scan.rows", 300036)
                                            .AddGauge("table.datashard.size_bytes", 20032)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                            .AddRate("table.datashard.write.bytes", 120036)
                                            .AddRate("table.datashard.write.rows", 100036)
                                        .EndNested()
                                        .StartNested("follower_id", "replicas_only")
                                            .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                            .AddRate("table.datashard.bulk_upsert.rows", 0)
                                            .AddRate("table.datashard.cache_hit.bytes", 0)
                                            .AddRate("table.datashard.cache_miss.bytes", 0)
                                            .AddRate("table.datashard.consumed_cpu_us", 0)
                                            .AddRate("table.datashard.erase.bytes", 0)
                                            .AddRate("table.datashard.erase.rows", 0)
                                            .AddRate("table.datashard.read.bytes", 0)
                                            .AddRate("table.datashard.read.rows", 0)
                                            .AddGauge("table.datashard.row_count", 0)
                                            .AddRate("table.datashard.scan.bytes", 0)
                                            .AddRate("table.datashard.scan.rows", 0)
                                            .AddGauge("table.datashard.size_bytes", 0)
                                            .AddCpuHistogram("table.datashard.used_core_percents", {})
                                            .AddRate("table.datashard.write.bytes", 0)
                                            .AddRate("table.datashard.write.rows", 0)
                                        .EndNested()
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                        .AddRate("table.datashard.cache_hit.bytes", 60036)
                                        .AddRate("table.datashard.cache_miss.bytes", 80036)
                                        .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                        .AddRate("table.datashard.erase.bytes", 240036)
                                        .AddRate("table.datashard.erase.rows", 220036)
                                        .AddRate("table.datashard.read.bytes", 140380072)
                                        .AddRate("table.datashard.read.rows", 60300072)
                                        .AddGauge("table.datashard.row_count", 10032)
                                        .AddRate("table.datashard.scan.bytes", 320036)
                                        .AddRate("table.datashard.scan.rows", 300036)
                                        .AddGauge("table.datashard.size_bytes", 20032)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                        .AddRate("table.datashard.write.bytes", 120036)
                                        .AddRate("table.datashard.write.rows", 100036)
                                    .EndNested()
                                .EndNested()
                                .AddRate("table.datashard.bulk_upsert.bytes", 280036)
                                .AddRate("table.datashard.bulk_upsert.rows", 260036)
                                .AddRate("table.datashard.cache_hit.bytes", 60036)
                                .AddRate("table.datashard.cache_miss.bytes", 80036)
                                .AddRate("table.datashard.consumed_cpu_us", 1200036)
                                .AddRate("table.datashard.erase.bytes", 240036)
                                .AddRate("table.datashard.erase.rows", 220036)
                                .AddRate("table.datashard.read.bytes", 140380072)
                                .AddRate("table.datashard.read.rows", 60300072)
                                .AddGauge("table.datashard.row_count", 10032)
                                .AddRate("table.datashard.scan.bytes", 320036)
                                .AddRate("table.datashard.scan.rows", 300036)
                                .AddGauge("table.datashard.size_bytes", 20032)
                                .AddCpuHistogram("table.datashard.used_core_percents", {{100, 1}})
                                .AddRate("table.datashard.write.bytes", 120036)
                                .AddRate("table.datashard.write.rows", 100036)
                            .EndNested()
                        .EndNested()
                        .StartNested("table", "/Root/fake-db/fake-table1")
                            .StartNested("detailed_metrics", "per_partition")
                                .StartNested("tablet_id", "101")
                                    .StartNested("follower_id", "0")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                        .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                        .AddRate("table.datashard.cache_hit.bytes", 60017)
                                        .AddRate("table.datashard.cache_miss.bytes", 80017)
                                        .AddRate("table.datashard.consumed_cpu_us", 800017)
                                        .AddRate("table.datashard.erase.bytes", 240017)
                                        .AddRate("table.datashard.erase.rows", 220017)
                                        .AddRate("table.datashard.read.bytes", 140380034)
                                        .AddRate("table.datashard.read.rows", 60300034)
                                        .AddGauge("table.datashard.row_count", 10016)
                                        .AddRate("table.datashard.scan.bytes", 320017)
                                        .AddRate("table.datashard.scan.rows", 300017)
                                        .AddGauge("table.datashard.size_bytes", 20016)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                        .AddRate("table.datashard.write.bytes", 120017)
                                        .AddRate("table.datashard.write.rows", 100017)
                                    .EndNested()
                                    .StartNested("follower_id", "replicas_only")
                                        .AddRate("table.datashard.bulk_upsert.bytes", 0)
                                        .AddRate("table.datashard.bulk_upsert.rows", 0)
                                        .AddRate("table.datashard.cache_hit.bytes", 0)
                                        .AddRate("table.datashard.cache_miss.bytes", 0)
                                        .AddRate("table.datashard.consumed_cpu_us", 0)
                                        .AddRate("table.datashard.erase.bytes", 0)
                                        .AddRate("table.datashard.erase.rows", 0)
                                        .AddRate("table.datashard.read.bytes", 0)
                                        .AddRate("table.datashard.read.rows", 0)
                                        .AddGauge("table.datashard.row_count", 0)
                                        .AddRate("table.datashard.scan.bytes", 0)
                                        .AddRate("table.datashard.scan.rows", 0)
                                        .AddGauge("table.datashard.size_bytes", 0)
                                        .AddCpuHistogram("table.datashard.used_core_percents", {})
                                        .AddRate("table.datashard.write.bytes", 0)
                                        .AddRate("table.datashard.write.rows", 0)
                                    .EndNested()
                                    .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                                    .AddRate("table.datashard.bulk_upsert.rows", 260017)
                                    .AddRate("table.datashard.cache_hit.bytes", 60017)
                                    .AddRate("table.datashard.cache_miss.bytes", 80017)
                                    .AddRate("table.datashard.consumed_cpu_us", 800017)
                                    .AddRate("table.datashard.erase.bytes", 240017)
                                    .AddRate("table.datashard.erase.rows", 220017)
                                    .AddRate("table.datashard.read.bytes", 140380034)
                                    .AddRate("table.datashard.read.rows", 60300034)
                                    .AddGauge("table.datashard.row_count", 10016)
                                    .AddRate("table.datashard.scan.bytes", 320017)
                                    .AddRate("table.datashard.scan.rows", 300017)
                                    .AddGauge("table.datashard.size_bytes", 20016)
                                    .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                                    .AddRate("table.datashard.write.bytes", 120017)
                                    .AddRate("table.datashard.write.rows", 100017)
                                .EndNested()
                            .EndNested()
                            .AddRate("table.datashard.bulk_upsert.bytes", 280017)
                            .AddRate("table.datashard.bulk_upsert.rows", 260017)
                            .AddRate("table.datashard.cache_hit.bytes", 60017)
                            .AddRate("table.datashard.cache_miss.bytes", 80017)
                            .AddRate("table.datashard.consumed_cpu_us", 800017)
                            .AddRate("table.datashard.erase.bytes", 240017)
                            .AddRate("table.datashard.erase.rows", 220017)
                            .AddRate("table.datashard.read.bytes", 140380034)
                            .AddRate("table.datashard.read.rows", 60300034)
                            .AddGauge("table.datashard.row_count", 10016)
                            .AddRate("table.datashard.scan.bytes", 320017)
                            .AddRate("table.datashard.scan.rows", 300017)
                            .AddGauge("table.datashard.size_bytes", 20016)
                            .AddCpuHistogram("table.datashard.used_core_percents", {{80, 1}})
                            .AddRate("table.datashard.write.bytes", 120017)
                            .AddRate("table.datashard.write.rows", 100017)
                        .EndNested()
                    .EndNested()
                    .BuildJson()
            )
        );
    }
}
