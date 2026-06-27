#include "ydb_metrics_aggregator.h"
#include "ut_helpers.h"
#include "ut_sensors_json_builder.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;

namespace {

/**
 * Create Data Shard public counters and populate them with fake values.
 *
 * @param[in] runtime The test runtime
 * @param[in] sourceGroupName The name of the counter group for the new counters
 * @param[in] offset The offset for the counter values
 *
 * @return The counters group created for these public counters
 */
std::pair<NMonitoring::TDynamicCounterPtr, TExpectedSensorGroup> PopulateDataShardPublicCounters(
    TTestBasicRuntime& runtime,
    const TString& sourceGroupName,
    ui32 offset
) {
    TExpectedSensorGroup expectedMetrics;

    auto sourceCountersGroup = runtime.GetAppData(0).Counters->GetSubgroup(
        "counters",
        sourceGroupName
    );

    // Simple counters
    const auto setSimpleCounterValue = [&](const char* name, ui64 value) {
        auto v = offset * 1000 + value;
        expectedMetrics.Add(name, TTabletSimpleCounter().Set(v));
        sourceCountersGroup->GetNamedCounter(
            "name",
            name,
            false /* derivative */
        )->Set(v);
    };

    setSimpleCounterValue("table.datashard.row_count",  1);
    setSimpleCounterValue("table.datashard.size_bytes", 2);

    // Cumulative counters
    const auto setCumulativeCounterValue = [&](const char* name, ui64 value) {
        auto v = offset * 1000 + value;
        expectedMetrics.Add(name, TTabletCumulativeCounter().Increment(v));
        sourceCountersGroup->GetNamedCounter(
            "name",
            name,
            true /* derivative */
        )->Set(v);
    };

    setCumulativeCounterValue("table.datashard.write.rows",         3);
    setCumulativeCounterValue("table.datashard.write.bytes",        4);
    setCumulativeCounterValue("table.datashard.read.rows",          5);
    setCumulativeCounterValue("table.datashard.read.bytes",         6);
    setCumulativeCounterValue("table.datashard.erase.rows",         7);
    setCumulativeCounterValue("table.datashard.erase.bytes",        8);
    setCumulativeCounterValue("table.datashard.bulk_upsert.rows",   9);
    setCumulativeCounterValue("table.datashard.bulk_upsert.bytes", 10);
    setCumulativeCounterValue("table.datashard.scan.rows",         11);
    setCumulativeCounterValue("table.datashard.scan.bytes",        12);
    setCumulativeCounterValue("table.datashard.cache_hit.bytes",   13);
    setCumulativeCounterValue("table.datashard.cache_miss.bytes",  14);
    setCumulativeCounterValue("table.datashard.consumed_cpu_us",   15);

    // Percentile counters
    const auto cpuHistogram = sourceCountersGroup->GetNamedHistogram(
        "name",
        "table.datashard.used_core_percents",
        NMonitoring::ExplicitHistogram({
            0,
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
        }),
        false /* derivative */
    );

    // NOTE: This is needed to allow PopulateDataShardPublicCounters() to be called
    //       multiple times to update counters for an existing source group
    cpuHistogram->Reset();

    cpuHistogram->Collect(static_cast<i64>(  0), offset * 1000 +  1);
    cpuHistogram->Collect(static_cast<i64>( 10), offset * 1000 +  2);
    cpuHistogram->Collect(static_cast<i64>( 20), offset * 1000 +  3);
    cpuHistogram->Collect(static_cast<i64>( 30), offset * 1000 +  4);
    cpuHistogram->Collect(static_cast<i64>( 40), offset * 1000 +  5);
    cpuHistogram->Collect(static_cast<i64>( 50), offset * 1000 +  6);
    cpuHistogram->Collect(static_cast<i64>( 60), offset * 1000 +  7);
    cpuHistogram->Collect(static_cast<i64>( 70), offset * 1000 +  8);
    cpuHistogram->Collect(static_cast<i64>( 80), offset * 1000 +  9);
    cpuHistogram->Collect(static_cast<i64>( 90), offset * 1000 + 10);
    cpuHistogram->Collect(static_cast<i64>(100), offset * 1000 + 11);
    cpuHistogram->Collect(static_cast<i64>(101), offset * 1000 + 12);

    std::unordered_map<ui64, ui64> cpuBuckets;
    for (ui64 i = 0; i < 11; i++) { cpuBuckets[i * 10] = offset * 1000 + i + 1; }
    expectedMetrics.AddHist("table.datashard.used_core_percents", cpuBuckets, offset * 1000 + 12);

    return std::make_pair(sourceCountersGroup, expectedMetrics);
}

} // namespace <anonymous>

/**
 * Unit tests for the YDB metrics aggregator class (TYdbMetricsAggregator).
 */
Y_UNIT_TEST_SUITE(TYdbMetricsAggregatorTest) {
    /**
     * Verify that the YDB metrics aggregator correctly aggregates metrics
     * of all types and handles adding/removing source groups.
     *
     * @note This test verifies only those YDB public metrics,
     *       which are exposed by Data Shard.
     */
    Y_UNIT_TEST(DataShard) {
        TTestBasicRuntime runtime(1);
        runtime.Initialize(TAppPrepare().Unwrap());

        // Create 3 sets of source counters
        auto [sourceCountersGroup1, expectedMetrics1] = PopulateDataShardPublicCounters(runtime, "source-group-1", 1);
        auto [sourceCountersGroup2, expectedMetrics2] = PopulateDataShardPublicCounters(runtime, "source-group-2", 20);
        auto [sourceCountersGroup3, expectedMetrics3] = PopulateDataShardPublicCounters(runtime, "source-group-3", 300);

        // TEST 1: Create the public metrics aggregator without any source groups:
        //         at this point the target values should be all zeros
        const auto targetCountersGroup = runtime.GetAppData(0).Counters->GetSubgroup(
            "counters",
            "target-group"
        );

        auto aggregator = CreateYdbMetricsAggregatorByTabletType(
            TTabletTypes::DataShard,
            targetCountersGroup
        );

        TString countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (initial):" << Endl << countersJson << Endl;

        TString expectedJsonAllZeros = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGroup(EmptyDataShardMetrics())
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJsonAllZeros,
            "Expected JSON (initial):" << Endl << expectedJsonAllZeros
        );

        // TEST 2: Add first two source groups and make sure the target counters are updated
        aggregator->AddSourceCountersGroup("source-group-1", sourceCountersGroup1);
        aggregator->AddSourceCountersGroup("source-group-2", sourceCountersGroup2);
        aggregator->RecalculateAllTargetCounters();

        countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (2 source groups added):" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGroup(expectedMetrics1 | expectedMetrics2)
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON (2 source groups added):" << Endl << expectedJson
        );

        // TEST 3: Add the third source groups and make sure the target counters are updated
        aggregator->AddSourceCountersGroup("source-group-3", sourceCountersGroup3);
        aggregator->RecalculateAllTargetCounters();

        countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (all source groups added):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGroup(expectedMetrics1 | expectedMetrics2 | expectedMetrics3)
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON (all source groups added):" << Endl << expectedJson
        );

        // TEST 4: Update the first source group and make sure the target counters are updated
        std::tie(sourceCountersGroup1, expectedMetrics1) = PopulateDataShardPublicCounters(runtime, "source-group-1", 1000);
        
        aggregator->RecalculateAllTargetCounters();

        countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (the first group updated):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGroup(expectedMetrics1 | expectedMetrics2 | expectedMetrics3)
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON (the first group updated):" << Endl << expectedJson
        );

        // TEST 5: Remove the first source group and make sure the target counters are updated
        aggregator->RemoveSourceCountersGroup("source-group-1");
        aggregator->RecalculateAllTargetCounters();

        countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (the first source group removed):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGroup(expectedMetrics2 | expectedMetrics3)
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON (the first source group removed):" << Endl << expectedJson
        );

        // TEST 6: Remove all remaining source group and make sure the target counters are all zeros
        aggregator->RemoveSourceCountersGroup("source-group-2");
        aggregator->RemoveSourceCountersGroup("source-group-3");
        aggregator->RecalculateAllTargetCounters();

        countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (all source group removed):" << Endl << countersJson << Endl;

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJsonAllZeros,
            "Expected JSON (all source group removed):" << Endl << expectedJsonAllZeros
        );
    }
}
