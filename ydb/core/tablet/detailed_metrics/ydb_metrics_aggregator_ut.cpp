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
NMonitoring::TDynamicCounterPtr PopulateDataShardPublicCounters(
    TTestBasicRuntime& runtime,
    const TString& sourceGroupName,
    ui32 offset
) {
    auto sourceCountersGroup = runtime.GetAppData(0).Counters->GetSubgroup(
        "counters",
        sourceGroupName
    );

    // Simple counters
    const auto setSimpleCounterValue = [&](const char* name, ui64 value) {
        sourceCountersGroup->GetNamedCounter(
            "name",
            name,
            false /* derivative */
        )->Set(offset * 1000 + value);
    };

    setSimpleCounterValue("table.datashard.row_count",  1);
    setSimpleCounterValue("table.datashard.size_bytes", 2);

    // Cumulative counters
    const auto setCumulativeCounterValue = [&](const char* name, ui64 value) {
        sourceCountersGroup->GetNamedCounter(
            "name",
            name,
            true /* derivative */
        )->Set(offset * 1000 + value);
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

    return sourceCountersGroup;
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
        auto sourceCountersGroup1 = PopulateDataShardPublicCounters(runtime, "source-group-1", 1);
        auto sourceCountersGroup2 = PopulateDataShardPublicCounters(runtime, "source-group-2", 20);
        auto sourceCountersGroup3 = PopulateDataShardPublicCounters(runtime, "source-group-3", 300);

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
                .AddRate("table.datashard.bulk_upsert.bytes", 21020)
                .AddRate("table.datashard.bulk_upsert.rows", 21018)
                .AddRate("table.datashard.cache_hit.bytes", 21026)
                .AddRate("table.datashard.cache_miss.bytes", 21028)
                .AddRate("table.datashard.consumed_cpu_us", 21030)
                .AddRate("table.datashard.erase.bytes", 21016)
                .AddRate("table.datashard.erase.rows", 21014)
                .AddRate("table.datashard.read.bytes", 21012)
                .AddRate("table.datashard.read.rows", 21010)
                .AddGauge("table.datashard.row_count", 21002)
                .AddRate("table.datashard.scan.bytes", 21024)
                .AddRate("table.datashard.scan.rows", 21022)
                .AddGauge("table.datashard.size_bytes", 21004)
                .AddCpuHistogram(
                    "table.datashard.used_core_percents",
                    {
                      {0,   21002},
                      {10,  21004},
                      {20,  21006},
                      {30,  21008},
                      {40,  21010},
                      {50,  21012},
                      {60,  21014},
                      {70,  21016},
                      {80,  21018},
                      {90,  21020},
                      {100, 21022},
                    },
                    21024
                )
                .AddRate("table.datashard.write.bytes", 21008)
                .AddRate("table.datashard.write.rows", 21006)
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
                .AddRate("table.datashard.bulk_upsert.bytes", 321030)
                .AddRate("table.datashard.bulk_upsert.rows", 321027)
                .AddRate("table.datashard.cache_hit.bytes", 321039)
                .AddRate("table.datashard.cache_miss.bytes", 321042)
                .AddRate("table.datashard.consumed_cpu_us", 321045)
                .AddRate("table.datashard.erase.bytes", 321024)
                .AddRate("table.datashard.erase.rows", 321021)
                .AddRate("table.datashard.read.bytes", 321018)
                .AddRate("table.datashard.read.rows", 321015)
                .AddGauge("table.datashard.row_count", 321003)
                .AddRate("table.datashard.scan.bytes", 321036)
                .AddRate("table.datashard.scan.rows", 321033)
                .AddGauge("table.datashard.size_bytes", 321006)
                .AddCpuHistogram(
                    "table.datashard.used_core_percents",
                    {
                      {0,   321003},
                      {10,  321006},
                      {20,  321009},
                      {30,  321012},
                      {40,  321015},
                      {50,  321018},
                      {60,  321021},
                      {70,  321024},
                      {80,  321027},
                      {90,  321030},
                      {100, 321033},
                    },
                    321036
                )
                .AddRate("table.datashard.write.bytes", 321012)
                .AddRate("table.datashard.write.rows", 321009)
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON (all source groups added):" << Endl << expectedJson
        );

        // TEST 4: Update the first source group and make sure the target counters are updated
        PopulateDataShardPublicCounters(runtime, "source-group-1", 1000);
        aggregator->RecalculateAllTargetCounters();

        countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (the first group updated):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddRate("table.datashard.bulk_upsert.bytes", 1320030)
                .AddRate("table.datashard.bulk_upsert.rows", 1320027)
                .AddRate("table.datashard.cache_hit.bytes", 1320039)
                .AddRate("table.datashard.cache_miss.bytes", 1320042)
                .AddRate("table.datashard.consumed_cpu_us", 1320045)
                .AddRate("table.datashard.erase.bytes", 1320024)
                .AddRate("table.datashard.erase.rows", 1320021)
                .AddRate("table.datashard.read.bytes", 1320018)
                .AddRate("table.datashard.read.rows", 1320015)
                .AddGauge("table.datashard.row_count", 1320003)
                .AddRate("table.datashard.scan.bytes", 1320036)
                .AddRate("table.datashard.scan.rows", 1320033)
                .AddGauge("table.datashard.size_bytes", 1320006)
                .AddCpuHistogram(
                    "table.datashard.used_core_percents",
                    {
                      {0,   1320003},
                      {10,  1320006},
                      {20,  1320009},
                      {30,  1320012},
                      {40,  1320015},
                      {50,  1320018},
                      {60,  1320021},
                      {70,  1320024},
                      {80,  1320027},
                      {90,  1320030},
                      {100, 1320033},
                    },
                    1320036
                )
                .AddRate("table.datashard.write.bytes", 1320012)
                .AddRate("table.datashard.write.rows", 1320009)
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
                .AddRate("table.datashard.bulk_upsert.bytes", 320020)
                .AddRate("table.datashard.bulk_upsert.rows", 320018)
                .AddRate("table.datashard.cache_hit.bytes", 320026)
                .AddRate("table.datashard.cache_miss.bytes", 320028)
                .AddRate("table.datashard.consumed_cpu_us", 320030)
                .AddRate("table.datashard.erase.bytes", 320016)
                .AddRate("table.datashard.erase.rows", 320014)
                .AddRate("table.datashard.read.bytes", 320012)
                .AddRate("table.datashard.read.rows", 320010)
                .AddGauge("table.datashard.row_count", 320002)
                .AddRate("table.datashard.scan.bytes", 320024)
                .AddRate("table.datashard.scan.rows", 320022)
                .AddGauge("table.datashard.size_bytes", 320004)
                .AddCpuHistogram(
                    "table.datashard.used_core_percents",
                    {
                      {0,   320002},
                      {10,  320004},
                      {20,  320006},
                      {30,  320008},
                      {40,  320010},
                      {50,  320012},
                      {60,  320014},
                      {70,  320016},
                      {80,  320018},
                      {90,  320020},
                      {100, 320022},
                    },
                    320024
                )
                .AddRate("table.datashard.write.bytes", 320008)
                .AddRate("table.datashard.write.rows", 320006)
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
