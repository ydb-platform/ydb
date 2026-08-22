#include "ydb_metrics_aggregator.h"
#include "ut_helpers.h"

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
R"json(
{
  "sensors": [
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_hit.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_miss.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.consumed_cpu_us"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.rows"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.row_count"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.rows"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.size_bytes"
      },
      "value": 0
    },
    {
      "hist": {
        "bounds": [
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
          100
        ],
        "buckets": [
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0
        ],
        "inf": 0
      },
      "kind": "HIST",
      "labels": {
        "name": "table.datashard.used_core_percents"
      }
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.rows"
      },
      "value": 0
    }
  ]
}
)json"
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
R"json(
{
  "sensors": [
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.bytes"
      },
      "value": 21020
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.rows"
      },
      "value": 21018
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_hit.bytes"
      },
      "value": 21026
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_miss.bytes"
      },
      "value": 21028
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.consumed_cpu_us"
      },
      "value": 21030
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.bytes"
      },
      "value": 21016
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.rows"
      },
      "value": 21014
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.bytes"
      },
      "value": 21012
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.rows"
      },
      "value": 21010
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.row_count"
      },
      "value": 21002
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.bytes"
      },
      "value": 21024
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.rows"
      },
      "value": 21022
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.size_bytes"
      },
      "value": 21004
    },
    {
      "hist": {
        "bounds": [
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
          100
        ],
        "buckets": [
          21002,
          21004,
          21006,
          21008,
          21010,
          21012,
          21014,
          21016,
          21018,
          21020,
          21022
        ],
        "inf": 21024
      },
      "kind": "HIST",
      "labels": {
        "name": "table.datashard.used_core_percents"
      }
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.bytes"
      },
      "value": 21008
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.rows"
      },
      "value": 21006
    }
  ]
}
)json"
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
R"json(
{
  "sensors": [
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.bytes"
      },
      "value": 321030
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.rows"
      },
      "value": 321027
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_hit.bytes"
      },
      "value": 321039
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_miss.bytes"
      },
      "value": 321042
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.consumed_cpu_us"
      },
      "value": 321045
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.bytes"
      },
      "value": 321024
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.rows"
      },
      "value": 321021
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.bytes"
      },
      "value": 321018
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.rows"
      },
      "value": 321015
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.row_count"
      },
      "value": 321003
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.bytes"
      },
      "value": 321036
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.rows"
      },
      "value": 321033
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.size_bytes"
      },
      "value": 321006
    },
    {
      "hist": {
        "bounds": [
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
          100
        ],
        "buckets": [
          321003,
          321006,
          321009,
          321012,
          321015,
          321018,
          321021,
          321024,
          321027,
          321030,
          321033
        ],
        "inf": 321036
      },
      "kind": "HIST",
      "labels": {
        "name": "table.datashard.used_core_percents"
      }
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.bytes"
      },
      "value": 321012
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.rows"
      },
      "value": 321009
    }
  ]
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON (all source groups added):" << Endl << expectedJson
        );

        // TEST 4: Update the first source groups and make sure the target counters are updated
        PopulateDataShardPublicCounters(runtime, "source-group-1", 1000);
        aggregator->RecalculateAllTargetCounters();

        countersJson = NormalizeJson(NMonitoring::ToJson(*targetCountersGroup));
        Cerr << "TEST Target counters (the first group updated):" << Endl << countersJson << Endl;

        expectedJson = NormalizeJson(
R"json(
{
  "sensors": [
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.bytes"
      },
      "value": 1320030
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.rows"
      },
      "value": 1320027
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_hit.bytes"
      },
      "value": 1320039
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_miss.bytes"
      },
      "value": 1320042
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.consumed_cpu_us"
      },
      "value": 1320045
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.bytes"
      },
      "value": 1320024
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.rows"
      },
      "value": 1320021
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.bytes"
      },
      "value": 1320018
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.rows"
      },
      "value": 1320015
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.row_count"
      },
      "value": 1320003
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.bytes"
      },
      "value": 1320036
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.rows"
      },
      "value": 1320033
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.size_bytes"
      },
      "value": 1320006
    },
    {
      "hist": {
        "bounds": [
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
          100
        ],
        "buckets": [
          1320003,
          1320006,
          1320009,
          1320012,
          1320015,
          1320018,
          1320021,
          1320024,
          1320027,
          1320030,
          1320033
        ],
        "inf": 1320036
      },
      "kind": "HIST",
      "labels": {
        "name": "table.datashard.used_core_percents"
      }
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.bytes"
      },
      "value": 1320012
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.rows"
      },
      "value": 1320009
    }
  ]
}
)json"
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
R"json(
{
  "sensors": [
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.bytes"
      },
      "value": 320020
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.rows"
      },
      "value": 320018
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_hit.bytes"
      },
      "value": 320026
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_miss.bytes"
      },
      "value": 320028
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.consumed_cpu_us"
      },
      "value": 320030
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.bytes"
      },
      "value": 320016
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.rows"
      },
      "value": 320014
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.bytes"
      },
      "value": 320012
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.rows"
      },
      "value": 320010
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.row_count"
      },
      "value": 320002
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.bytes"
      },
      "value": 320024
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.rows"
      },
      "value": 320022
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.size_bytes"
      },
      "value": 320004
    },
    {
      "hist": {
        "bounds": [
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
          100
        ],
        "buckets": [
          320002,
          320004,
          320006,
          320008,
          320010,
          320012,
          320014,
          320016,
          320018,
          320020,
          320022
        ],
        "inf": 320024
      },
      "kind": "HIST",
      "labels": {
        "name": "table.datashard.used_core_percents"
      }
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.bytes"
      },
      "value": 320008
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.rows"
      },
      "value": 320006
    }
  ]
}
)json"
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
