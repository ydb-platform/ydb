#include "ut_helpers.h"

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
 * Create and initialize the Tablet Counters Aggregator actor.
 *
 * @param[in] runtime The test runtime
 *
 * @return The ID of the Tablet Counters Aggregator actor
 */
TActorId InitializeTabletCountersAggregator(TTestBasicRuntime& runtime) {
    // Register the Tablet Counters Aggregator actor
    TActorId aggregatorId = runtime.Register(CreateTabletCountersAggregator(false /* follower */));
    runtime.EnableScheduleForActor(aggregatorId);

    // Wait for the TEvBootstrap event to be processed, after this the actor is ready
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);

    runtime.DispatchEvents(options);
    return aggregatorId;
}

/**
 * Force the Tablet Counters Aggregator actor to recalculate and update all YDB metrics.
 *
 * @param[in] runtime The test runtime
 * @param[in] aggregatorId The ID of the Tablet Counters Aggregator actor
 * @param[in] edgeActorId The ID of the edge actor used for sending all messages
 */
void ForceCounterRecalculation(
    TTestBasicRuntime& runtime,
    const TActorId& aggregatorId,
    const TActorId& edgeActorId
) {
    // The aggregator uses TEvWakeup for recalculating all metrics,
    // but it only happens every 4 seconds. Sending this event explicitly
    // will force an immediate recalculation
    runtime.Send(
        new IEventHandle(
            aggregatorId,
            edgeActorId,
            new NActors::TEvents::TEvWakeup()
        )
    );
}

/**
 * Send low level metrics to the Tablet Counters Aggregator for the given DataShard tablet.
 *
 * @param[in] runtime The test runtime
 * @param[in] aggregatorId The ID of the Tablet Counters Aggregator actor
 * @param[in] edgeActorId The ID of the edge actor used for sending all messages
 * @param[in] tabletId The ID of the DataShard tablet
 * @param[in] cpuLoadPercentage The CPU load percentage to report for this tablet
 */
void SendDataShardMetrics(
    TTestBasicRuntime& runtime,
    const TActorId& aggregatorId,
    const TActorId& edgeActorId,
    ui64 tabletId,
    ui32 cpuLoadPercentage
) {
    // Populate executor counters with some fake values
    auto executorCounters = MakeHolder<TExecutorCounters>();
    auto executorCountersBaseline = MakeHolder<TExecutorCounters>();

    executorCounters->RememberCurrentStateAsBaseline(*executorCountersBaseline);

    executorCounters->Simple()[TExecutorCounters::DB_UNIQUE_ROWS_TOTAL] = 1000 + tabletId;
    executorCounters->Simple()[TExecutorCounters::DB_UNIQUE_DATA_BYTES] = 2000 + tabletId;

    executorCounters->Cumulative()[TExecutorCounters::TX_BYTES_CACHED].Increment(3000 + tabletId);
    executorCounters->Cumulative()[TExecutorCounters::TX_BYTES_READ].Increment(4000 + tabletId);

    // Populate DataShard counters with some fake values
    auto tabletCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
    auto tabletCountersBaseline = CreateAppCountersByTabletType(TTabletTypes::DataShard);

    tabletCounters->RememberCurrentStateAsBaseline(*tabletCountersBaseline);

    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW].Increment(5000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW_BYTES].Increment(6000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW].Increment(10007000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS].Increment(20008000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW_BYTES].Increment(30009000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES].Increment(40010000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW].Increment(11000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW_BYTES].Increment(12000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_UPLOAD_ROWS].Increment(13000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_UPLOAD_ROWS_BYTES].Increment(14000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_SCANNED_ROWS].Increment(15000 + tabletId);
    tabletCounters->Cumulative()[NDataShard::COUNTER_SCANNED_BYTES].Increment(16000 + tabletId);

    // Send these counters to the Tablet Counters Aggregator
    runtime.Send(
        new IEventHandle(
            aggregatorId,
            edgeActorId,
            new TEvTabletCounters::TEvTabletAddCounters(
                new TEvTabletCounters::TInFlightCookie(),
                tabletId,
                TTabletTypes::DataShard,
                TPathId(1113, 1001),
                executorCounters->MakeDiffForAggr(*executorCountersBaseline),
                tabletCounters->MakeDiffForAggr(*tabletCountersBaseline)
            )
        )
    );

    // NOTE: Tablet Counters Aggregator automatically differentiates cumulative
    //       counters and uses the differential to update the associated histograms
    //       (if present). It uses the time between consecutive TEvTabletAddCounters
    //       messages (per tablet ID) to calculate the differential.
    //
    //       The code here sends the main update with all counters as needed,
    //       but the CPU consumption value is set to zero. Then the time is moved
    //       forward by exactly 1 second and a new update is sent with only
    //       the CPU consumption value.
    executorCounters->RememberCurrentStateAsBaseline(*executorCountersBaseline);
    tabletCounters->RememberCurrentStateAsBaseline(*tabletCountersBaseline);

    executorCounters->Cumulative()[TExecutorCounters::CONSUMED_CPU].Increment(
        cpuLoadPercentage * 10000 + tabletId
    );

    runtime.AdvanceCurrentTime(TDuration::Seconds(1));
    runtime.Send(
        new IEventHandle(
            aggregatorId,
            edgeActorId,
            new TEvTabletCounters::TEvTabletAddCounters(
                new TEvTabletCounters::TInFlightCookie(),
                tabletId,
                TTabletTypes::DataShard,
                TPathId(1113, 1001),
                executorCounters->MakeDiffForAggr(*executorCountersBaseline),
                tabletCounters->MakeDiffForAggr(*tabletCountersBaseline)
            )
        )
    );
}

} // namespace <anonymous>

/**
 * Unit tests for the public YDB metrics logic in the Tablet Counters Aggregator.
 */
Y_UNIT_TEST_SUITE(TTabletCountersAggregatorYdbMetricsTest) {
    /**
     * Verify that the Tablet Counters Aggregator handles YDB metrics correctly,
     * if there are no metrics from any tablets yet.
     */
    Y_UNIT_TEST(NoTabletMetrics) {
        TTestBasicRuntime runtime(1);
        runtime.Initialize(TAppPrepare().Unwrap());

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Do not send any tablet metrics, just force the YDB metrics to be recalculated
        ForceCounterRecalculation(runtime, aggregatorId, edgeActorId);

        // The "ydb" counter group should contain all zero values
        auto ydbCounters = runtime.GetAppData(0).Counters->FindSubgroup("counters", "ydb");
        UNIT_ASSERT(ydbCounters);

        TString countersJson = NormalizeJson(NMonitoring::ToJson(*ydbCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
R"json(
{
  "sensors": [
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.limit_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.limit_bytes.hdd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.limit_bytes.ssd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.table.used_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.table.used_bytes.hdd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.table.used_bytes.ssd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.topic.used_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.used_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.used_bytes.hdd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.used_bytes.ssd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.limit_shards"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.storage.limit_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.storage.reserved_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.throughput.limit_bytes_per_second"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.used_shards"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.bulk_upsert.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.bulk_upsert.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.erase.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.erase.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.scan.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.scan.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.write.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.write.rows"
      },
      "value": 0
    },
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
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }

    /**
     * Verify that the Tablet Counters Aggregator handles YDB metrics,
     * mapped from DataShard low level metrics.
     */
    Y_UNIT_TEST(DataShardMetrics) {
        TTestBasicRuntime runtime(1);
        runtime.Initialize(TAppPrepare().Unwrap());

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Send some fake DataShard metrics and force the YDB metrics to be recalculated
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 1, 20);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 20, 40);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 300, 60);

        ForceCounterRecalculation(runtime, aggregatorId, edgeActorId);

        // The "ydb" counter group should contain the correct non-zero values for all metrics
        auto ydbCounters = runtime.GetAppData(0).Counters->FindSubgroup("counters", "ydb");
        UNIT_ASSERT(ydbCounters);

        TString countersJson = NormalizeJson(NMonitoring::ToJson(*ydbCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
R"json(
{
  "sensors": [
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.limit_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.limit_bytes.hdd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.limit_bytes.ssd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.table.used_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.table.used_bytes.hdd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.table.used_bytes.ssd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.topic.used_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.used_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.used_bytes.hdd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.storage.used_bytes.ssd"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.limit_shards"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.storage.limit_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.storage.reserved_bytes"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.throughput.limit_bytes_per_second"
      },
      "value": 0
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "resources.stream.used_shards"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.bulk_upsert.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.bulk_upsert.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.erase.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.erase.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.scan.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.scan.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.write.bytes"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.columnshard.write.rows"
      },
      "value": 0
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.bytes"
      },
      "value": 42321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.bulk_upsert.rows"
      },
      "value": 39321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_hit.bytes"
      },
      "value": 9321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.cache_miss.bytes"
      },
      "value": 12321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.consumed_cpu_us"
      },
      "value": 1200321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.bytes"
      },
      "value": 36321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.erase.rows"
      },
      "value": 33321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.bytes"
      },
      "value": 210057642
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.read.rows"
      },
      "value": 90045642
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.row_count"
      },
      "value": 3321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.bytes"
      },
      "value": 48321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.scan.rows"
      },
      "value": 45321
    },
    {
      "kind": "GAUGE",
      "labels": {
        "name": "table.datashard.size_bytes"
      },
      "value": 6321
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
          1,
          0,
          1,
          0,
          1,
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
      "value": 18321
    },
    {
      "kind": "RATE",
      "labels": {
        "name": "table.datashard.write.rows"
      },
      "value": 15321
    }
  ]
}
)json"
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }
}
