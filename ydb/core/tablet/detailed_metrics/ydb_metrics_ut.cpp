#include "ut_helpers.h"
#include "ut_sensors_json_builder.h"

#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;

namespace {

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

} // namespace <anonymous>

TExpectedSensorGroup EmptyYdbMetrics() {
    TExpectedSensorGroup result;

    result.Add("resources.storage.limit_bytes", TTabletSimpleCounter{});
    result.Add("resources.storage.limit_bytes.hdd", TTabletSimpleCounter{});
    result.Add("resources.storage.limit_bytes.ssd", TTabletSimpleCounter{});
    result.Add("resources.storage.table.used_bytes", TTabletSimpleCounter{});
    result.Add("resources.storage.table.used_bytes.hdd", TTabletSimpleCounter{});
    result.Add("resources.storage.table.used_bytes.ssd", TTabletSimpleCounter{});
    result.Add("resources.storage.topic.used_bytes", TTabletSimpleCounter{});
    result.Add("resources.storage.used_bytes", TTabletSimpleCounter{});
    result.Add("resources.storage.used_bytes.hdd", TTabletSimpleCounter{});
    result.Add("resources.storage.used_bytes.ssd", TTabletSimpleCounter{});
    result.Add("resources.stream.limit_shards", TTabletSimpleCounter{});
    result.Add("resources.stream.storage.limit_bytes", TTabletSimpleCounter{});
    result.Add("resources.stream.storage.reserved_bytes", TTabletSimpleCounter{});
    result.Add("resources.stream.throughput.limit_bytes_per_second", TTabletSimpleCounter{});
    result.Add("resources.stream.used_shards", TTabletSimpleCounter{});
    result.Add("table.columnshard.bulk_upsert.bytes", TTabletCumulativeCounter{});
    result.Add("table.columnshard.bulk_upsert.rows", TTabletCumulativeCounter{});
    result.Add("table.columnshard.erase.bytes", TTabletCumulativeCounter{});
    result.Add("table.columnshard.erase.rows", TTabletCumulativeCounter{});
    result.Add("table.columnshard.scan.bytes", TTabletCumulativeCounter{});
    result.Add("table.columnshard.scan.rows", TTabletCumulativeCounter{});
    result.Add("table.columnshard.write.bytes", TTabletCumulativeCounter{});
    result.Add("table.columnshard.write.rows", TTabletCumulativeCounter{});

    return result;
}

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

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Do not send any tablet metrics, just force the YDB metrics to be recalculated
        ForceCounterRecalculation(runtime, aggregatorId, edgeActorId);

        // The "ydb" counter group should contain all zero values
        auto ydbCounters = runtime.GetAppData(0).Counters->FindSubgroup("counters", "ydb");
        UNIT_ASSERT(ydbCounters);

        TString countersJson = NormalizeJson(NMonitoring::ToJson(*ydbCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGroup(EmptyYdbMetrics())
                .AddGroup(EmptyDataShardMetrics())
                .BuildJson()
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

        TActorId aggregatorId = InitializeTabletCountersAggregator(runtime, false /* forFollowers */);
        TActorId edgeActorId = runtime.AllocateEdgeActor();

        // Send some fake DataShard metrics and force the YDB metrics to be recalculated
        auto expectedMetrics = SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 1, 0, 20);
        expectedMetrics |= SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 20, 0, 40);
        expectedMetrics |= SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 300, 0, 60);

        ForceCounterRecalculation(runtime, aggregatorId, edgeActorId);

        // The "ydb" counter group should contain the correct non-zero values for all metrics
        auto ydbCounters = runtime.GetAppData(0).Counters->FindSubgroup("counters", "ydb");
        UNIT_ASSERT(ydbCounters);

        TString countersJson = NormalizeJson(NMonitoring::ToJson(*ydbCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGroup(EmptyYdbMetrics())
                .AddGroup(expectedMetrics)
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }
}
