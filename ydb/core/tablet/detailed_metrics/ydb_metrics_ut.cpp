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
                .AddGauge("resources.storage.limit_bytes", 0)
                .AddGauge("resources.storage.limit_bytes.hdd", 0)
                .AddGauge("resources.storage.limit_bytes.ssd", 0)
                .AddGauge("resources.storage.table.used_bytes", 0)
                .AddGauge("resources.storage.table.used_bytes.hdd", 0)
                .AddGauge("resources.storage.table.used_bytes.ssd", 0)
                .AddGauge("resources.storage.topic.used_bytes", 0)
                .AddGauge("resources.storage.used_bytes", 0)
                .AddGauge("resources.storage.used_bytes.hdd", 0)
                .AddGauge("resources.storage.used_bytes.ssd", 0)
                .AddGauge("resources.stream.limit_shards", 0)
                .AddGauge("resources.stream.storage.limit_bytes", 0)
                .AddGauge("resources.stream.storage.reserved_bytes", 0)
                .AddGauge("resources.stream.throughput.limit_bytes_per_second", 0)
                .AddGauge("resources.stream.used_shards", 0)
                .AddRate("table.columnshard.bulk_upsert.bytes", 0)
                .AddRate("table.columnshard.bulk_upsert.rows", 0)
                .AddRate("table.columnshard.erase.bytes", 0)
                .AddRate("table.columnshard.erase.rows", 0)
                .AddRate("table.columnshard.scan.bytes", 0)
                .AddRate("table.columnshard.scan.rows", 0)
                .AddRate("table.columnshard.write.bytes", 0)
                .AddRate("table.columnshard.write.rows", 0)
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
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 1, 0, 20);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 20, 0, 40);
        SendDataShardMetrics(runtime, aggregatorId, edgeActorId, 300, 0, 60);

        ForceCounterRecalculation(runtime, aggregatorId, edgeActorId);

        // The "ydb" counter group should contain the correct non-zero values for all metrics
        auto ydbCounters = runtime.GetAppData(0).Counters->FindSubgroup("counters", "ydb");
        UNIT_ASSERT(ydbCounters);

        TString countersJson = NormalizeJson(NMonitoring::ToJson(*ydbCounters));
        Cerr << "TEST Current counters:" << Endl << countersJson << Endl;

        TString expectedJson = NormalizeJson(
            TSensorsJsonBuilder::Start()
                .AddGauge("resources.storage.limit_bytes", 0)
                .AddGauge("resources.storage.limit_bytes.hdd", 0)
                .AddGauge("resources.storage.limit_bytes.ssd", 0)
                .AddGauge("resources.storage.table.used_bytes", 0)
                .AddGauge("resources.storage.table.used_bytes.hdd", 0)
                .AddGauge("resources.storage.table.used_bytes.ssd", 0)
                .AddGauge("resources.storage.topic.used_bytes", 0)
                .AddGauge("resources.storage.used_bytes", 0)
                .AddGauge("resources.storage.used_bytes.hdd", 0)
                .AddGauge("resources.storage.used_bytes.ssd", 0)
                .AddGauge("resources.stream.limit_shards", 0)
                .AddGauge("resources.stream.storage.limit_bytes", 0)
                .AddGauge("resources.stream.storage.reserved_bytes", 0)
                .AddGauge("resources.stream.throughput.limit_bytes_per_second", 0)
                .AddGauge("resources.stream.used_shards", 0)
                .AddRate("table.columnshard.bulk_upsert.bytes", 0)
                .AddRate("table.columnshard.bulk_upsert.rows", 0)
                .AddRate("table.columnshard.erase.bytes", 0)
                .AddRate("table.columnshard.erase.rows", 0)
                .AddRate("table.columnshard.scan.bytes", 0)
                .AddRate("table.columnshard.scan.rows", 0)
                .AddRate("table.columnshard.write.bytes", 0)
                .AddRate("table.columnshard.write.rows", 0)
                .AddRate("table.datashard.bulk_upsert.bytes", 420321)
                .AddRate("table.datashard.bulk_upsert.rows", 390321)
                .AddRate("table.datashard.cache_hit.bytes", 90321)
                .AddRate("table.datashard.cache_miss.bytes", 120321)
                .AddRate("table.datashard.consumed_cpu_us", 1200321)
                .AddRate("table.datashard.erase.bytes", 360321)
                .AddRate("table.datashard.erase.rows", 330321)
                .AddRate("table.datashard.read.bytes", 210570642)
                .AddRate("table.datashard.read.rows", 90450642)
                .AddGauge("table.datashard.row_count", 30321)
                .AddRate("table.datashard.scan.bytes", 480321)
                .AddRate("table.datashard.scan.rows", 450321)
                .AddGauge("table.datashard.size_bytes", 60321)
                .AddCpuHistogram("table.datashard.used_core_percents", {{30, 1}, {50, 1}, {70, 1}})
                .AddRate("table.datashard.write.bytes", 180321)
                .AddRate("table.datashard.write.rows", 150321)
                .BuildJson()
        );

        UNIT_ASSERT_EQUAL_C(
            countersJson,
            expectedJson,
            "Expected JSON:" << Endl << expectedJson
        );
    }
}
