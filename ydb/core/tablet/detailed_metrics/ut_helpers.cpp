#include "ut_helpers.h"

#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr::NTabletFlatExecutor;

namespace NKikimr {

namespace NDetailedMetricsTests {

TString NormalizeJson(const TString& jsonString,
    const std::unordered_map<TString, std::unordered_set<TString>>& removeSensorsByLabels)
{
    NJson::TJsonValue parsedJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(jsonString), &parsedJson));

    if (!removeSensorsByLabels.empty()) {
        auto& sensorsArray = parsedJson["sensors"];

        for (size_t i = 0; i < sensorsArray.GetArraySafe().size(); ++i) {
            for (const auto& [labelName, labelValue] : sensorsArray[i]["labels"].GetMapSafe()) {
                auto labelIt = removeSensorsByLabels.find(labelName);
                if (labelIt != removeSensorsByLabels.end() && labelIt->second.contains(labelValue.GetStringSafe())) {
                    sensorsArray.EraseValue(i);
                    --i; // Don't step over the next element.
                    break;
                }
            }
        }
    }

    // NOTE: The prettifier is needed here to make sure all brackets (both [] and {})
    //       are aligned "Python style" with the opening bracket placed on the starting line.
    //       By default, WriteJson() places opening brackets on a separate line and makes
    //       all the inner strings double-aligned, which takes too many lines
    //       and makes it harder for humans to read.
    return NJson::PrettifyJson(
        NJson::WriteJson(
            parsedJson,
            true /* formatOutput */,
            true /* sortkeys */
        ),
        false /* unquote */,
        2 /* padding */
    );
}

TActorId InitializeTabletCountersAggregator(TTestBasicRuntime& runtime,
    bool forFollowers,
    bool enableDetailedMetrics)
{
    // To use detailed metrics per-database counters in the Tablet Counters Aggregator should be enabled
    runtime.GetAppData().FeatureFlags.SetEnableDbCounters(true);
    runtime.GetAppData().FeatureFlags.SetEnableDataShardDetailedMetrics(enableDetailedMetrics);

    // Register the Tablet Counters Aggregator actor
    TActorId aggregatorId = runtime.Register(CreateTabletCountersAggregator(forFollowers));
    runtime.EnableScheduleForActor(aggregatorId);

    // Wait for the TEvBootstrap event to be processed, after this the actor is ready
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);

    runtime.DispatchEvents(options);
    return aggregatorId;
}

TExpectedSensorGroup SendDataShardMetrics(TTestBasicRuntime& runtime,
    const TActorId& aggregatorId,
    const TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId,
    ui32 cpuLoadPercentage,
    TEvTabletCounters::TTableMetricsConfig* tableMetricsConfig,
    std::optional<ui64> metricsOffset)
{
    if (!metricsOffset) {
        metricsOffset = tabletId; // Use the tablet ID as the default metrics offset
    }

    TExpectedSensorGroup result;

    // Populate executor counters with some fake values
    auto executorCounters = MakeHolder<TExecutorCounters>();
    auto executorCountersBaseline = MakeHolder<TExecutorCounters>();

    executorCounters->RememberCurrentStateAsBaseline(*executorCountersBaseline);

    result.Add("table.datashard.row_count", executorCounters->Simple()[TExecutorCounters::DB_UNIQUE_ROWS_TOTAL] = 10000 + (*metricsOffset));
    result.Add("table.datashard.size_bytes", executorCounters->Simple()[TExecutorCounters::DB_UNIQUE_DATA_BYTES] = 20000 + (*metricsOffset));

    result.Add("table.datashard.cache_hit.bytes", executorCounters->Cumulative()[TExecutorCounters::TX_BYTES_CACHED].Increment(30000 + (*metricsOffset)));
    result.Add("table.datashard.cache_miss.bytes", executorCounters->Cumulative()[TExecutorCounters::TX_BYTES_READ].Increment(40000 + (*metricsOffset)));

    // Populate DataShard counters with some fake values
    auto tabletCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
    auto tabletCountersBaseline = CreateAppCountersByTabletType(TTabletTypes::DataShard);

    tabletCounters->RememberCurrentStateAsBaseline(*tabletCountersBaseline);

    result.Add("table.datashard.write.rows", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW].Increment(50000 + (*metricsOffset)));
    result.Add("table.datashard.write.bytes", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW_BYTES].Increment(60000 + (*metricsOffset)));
    result.Add("table.datashard.read.rows", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW].Increment(10070000 + (*metricsOffset)));
    result.Add("table.datashard.read.rows", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS].Increment(20080000 + (*metricsOffset)));
    result.Add("table.datashard.read.bytes", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW_BYTES].Increment(30090000 + (*metricsOffset)));
    result.Add("table.datashard.read.bytes", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES].Increment(40100000 + (*metricsOffset)));
    result.Add("table.datashard.erase.rows", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW].Increment(110000 + (*metricsOffset)));
    result.Add("table.datashard.erase.bytes", tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW_BYTES].Increment(120000 + (*metricsOffset)));
    result.Add("table.datashard.bulk_upsert.rows", tabletCounters->Cumulative()[NDataShard::COUNTER_UPLOAD_ROWS].Increment(130000 + (*metricsOffset)));
    result.Add("table.datashard.bulk_upsert.bytes", tabletCounters->Cumulative()[NDataShard::COUNTER_UPLOAD_ROWS_BYTES].Increment(140000 + (*metricsOffset)));
    result.Add("table.datashard.scan.rows", tabletCounters->Cumulative()[NDataShard::COUNTER_SCANNED_ROWS].Increment(150000 + (*metricsOffset)));
    result.Add("table.datashard.scan.bytes", tabletCounters->Cumulative()[NDataShard::COUNTER_SCANNED_BYTES].Increment(160000 + (*metricsOffset)));

    // Send these counters to the Tablet Counters Aggregator
    runtime.Send(new IEventHandle(aggregatorId,
        edgeActorId,
        new TEvTabletCounters::TEvTabletAddCounters(new TEvTabletCounters::TInFlightCookie(),
            tabletId,
            followerId,
            TTabletTypes::DataShard,
            TPathId(1113, 1001),
            executorCounters->MakeDiffForAggr(*executorCountersBaseline),
            tabletCounters->MakeDiffForAggr(*tabletCountersBaseline),
            tableMetricsConfig)));

    executorCounters->RememberCurrentStateAsBaseline(*executorCountersBaseline);
    tabletCounters->RememberCurrentStateAsBaseline(*tabletCountersBaseline);

    result.Add("table.datashard.consumed_cpu_us", executorCounters->Cumulative()[TExecutorCounters::CONSUMED_CPU].Increment(
        cpuLoadPercentage * 10000 + (*metricsOffset)
    ));

    // Send cpu consumption to the Tablet Counters Aggregator as cumulative counter for a simulated one-second interval.
    // The aggregator will update corresponding CPU consumption histogram automatically.
    runtime.AdvanceCurrentTime(TDuration::Seconds(1));
    runtime.Send(new IEventHandle(aggregatorId,
        edgeActorId,
        new TEvTabletCounters::TEvTabletAddCounters(new TEvTabletCounters::TInFlightCookie(),
            tabletId,
            followerId,
            TTabletTypes::DataShard,
            TPathId(1113, 1001),
            executorCounters->MakeDiffForAggr(*executorCountersBaseline),
            tabletCounters->MakeDiffForAggr(*tabletCountersBaseline),
            tableMetricsConfig)));

    ui64 bucket = (cpuLoadPercentage * 10000 + (*metricsOffset) + 99999) / 100000 * 10;
    result.AddHist("table.datashard.used_core_percents", {{bucket, 1}});

    return result;
}

TExpectedSensorGroup EmptyDataShardMetrics() {
    TExpectedSensorGroup result;

    result.Add("table.datashard.row_count", TTabletSimpleCounter{});
    result.Add("table.datashard.size_bytes", TTabletSimpleCounter{});
    result.Add("table.datashard.cache_hit.bytes", TTabletCumulativeCounter{});
    result.Add("table.datashard.cache_miss.bytes", TTabletCumulativeCounter{});
    result.Add("table.datashard.write.rows", TTabletCumulativeCounter{});
    result.Add("table.datashard.write.bytes", TTabletCumulativeCounter{});
    result.Add("table.datashard.read.rows", TTabletCumulativeCounter{});
    result.Add("table.datashard.read.bytes", TTabletCumulativeCounter{});
    result.Add("table.datashard.erase.rows", TTabletCumulativeCounter{});
    result.Add("table.datashard.erase.bytes", TTabletCumulativeCounter{});
    result.Add("table.datashard.bulk_upsert.rows", TTabletCumulativeCounter{});
    result.Add("table.datashard.bulk_upsert.bytes", TTabletCumulativeCounter{});
    result.Add("table.datashard.scan.rows", TTabletCumulativeCounter{});
    result.Add("table.datashard.scan.bytes", TTabletCumulativeCounter{});
    result.Add("table.datashard.consumed_cpu_us", TTabletCumulativeCounter{});
    result.AddHist("table.datashard.used_core_percents", {});

    return result;
}

void SendForgetDataShardTablet(NActors::TTestBasicRuntime& runtime,
    const NActors::TActorId& aggregatorId,
    const NActors::TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId)
{
    runtime.Send(new IEventHandle(aggregatorId,
        edgeActorId,
        new TEvTabletCounters::TEvTabletCountersForgetTablet(tabletId,
            followerId,
            TTabletTypes::DataShard,
            TPathId(1113, 1001))));
}

} // namespace NDetailedMetricsTests

} // namespace NKikimr
