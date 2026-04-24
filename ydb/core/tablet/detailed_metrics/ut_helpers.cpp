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

TString NormalizeJson(
    const TString& jsonString,
    const std::unordered_map<TString, std::unordered_set<TString>>& removeSensorsByLabels
) {
    NJson::TJsonValue parsedJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(jsonString), &parsedJson));

    // Filter some sensors, if requested
    if (!removeSensorsByLabels.empty()) {
        auto& sensorsArray = parsedJson["sensors"];

        for (size_t i = 0; i < sensorsArray.GetArraySafe().size(); ++i) {
            for (const auto& [labelName, labelValue] : sensorsArray[i]["labels"].GetMapSafe()) {
                auto labelIt = removeSensorsByLabels.find(labelName);

                if (labelIt != removeSensorsByLabels.end()) {
                    if (labelIt->second.contains(labelValue.GetStringSafe())) {
                        // This sensor should be excluded
                        sensorsArray.EraseValue(i);
                        --i; // To process the next element

                        break;
                    }
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

TActorId InitializeTabletCountersAggregator(
    TTestBasicRuntime& runtime,
    bool forFollowers,
    bool enableDetailedMetrics
) {
    // NOTE: Enable the per-database counters in the Tablet Counters Aggregator
    //       (required to use detailed metrics)
    runtime.GetAppData().FeatureFlags.SetEnableDbCounters(true);
    runtime.GetAppData().FeatureFlags.SetEnableDetailedMetrics(enableDetailedMetrics);

    // Register the Tablet Counters Aggregator actor
    TActorId aggregatorId = runtime.Register(CreateTabletCountersAggregator(forFollowers));
    runtime.EnableScheduleForActor(aggregatorId);

    // Wait for the TEvBootstrap event to be processed, after this the actor is ready
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);

    runtime.DispatchEvents(options);
    return aggregatorId;
}

void SendDataShardMetrics(
    TTestBasicRuntime& runtime,
    const TActorId& aggregatorId,
    const TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId,
    ui32 cpuLoadPercentage,
    TEvTabletCounters::TTableMetricsConfig* tableMetricsConfig,
    std::optional<ui64> metricsOffset
) {
    // Use the tablet ID as the default metrics offset
    if (!metricsOffset) {
        metricsOffset = tabletId;
    }

    // Populate executor counters with some fake values
    auto executorCounters = MakeHolder<TExecutorCounters>();
    auto executorCountersBaseline = MakeHolder<TExecutorCounters>();

    executorCounters->RememberCurrentStateAsBaseline(*executorCountersBaseline);

    executorCounters->Simple()[TExecutorCounters::DB_UNIQUE_ROWS_TOTAL] = 10000 + (*metricsOffset);
    executorCounters->Simple()[TExecutorCounters::DB_UNIQUE_DATA_BYTES] = 20000 + (*metricsOffset);

    executorCounters->Cumulative()[TExecutorCounters::TX_BYTES_CACHED].Increment(30000 + (*metricsOffset));
    executorCounters->Cumulative()[TExecutorCounters::TX_BYTES_READ].Increment(40000 + (*metricsOffset));

    // Populate DataShard counters with some fake values
    auto tabletCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
    auto tabletCountersBaseline = CreateAppCountersByTabletType(TTabletTypes::DataShard);

    tabletCounters->RememberCurrentStateAsBaseline(*tabletCountersBaseline);

    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW].Increment(50000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW_BYTES].Increment(60000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW].Increment(10070000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS].Increment(20080000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW_BYTES].Increment(30090000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES].Increment(40100000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW].Increment(110000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW_BYTES].Increment(120000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_UPLOAD_ROWS].Increment(130000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_UPLOAD_ROWS_BYTES].Increment(140000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_SCANNED_ROWS].Increment(150000 + (*metricsOffset));
    tabletCounters->Cumulative()[NDataShard::COUNTER_SCANNED_BYTES].Increment(160000 + (*metricsOffset));

    // Send these counters to the Tablet Counters Aggregator
    runtime.Send(
        new IEventHandle(
            aggregatorId,
            edgeActorId,
            new TEvTabletCounters::TEvTabletAddCounters(
                new TEvTabletCounters::TInFlightCookie(),
                tabletId,
                followerId,
                TTabletTypes::DataShard,
                TPathId(1113, 1001),
                executorCounters->MakeDiffForAggr(*executorCountersBaseline),
                tabletCounters->MakeDiffForAggr(*tabletCountersBaseline),
                tableMetricsConfig
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
        cpuLoadPercentage * 10000 + (*metricsOffset)
    );

    runtime.AdvanceCurrentTime(TDuration::Seconds(1));
    runtime.Send(
        new IEventHandle(
            aggregatorId,
            edgeActorId,
            new TEvTabletCounters::TEvTabletAddCounters(
                new TEvTabletCounters::TInFlightCookie(),
                tabletId,
                followerId,
                TTabletTypes::DataShard,
                TPathId(1113, 1001),
                executorCounters->MakeDiffForAggr(*executorCountersBaseline),
                tabletCounters->MakeDiffForAggr(*tabletCountersBaseline),
                tableMetricsConfig
            )
        )
    );
}

void SendForgetDataShardTablet(
    NActors::TTestBasicRuntime& runtime,
    const NActors::TActorId& aggregatorId,
    const NActors::TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId
) {
    runtime.Send(
        new IEventHandle(
            aggregatorId,
            edgeActorId,
            new TEvTabletCounters::TEvTabletCountersForgetTablet(
                tabletId,
                followerId,
                TTabletTypes::DataShard,
                TPathId(1113, 1001)
            )
        )
    );
}

} // namespace NDetailedMetricsTests

} // namespace NKikimr
