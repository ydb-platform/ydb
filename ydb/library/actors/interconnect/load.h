#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

namespace NInterconnect {
    // load responder -- lives on every node as a service actor
    NActors::IActor* CreateLoadResponderActor();
    NActors::TActorId MakeLoadResponderActorId(ui32 node);

    // load actor -- generates load with specific parameters
    struct TLoadParams {
        TString Name;
        ui32 Channel;
        TVector<ui32> NodeHops;             // node ids for the message route
        ui32 SizeMin, SizeMax;              // min and max size for payloads
        ui32 InFlyMax;                      // maximum number of in fly messages
        TDuration IntervalMin, IntervalMax; // min and max intervals between sending messages
        bool SoftLoad;                      // is the load soft?
        TDuration Duration;                 // test duration
        bool UseProtobufWithPayload;        // store payload separately
        ui32 RdmaMode;                      // rdma params bitmap, 0 - not used
        TDuration DelayBeforeMeasurements;   // warm-up period excluded from reported stats
    };

    // Aggregated throughput/RTT statistics of a finished (or just-terminated)
    // TLoadActor run, computed directly from its internal counters (no log
    // parsing involved). Passed to TFinishCallback alongside the rendered
    // HTML page so that callers can build their own summary representation.
    struct TLoadActorStats {
        TDuration ThroughputWindow;      // window over which throughput was measured (full run)
        ui64 ThroughputBytes = 0;        // bytes transferred within ThroughputWindow
        ui64 ThroughputSamples = 0;      // number of messages accounted within ThroughputWindow
        ui64 BytesPerSecond = 0;         // average throughput over the whole run, bytes/sec
        TDuration RttWindow;             // window over which RTT samples were collected (last aggregation window)
        ui64 RttSamples = 0;             // number of RTT samples within RttWindow
        ui64 NumDropped = 0;             // number of dropped (timed out) messages, full-run total

        // Latency (RTT) percentiles; pair of {quantile in [0..1], value in microseconds}.
        TVector<std::pair<double, ui64>> LatencyPercentilesUs;
    };

    using TFinishCallback = std::function<void(const NActors::TActorContext& ctx, TString&& html, const TLoadActorStats& stats)>;
    NActors::IActor* CreateLoadActor(const TLoadParams& params, const TFinishCallback& finishCallback = {});

}
