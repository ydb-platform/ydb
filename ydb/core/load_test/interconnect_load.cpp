#include "service_actor.h"

#include <ydb/library/actors/interconnect/load.h>
#include <ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/string/cast.h>

namespace NKikimr {

namespace {

// Converts the throughput/RTT statistics computed directly by
// NInterconnect::TLoadActor (see ydb/library/actors/interconnect/load.cpp)
// into the load-test event's TInterconnectLoadFinishStats representation, so
// that the stress tool can render a proper summary table (see
// ydb/tools/stress_tool/device_test_tool_interconnect_test.h). No log parsing
// is involved -- the statistics are passed straight through the load actor's
// finish callback.
TInterconnectLoadFinishStats ToFinishStats(const NInterconnect::TLoadActorStats& src) {
    TInterconnectLoadFinishStats dst;
    dst.Valid = true;
    dst.ThroughputWindow = src.ThroughputWindow;
    dst.ThroughputBytes = src.ThroughputBytes;
    dst.ThroughputSamples = src.ThroughputSamples;
    dst.BytesPerSecond = src.BytesPerSecond;
    dst.RttWindow = src.RttWindow;
    dst.RttSamples = src.RttSamples;
    dst.NumDropped = src.NumDropped;
    dst.LatencyPercentilesUs = src.LatencyPercentilesUs;
    return dst;
}

} // anonymous namespace

IActor *CreateInterconnectLoadTest(const NKikimr::TEvLoadTestRequest::TInterconnectLoad& cmd, const NActors::TActorId& parent,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>&, ui64 tag) {
    const TString name = cmd.HasName() ? cmd.GetName() : TString("Interconnect load #") += ToString(tag);

    NInterconnect::TLoadParams params {
        .Name = name,
        .Channel = 0U,
        .SizeMin = cmd.HasSizeMin() ? cmd.GetSizeMin() : 0U,
        .SizeMax = cmd.HasSizeMax() ? cmd.GetSizeMax() : 0U,
        .InFlyMax = cmd.GetInFlyMax(),
        .IntervalMin = cmd.HasIntervalMinUs() ? TDuration::MicroSeconds(cmd.GetIntervalMinUs()) : TDuration::Zero(),
        .IntervalMax = cmd.HasIntervalMaxUs() ? TDuration::MicroSeconds(cmd.GetIntervalMaxUs()) : TDuration::Zero(),
        .SoftLoad = cmd.HasSoftLoad() && cmd.GetSoftLoad(),
        .Duration = TDuration::Seconds(cmd.GetDurationSeconds()),
        .UseProtobufWithPayload = cmd.HasUseProtobufWithPayload() && cmd.GetUseProtobufWithPayload()
    };

    for (const auto& node : cmd.GetNodeHops())
        params.NodeHops.emplace_back(node);

    const auto callback = [tag, parent] (const TActorContext& ctx, TString&& html, const NInterconnect::TLoadActorStats& stats) {
        TIntrusivePtr<TEvLoad::TLoadReport> report(new TEvLoad::TLoadReport());
        auto finishEv = new TEvLoad::TEvLoadTestFinished(tag, report, "Load test finished.");
        finishEv->LastHtmlPage = std::move(html);

        SetInterconnectLoadFinishStats(*finishEv, ToFinishStats(stats));

        ctx.Send(parent, finishEv);
    };

    return CreateLoadActor(params, callback);
}

} // NKikimr
