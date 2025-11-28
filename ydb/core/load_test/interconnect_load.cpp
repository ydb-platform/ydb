#include "service_actor.h"

#include <ydb/library/actors/interconnect/load.h>

namespace NKikimr {

IActor *CreateInterconnectLoadTest(const NKikimr::TEvLoadTestRequest::TInterconnectLoad& cmd, const NActors::TActorId& parent,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>&, ui64 tag) {
    NInterconnect::TLoadParams params {
        .Name = cmd.HasName() ? cmd.GetName() : TString("Interconnect load #") += ToString(tag),
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

    const auto callback = [tag, parent] (const TActorContext& ctx, TString&& html) {
        TIntrusivePtr<TEvLoad::TLoadReport> report(new TEvLoad::TLoadReport());
        auto finishEv = new TEvLoad::TEvLoadTestFinished(tag, report, "Load test finished.");
        finishEv->LastHtmlPage = std::move(html);
        ctx.Send(parent, finishEv);
    };

    return CreateLoadActor(params, callback);
}

} // NKikimr

