#include "service_actor.h"

#include <ydb/library/actors/interconnect/load.h>

namespace NKikimr {

IActor *CreateInterconnectLoadTest(const NKikimr::TEvLoadTestRequest::TInterconnectLoad& cmd, const NActors::TActorId& parent,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 tag) {
    Y_UNUSED(parent, counters);
    NInterconnect::TLoadParams params {
        .Name = cmd.HasName() ? cmd.GetName() : TString("Interconnect load #") += ToString(tag),
        .Channel = 0U,
        .NodeHops = {1U, 50000U},
        .SizeMin = cmd.HasSizeMin() ? cmd.GetSizeMin() : 0U,
        .SizeMax = cmd.HasSizeMax() ? cmd.GetSizeMax() : 0U,
        .InFlyMax = cmd.GetInFlyMax(),
        .IntervalMin = cmd.HasIntervalMinUs() ? TDuration::MicroSeconds(cmd.GetIntervalMinUs()) : TDuration::Zero(),
        .IntervalMax = cmd.HasIntervalMaxUs() ? TDuration::MicroSeconds(cmd.GetIntervalMaxUs()) : TDuration::Zero(),
        .SoftLoad = cmd.HasSoftLoad() && cmd.GetSoftLoad(),
        .Duration = TDuration::Seconds(cmd.GetDurationSeconds()),
        .UseProtobufWithPayload = cmd.HasUseProtobufWithPayload() && cmd.GetUseProtobufWithPayload()
    };
    return CreateLoadActor(params);
}

} // NKikimr

