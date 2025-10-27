#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/counters_info.pb.h>

namespace NKikimr {
namespace NCountersInfo {

enum EEv {
    EvCountersInfoRequest = EventSpaceBegin(TKikimrEvents::ES_COUNTERS_INFO),
    EvCountersInfoResponse,
    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_COUNTERS_INFO), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_COUNTERS_INFO)");

struct TEvCountersInfoRequest : public TEventPB<TEvCountersInfoRequest, NKikimrCountersInfoProto::TEvCountersInfoRequest, EvCountersInfoRequest> {};
struct TEvCountersInfoResponse : public TEventPB<TEvCountersInfoResponse, NKikimrCountersInfoProto::TEvCountersInfoResponse, EvCountersInfoResponse> {};

inline NActors::TActorId MakeCountersInfoProviderServiceID(ui32 nodeId) { return NActors::TActorId(nodeId, "countersInfo"); }
IActor* CreateCountersInfoProviderService(::NMonitoring::TDynamicCounterPtr counters);
}
}
