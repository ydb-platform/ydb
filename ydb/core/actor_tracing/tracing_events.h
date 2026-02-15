#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/core/protos/actor_tracing.pb.h>

namespace NKikimr::NActorTracing {

    struct TEvTracing {
        enum EEv {
            EvTraceStart = EventSpaceBegin(TKikimrEvents::ES_TRACING),
            EvTraceStop,
            EvTraceFetch,
            EvTraceStartResult,
            EvTraceStopResult,
            EvTraceFetchResult,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TRACING));

        struct TEvTraceStart : TEventPB<TEvTraceStart, NKikimrActorTracing::TEvTraceStartRequest, EvTraceStart> {};
        struct TEvTraceStop : TEventPB<TEvTraceStop, NKikimrActorTracing::TEvTraceStopRequest, EvTraceStop> {};
        struct TEvTraceFetch : TEventPB<TEvTraceFetch, NKikimrActorTracing::TEvTraceFetchRequest, EvTraceFetch> {};

        struct TEvTraceStartResult : TEventPB<TEvTraceStartResult, NKikimrActorTracing::TEvTraceStartResponse, EvTraceStartResult> {};
        struct TEvTraceStopResult : TEventPB<TEvTraceStopResult, NKikimrActorTracing::TEvTraceStopResponse, EvTraceStopResult> {};
        struct TEvTraceFetchResult : TEventPB<TEvTraceFetchResult, NKikimrActorTracing::TEvTraceFetchResponse, EvTraceFetchResult> {};
    };

    inline NActors::TActorId MakeActorTracingServiceId(ui32 nodeId) {
        return NActors::TActorId(nodeId, "act_trc_svc");
    }

} // namespace NKikimr::NActorTracing
