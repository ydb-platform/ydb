#pragma once
#include <ydb/library/actors/core/events.h>

namespace NRetroTracing {

struct TEvRetroTracing {
    enum EEv : ui32 {
        Start = EventSpaceBegin(NActors::TEvents::ES_RETRO_TRACING),
        EvCollectRetroTrace = Start,
        EvCollectAllRetroTraces,
        End,
    };

    static_assert(End < EventSpaceEnd(NActors::TEvents::ES_RETRO_TRACING));
};

struct TEvCollectRetroTrace : NActors::TEventLocal<TEvCollectRetroTrace,
        TEvRetroTracing::EvCollectRetroTrace> {
    NWilson::TTraceId TraceId;

    TEvCollectRetroTrace(const NWilson::TTraceId& traceId);
};

struct TEvCollectAllRetroTraces : NActors::TEventLocal<TEvCollectAllRetroTraces,
        TEvRetroTracing::EvCollectAllRetroTraces> {
};

}  // namespace NRetroTracing
