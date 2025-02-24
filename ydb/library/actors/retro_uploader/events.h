#pragma once

#include <ydb/library/retro_tracing/retro_span_base.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>

namespace NRetro {
    enum EEvents {
        EvDemandTrace = EventSpaceBegin(NActors::TEvents::ES_RETRO),
    };

    struct TEvDemandTrace : public NActors::TEventLocal<TEvDemandTrace, EvDemandTrace> {
    public:
        TEvDemandTrace(TTraceId traceId)
            : TraceId(traceId)
        {}
    
    public:
        TTraceId TraceId;
    };
    

} // namespace NRetro
