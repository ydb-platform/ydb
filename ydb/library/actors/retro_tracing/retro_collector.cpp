#include "retro_collector.h"
#include "span_buffer.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NRetroTracing {

struct TEvPrivate {
    enum EEv {
        EvCollectRetroTrace = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCollectAllRetroTraces
    };

    struct TEvCollectRetroTrace : NActors::TEventLocal<TEvCollectRetroTrace, EvCollectRetroTrace> {
        NWilson::TTraceId TraceId;

        TEvCollectRetroTrace(const NWilson::TTraceId& traceId)
            : TraceId(traceId)
        {}
    };

    struct TEvCollectAllRetroTraces : NActors::TEventLocal<TEvCollectAllRetroTraces, EvCollectAllRetroTraces> {};
};

class TRetroCollector : public NActors::TActorBootstrapped<TRetroCollector> {
private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCollectRetroTrace, Handle);
        cFunc(TEvPrivate::TEvCollectAllRetroTraces::EventType, HandleCollectAll);
        cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
    );

    void Handle(const TEvPrivate::TEvCollectRetroTrace::TPtr& ev) {
        if (!ev->Get()->TraceId) {
            return;
        }

        ConvertAndSend(GetSpansOfTrace(ev->Get()->TraceId));
    }

    void HandleCollectAll() {
        ConvertAndSend(GetAllSpans());
    }

    void ConvertAndSend(std::vector<std::unique_ptr<TRetroSpan>>&& spans) {
        for (const std::unique_ptr<TRetroSpan>& span : spans) {
            std::unique_ptr<NWilson::TSpan> wilson = span->MakeWilsonSpan();
            wilson->Attribute("type", "RETRO");
            wilson->End();
        }
    }

public:
    void Bootstrap() {
        Become(&TThis::StateFunc);
    }
};

NActors::IActor* CreateRetroCollector() {
    return new TRetroCollector;
}

void DemandTrace(const NWilson::TTraceId& traceId) {
    NActors::TActivationContext::Send(std::make_unique<NActors::IEventHandle>(
            MakeRetroCollectorId(), NActors::TActorId{},
            new TEvPrivate::TEvCollectRetroTrace(traceId)));
}

void DemandAllTraces() {
    NActors::TActivationContext::Send(std::make_unique<NActors::IEventHandle>(
            MakeRetroCollectorId(), NActors::TActorId{},
            new TEvPrivate::TEvCollectAllRetroTraces));
}

} // namespace NRetroTracing
