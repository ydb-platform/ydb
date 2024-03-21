#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http_proxy.h>

#if defined(PROFILE_MEMORY_ALLOCATIONS) || true
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>

class THandlerActorMemProfiler : public NActors::TActor<THandlerActorMemProfiler> {
public:
    using TBase = NActors::TActor<THandlerActorMemProfiler>;

    THandlerActorMemProfiler()
        : TBase(&THandlerActorMemProfiler::StateWork)
    {}

    bool Enabled = false;

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        if (Enabled) {
            TStringStream data;
            NAllocProfiler::StopAllocationSampling(data);
            Enabled = false;
            auto response = event->Get()->Request->CreateResponseOK(data.Str(), "text/plain");
            Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        } else {
            if (!NAllocProfiler::StartAllocationSampling(true)) {
                auto response = event->Get()->Request->CreateResponseServiceUnavailable("Can't enable profiling\n", "text/plain");
                Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            } else {
                Enabled = true;
                auto response = event->Get()->Request->CreateResponseOK("Profiling enabled\n", "text/plain");
                Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            }
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

#else

class THandlerActorMemProfiler : public NActors::TActor<THandlerActorMemProfiler> {
public:
    using TBase = NActors::TActor<THandlerActorMemProfiler>;

    THandlerActorMemProfiler()
        : TBase(&THandlerActorMemProfiler::StateWork)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        auto response = event->Get()->Request->CreateResponseServiceUnavailable("Rebuild with PROFILE_MEMORY_ALLOCATIONS to enable memory profiling\n", "text/plain");
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

#endif

NActors::IActor* CreateMemProfiler() {
    return new THandlerActorMemProfiler();
}
