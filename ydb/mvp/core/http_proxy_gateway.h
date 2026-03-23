#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP {

class THttpProxyGateway : public NActors::TActorBootstrapped<THttpProxyGateway> {
    using TBase = NActors::TActorBootstrapped<THttpProxyGateway>;

    const NActors::TActorId NextProxyId;

public:
    explicit THttpProxyGateway(const NActors::TActorId& nextProxyId);

    void Bootstrap();
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // namespace NMVP
