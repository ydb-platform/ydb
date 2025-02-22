#pragma once

#include "oidc_settings.h"
#include "context.h"
#include <ydb/library/actors/core/events.h>

namespace NMVP::NOIDC {

class TImpersonateStopPageHandler : public NActors::TActor<TImpersonateStopPageHandler> {
    using TBase = NActors::TActor<TImpersonateStopPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TImpersonateStopPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
