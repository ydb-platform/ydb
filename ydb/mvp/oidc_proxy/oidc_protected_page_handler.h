#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "oidc_settings.h"

namespace NMVP::NOIDC {

using namespace NActors;

class TProtectedPageHandler : public NActors::TActor<TProtectedPageHandler> {
    using TBase = NActors::TActor<TProtectedPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TProtectedPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
