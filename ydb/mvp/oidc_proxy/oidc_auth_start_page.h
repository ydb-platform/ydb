#pragma once

#include <ydb/mvp/core/mvp_log.h>

#include "oidc_settings.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP::NOIDC {

class THandlerAuthStart
    : public NActors::TActorBootstrapped<THandlerAuthStart>
    , protected TMvpLogContextProvider {
private:
    using TBase = NActors::TActorBootstrapped<THandlerAuthStart>;

protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    THandlerAuthStart(const NActors::TActorId& sender,
                      const NHttp::THttpIncomingRequestPtr& request,
                      const NActors::TActorId& httpProxyId,
                      const TOpenIdConnectSettings& settings);

    void Bootstrap();
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
};

class TAuthStartPageHandler : public NActors::TActor<TAuthStartPageHandler> {
    using TBase = NActors::TActor<TAuthStartPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TAuthStartPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
