#pragma once

#include "oidc_settings.h"
#include "context.h"
#include <ydb/library/actors/core/events.h>

namespace NMVP::NOIDC {

class THandlerCleanup : public NActors::TActorBootstrapped<THandlerCleanup> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerCleanup>;

protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    const TString CookieName;

public:
    THandlerCleanup(const NActors::TActorId& sender,
                    const NHttp::THttpIncomingRequestPtr& request,
                    const NActors::TActorId& httpProxyId,
                    const TOpenIdConnectSettings& settings,
                    const TString& cookieName);

    void Bootstrap();
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
};

class TCleanupPageHandler : public NActors::TActor<TCleanupPageHandler> {
    using TBase = NActors::TActor<TCleanupPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TCleanupPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
