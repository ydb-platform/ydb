#pragma once

#include <ydb/mvp/core/mvp_log.h>

#include "oidc_settings.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP::NOIDC {

struct TRestoreOidcContextResult;

class THandlerAuthCallbackContext
    : public NActors::TActorBootstrapped<THandlerAuthCallbackContext>
    , protected TMvpLogContextProvider {
private:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    const TOpenIdConnectSettings Settings;

public:
    THandlerAuthCallbackContext(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const TOpenIdConnectSettings& settings);

    void Bootstrap();
    NHttp::THttpOutgoingResponsePtr CreateTextResponse(TStringBuf status, TStringBuf message, TStringBuf body = "") const;
    NHttp::THttpOutgoingResponsePtr CreateJsonResponse(TStringBuf requestedAddress) const;
    void ReplyContextRestoreFailureAndPassAway(TStringBuf flowId, const TRestoreOidcContextResult& restoreContextResult);
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr response);
};

class TAuthCallbackContextHandler : public NActors::TActor<TAuthCallbackContextHandler> {
private:
    using TBase = NActors::TActor<TAuthCallbackContextHandler>;
    const TOpenIdConnectSettings Settings;

public:
    explicit TAuthCallbackContextHandler(const TOpenIdConnectSettings& settings);

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
