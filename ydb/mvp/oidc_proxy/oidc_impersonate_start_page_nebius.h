#pragma once

#include "oidc_settings.h"
#include "context.h"
#include <ydb/library/actors/core/events.h>

namespace NMVP::NOIDC {

class THandlerImpersonateStart : public NActors::TActorBootstrapped<THandlerImpersonateStart> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerImpersonateStart>;

protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    THandlerImpersonateStart(const NActors::TActorId& sender,
                             const NHttp::THttpIncomingRequestPtr& request,
                             const NActors::TActorId& httpProxyId,
                             const TOpenIdConnectSettings& settings);
    void Bootstrap();
    void RequestImpersonatedToken(TString&, TString&);
    void ProcessImpersonatedToken(const NJson::TJsonValue& jsonValue);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
    void ReplyBadRequestAndPassAway(const TString& errorMessage);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        }
    }
};

class TImpersonateStartPageHandler : public NActors::TActor<TImpersonateStartPageHandler> {
    using TBase = NActors::TActor<TImpersonateStartPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TImpersonateStartPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
