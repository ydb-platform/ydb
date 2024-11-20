#pragma once

#include "oidc_session_create.h"
#include "openid_connect.h"

namespace NMVP::NOIDC {

using namespace NActors;

class THandlerSessionCreateYandex : public THandlerSessionCreate {
private:
    using TBase = THandlerSessionCreate;
    using TSessionService = yandex::cloud::priv::oauth::v1::SessionService;

public:
    THandlerSessionCreateYandex(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings);

    void RequestSessionToken(const TString& code, const NActors::TActorContext& ctx) override;
    void ProcessSessionToken(const TString& sessionToken, const NActors::TActorContext& ctx) override;
    void HandleCreateSession(TEvPrivate::TEvCreateSessionResponse::TPtr event, const NActors::TActorContext& ctx);
    void HandleError(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvCreateSessionResponse, HandleCreateSession);
            HFunc(TEvPrivate::TEvErrorResponse, HandleError);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
