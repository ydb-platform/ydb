#pragma once

#include "oidc_session_create.h"
#include "openid_connect.h"

namespace NMVP::NOIDC {

class THandlerSessionCreateYandex : public THandlerSessionCreate {
private:
    using TBase = THandlerSessionCreate;
    using TSessionService = yandex::cloud::priv::oauth::v1::SessionService;

public:
    THandlerSessionCreateYandex(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings);

    void RequestSessionToken(const TString& code) override;
    void ProcessSessionToken(const NJson::TJsonValue& jsonValue) override;
    void HandleCreateSession(TEvPrivate::TEvCreateSessionResponse::TPtr event);
    void HandleError(TEvPrivate::TEvErrorResponse::TPtr event);

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(TEvPrivate::TEvCreateSessionResponse, HandleCreateSession);
            hFunc(TEvPrivate::TEvErrorResponse, HandleError);
        }
    }
};

} // NMVP::NOIDC
