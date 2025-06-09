#pragma once

#include "openid_connect.h"
#include "oidc_protected_page.h"

namespace NMVP::NOIDC {

class THandlerSessionServiceCheckYandex : public THandlerSessionServiceCheck {
private:
    using TBase = THandlerSessionServiceCheck;
    using TSessionService = yandex::cloud::priv::oauth::v1::SessionService;

public:
    THandlerSessionServiceCheckYandex(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings);

    void Bootstrap(const NActors::TActorContext& ctx) override;

    void Handle(TEvPrivate::TEvCheckSessionResponse::TPtr event);
    void Handle(TEvPrivate::TEvErrorResponse::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCheckSessionResponse, Handle);
            hFunc(TEvPrivate::TEvErrorResponse, Handle);
            default:
                TBase::StateWork(ev);
                break;
        }
    }

private:
    void StartOidcProcess(const NActors::TActorContext& ctx) override;
    bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const override;
};

} // NMVP::NOIDC
