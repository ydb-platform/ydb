#pragma once

#include "oidc_protected_page.h"

namespace NMVP {
namespace NOIDC {

class THandlerSessionServiceCheckNebius : public THandlerSessionServiceCheck {
private:
    using TBase = THandlerSessionServiceCheck;

public:
    THandlerSessionServiceCheckNebius(const NActors::TActorId& sender,
                                      const NHttp::THttpIncomingRequestPtr& request,
                                      const NActors::TActorId& httpProxyId,
                                      const TOpenIdConnectSettings& settings);

    void StartOidcProcess(const NActors::TActorContext& ctx) override;
    void HandleExchange(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleProxy);
        }
    }

    STFUNC(StateExchange) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleExchange);
        }
    }

private:

    void ExchangeSessionToken(const TString sessionToken, const NActors::TActorContext& ctx);
    void RequestAuthorizationCode(const NActors::TActorContext& ctx);
    void ForwardUserRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure = false) override;
    bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const override;
};

}  // NOIDC
}  // NMVP
