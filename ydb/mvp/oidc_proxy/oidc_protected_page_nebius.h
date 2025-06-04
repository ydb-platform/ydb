#pragma once

#include "oidc_protected_page.h"

namespace NMVP::NOIDC {

class THandlerSessionServiceCheckNebius : public THandlerSessionServiceCheck {
private:
    using TBase = THandlerSessionServiceCheck;

protected:
    enum class ETokenExchangeType {
        SessionToken,
        ImpersonatedToken
    };

    ETokenExchangeType tokenExchangeType = ETokenExchangeType::SessionToken;

public:
    THandlerSessionServiceCheckNebius(const NActors::TActorId& sender,
                                      const NHttp::THttpIncomingRequestPtr& request,
                                      const NActors::TActorId& httpProxyId,
                                      const TOpenIdConnectSettings& settings);
    void StartOidcProcess(const NActors::TActorContext& ctx) override;
    void HandleExchange(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event);

    STFUNC(StateExchange) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleExchange);
        }
    }

private:
    void SendTokenExchangeRequest(const TCgiParameters& body, const ETokenExchangeType exchangeType);
    void ExchangeSessionToken(const TString& sessionToken);
    void ExchangeImpersonatedToken(const TString& sessionToken, const TString& impersonatedToken);
    void ClearImpersonatedCookie();
    void RequestAuthorizationCode();
    void ForwardUserRequest(TStringBuf authHeader, bool secure = false) override;
    bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const override;
};

} // NMVP::NOIDC
