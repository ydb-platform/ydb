#pragma once

#include "oidc_protected_page.h"

namespace NMVP::NOIDC {

using namespace NActors;

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
    void HandleExchange(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleProxy);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STFUNC(StateExchange) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleExchange);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    void SendTokenExchangeRequest(const TStringBuilder& body, const ETokenExchangeType exchangeType, const NActors::TActorContext& ctx);
    void ExchangeSessionToken(const TString& sessionToken, const NActors::TActorContext& ctx);
    void ExchangeImpersonatedToken(const TString& sessionToken, const TString& impersonatedToken, const NActors::TActorContext& ctx);
    void ClearImpersonatedCookie(const NActors::TActorContext& ctx);
    void RequestAuthorizationCode(const NActors::TActorContext& ctx);
    void ForwardUserRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure = false) override;
    bool NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const override;
};

} // NMVP::NOIDC
