#pragma once

#include "oidc_session_create.h"

namespace NMVP {

class THandlerSessionCreateNebius : public THandlerSessionCreate {
private:
    using TBase = THandlerSessionCreate;

public:
    THandlerSessionCreateNebius(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings)
        : THandlerSessionCreate(sender, request, httpProxyId, settings)
        {}

    void RemoveAppliedCookie(const TString& cookieName) override {
        ResponseHeaders.Set("Set-Cookie", TStringBuilder() << cookieName << "=; Path=" << GetAuthCallbackUrl() << "; Max-Age=0");
        ResponseHeaders.Set("Set-Cookie", TStringBuilder() << CreateNameSessionCookie(Settings.ClientId) << "=; Max-Age=0");
    }

    void RequestSessionToken(const TString& code, const NActors::TActorContext& ctx) override {
        TStringBuilder body;
        TStringBuf host = Request->Host;
        body << "code=" << code
             << "&client_id=" << Settings.ClientId
             << "&grant_type=authorization_code"
             << "&redirect_uri="
             << (Request->Endpoint->Secure ? "https://" : "http://")
             << host
             << GetAuthCallbackUrl();

        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetTokenEndpointURL());
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");
        httpRequest->Set("Authorization", Settings.GetAuthorizationString());
        httpRequest->Set<&NHttp::THttpRequest::Body>(body);

        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        Become(&THandlerSessionCreateNebius::StateWork);
    }

    virtual void ProcessSessionToken(const TString& sessionToken, const NActors::TActorContext& ctx) override {
        ResponseHeaders.Set("Set-Cookie", CreateSecureCookie(Settings.ClientId, sessionToken));
        ResponseHeaders.Set("Location", RedirectUrl);
        NHttp::THttpOutgoingResponsePtr httpResponse;
        httpResponse = Request->CreateResponse("302", "Cookie set", ResponseHeaders);
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        }
    }
};

}  // NMVP
