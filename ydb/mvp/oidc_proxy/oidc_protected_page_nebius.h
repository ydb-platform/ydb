#pragma once

#include "oidc_protected_page.h"

namespace NMVP {

class THandlerSessionServiceCheckNebius : public THandlerSessionServiceCheck {
private:
    using TBase = THandlerSessionServiceCheck;

public:
    THandlerSessionServiceCheckNebius(const NActors::TActorId& sender,
                                      const NHttp::THttpIncomingRequestPtr& request,
                                      const NActors::TActorId& httpProxyId,
                                      const TOpenIdConnectSettings& settings)
        : THandlerSessionServiceCheck(sender, request, httpProxyId, settings)
        {}

    void StartOidcProcess(const NActors::TActorContext& ctx) override {
        NHttp::THeaders headers(Request->Headers);
        LOG_DEBUG_S(ctx, EService::MVP, "Start OIDC process");

        NHttp::TCookies cookies(headers.Get("Cookie"));

        TString sessionToken;
        try {
            Base64StrictDecode(cookies.Get(CreateNameSessionCookie(Settings.ClientId)), sessionToken);
        } catch (std::exception& e) {
            LOG_DEBUG_S(ctx, EService::MVP, "Base64Decode session cookie: " << e.what());
            sessionToken.clear();
        }

        if (sessionToken) {
            ExchangeSessionToken(sessionToken, ctx);
        } else {
            RequestAuthorizationCode(ctx);
        }
    }

    void HandleExchange(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        if (!event->Get()->Response) {
            LOG_DEBUG_S(ctx, EService::MVP, "Getting access token: Bad Request");
            NHttp::THeadersBuilder responseHeaders;
            responseHeaders.Set("Content-Type", "text/plain");
            NHttp::THttpOutgoingResponsePtr httpResponse = Request->CreateResponse("400", "Bad Request", responseHeaders, event->Get()->Error);
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
            Die(ctx);
        } else {
            NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
            LOG_DEBUG_S(ctx, EService::MVP, "Getting access token: " << response->Status);
            if (response->Status == "200") {
                TString iamToken;
                static const NJson::TJsonReaderConfig JsonConfig;
                NJson::TJsonValue requestData;
                bool success = NJson::ReadJsonTree(response->Body, &JsonConfig, &requestData);
                if (success) {
                    iamToken = requestData["access_token"].GetStringSafe({});
                    const TString authHeader = IAM_TOKEN_SCHEME + iamToken;
                    ForwardUserRequest(authHeader, ctx);
                    return;
                }
            } else if (response->Status == "400" || response->Status == "401") {
                RequestAuthorizationCode(ctx);
                return;
            }
            // don't know what to do, just forward response
            NHttp::THttpOutgoingResponsePtr httpResponse;
            NHttp::THeadersBuilder responseHeaders;
            responseHeaders.Parse(response->Headers);
            httpResponse = Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body);
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
            Die(ctx);
        }
    }

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

    void ExchangeSessionToken(const TString sessionToken, const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, "Exchange session token");
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetExchangeEndpointURL());
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");

        NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
        TString token = "";
        if (tokenator) {
            token = tokenator->GetToken(Settings.SessionServiceTokenName);
        }
        httpRequest->Set("Authorization", token); // Bearer included
        TStringBuilder body;
        body << "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
             << "&requested_token_type=urn:ietf:params:oauth:token-type:access_token"
             << "&subject_token_type=urn:ietf:params:oauth:token-type:session_token"
             << "&subject_token=" << sessionToken;
        httpRequest->Set<&NHttp::THttpRequest::Body>(body);

        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

        Become(&THandlerSessionServiceCheckNebius::StateExchange);
    }

    void RequestAuthorizationCode(const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, "Request authorization code");
        NHttp::THttpOutgoingResponsePtr httpResponse = GetHttpOutgoingResponsePtr(TStringBuf(), Request, Settings, IsAjaxRequest);
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }

    void ForwardUserRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure = false) override {
        THandlerSessionServiceCheck::ForwardUserRequest(authHeader, ctx, secure);
        Become(&THandlerSessionServiceCheckNebius::StateWork);
    }
};

}  // NMVP
