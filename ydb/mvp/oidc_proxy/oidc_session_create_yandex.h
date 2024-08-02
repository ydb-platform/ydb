#pragma once

#include "oidc_session_create.h"

namespace NMVP {

class THandlerSessionCreateYandex : public THandlerSessionCreate {
private:
    using TBase = THandlerSessionCreate;

public:
    THandlerSessionCreateYandex(const NActors::TActorId& sender,
                          const NHttp::THttpIncomingRequestPtr& request,
                          const NActors::TActorId& httpProxyId,
                          const TOpenIdConnectSettings& settings)
        : THandlerSessionCreate(sender, request, httpProxyId, settings)
        {}

    void RemoveAppliedCookie(const TString& cookieName) override {
        ResponseHeaders.Set("Set-Cookie", TStringBuilder() << cookieName << "=; Path=" << GetAuthCallbackUrl() << "; Max-Age=0");
    }

    void RequestSessionToken(const TString& code, const NActors::TActorContext& ctx) override {
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetTokenEndpointURL());
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");
        httpRequest->Set("Authorization", Settings.GetAuthorizationString());
        TStringBuilder body;
        body << "grant_type=authorization_code&code=" << code;
        httpRequest->Set<&NHttp::THttpRequest::Body>(body);
        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        Become(&THandlerSessionCreateYandex::StateWork);
    }


    virtual void ProcessSessionToken(const TString& sessionToken, const NActors::TActorContext& ctx) override {
        std::unique_ptr<NYdbGrpc::TServiceConnection<TSessionService>> connection = CreateGRpcServiceConnection<TSessionService>(Settings.SessionServiceEndpoint);

        yandex::cloud::priv::oauth::v1::CreateSessionRequest requestCreate;
        requestCreate.Setaccess_token(sessionToken);

        NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
        TString token = "";
        if (tokenator) {
            token = tokenator->GetToken(Settings.SessionServiceTokenName);
        }
        NYdbGrpc::TCallMeta meta;
        SetHeader(meta, "authorization", token);
        meta.Timeout = TDuration::Seconds(10);

        NActors::TActorSystem* actorSystem = ctx.ActorSystem();
        NActors::TActorId actorId = ctx.SelfID;
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::oauth::v1::CreateSessionResponse> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::oauth::v1::CreateSessionResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };
        connection->DoRequest(requestCreate, std::move(responseCb), &yandex::cloud::priv::oauth::v1::SessionService::Stub::AsyncCreate, meta);
    }

    void HandleCreateSession(TEvPrivate::TEvCreateSessionResponse::TPtr event, const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, "SessionService.Create(): OK");
        auto response = event->Get()->Response;
        for (const auto& cookie : response.Getset_cookie_header()) {
            ResponseHeaders.Set("Set-Cookie", ChangeSameSiteFieldInSessionCookie(cookie));
        }
        NHttp::THttpOutgoingResponsePtr httpResponse;
        ResponseHeaders.Set("Location", RedirectUrl);
        httpResponse = Request->CreateResponse("302", "Cookie set", ResponseHeaders);
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }

    void HandleError(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, "SessionService.Create(): " << event->Get()->Status);
        NHttp::THttpOutgoingResponsePtr httpResponse;
        if (event->Get()->Status == "400") {
            httpResponse = GetHttpOutgoingResponsePtr(event->Get()->Details, Request, Settings, ResponseHeaders, IsAjaxRequest);
        } else {
            ResponseHeaders.Set("Content-Type", "text/plain");
            httpResponse = Request->CreateResponse( event->Get()->Status, event->Get()->Message, ResponseHeaders, event->Get()->Details);
        }
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvCreateSessionResponse, HandleCreateSession);
            HFunc(TEvPrivate::TEvErrorResponse, HandleError);
        }
    }
};

}  // NMVP
