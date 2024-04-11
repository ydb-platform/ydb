#pragma once

#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/public/api/client/yc_private/oauth/session_service.grpc.pb.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/core/util/wildcard.h>
#include "openid_connect.h"

namespace NMVP {

class THandlerSessionServiceCheck : public NActors::TActorBootstrapped<THandlerSessionServiceCheck> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerSessionServiceCheck>;
    using TSessionService = yandex::cloud::priv::oauth::v1::SessionService;

    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    TString ProtectedPageUrl;
    TString RequestedPageScheme;
    bool  IsAjaxRequest = false;

    const static inline TStringBuf NOT_FOUND_HTML_PAGE = "<html><head><title>404 Not Found</title></head><body bgcolor=\"white\"><center><h1>404 Not Found</h1></center></body></html>";
    const static inline TStringBuf IAM_TOKEN_SCHEME = "Bearer ";
    const static inline TStringBuf AUTH_HEADER_NAME = "Authorization";

public:
    THandlerSessionServiceCheck(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings)
        : Sender(sender)
        , Request(request)
        , HttpProxyId(httpProxyId)
        , Settings(settings)
        , ProtectedPageUrl(Request->URL.SubStr(1))
        {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        if (!CheckRequestedHost()) {
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponseNotFound(NOT_FOUND_HTML_PAGE, "text/html")));
            Die(ctx);
            return;
        }
        NHttp::THeaders headers(Request->Headers);
        TStringBuf authHeader = headers.Get(AUTH_HEADER_NAME);
        if (Request->Method == "OPTIONS" || IsAuthorizedRequest(authHeader)) {
            LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Forward request bypass OIDC");
            ForwardRequest(TString(authHeader), ctx);
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Start OIDC process");
            StartOidcProcess(headers, ctx);
        }

        Become(&THandlerSessionServiceCheck::StateWork);
    }

    void Handle(TEvPrivate::TEvCheckSessionResponse::TPtr event, const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, "SessionService.Check(): OK");
        auto response = event->Get()->Response;
        const auto& iamToken = response.Getiam_token();
        const TString authHeader = IAM_TOKEN_SCHEME + iamToken.Getiam_token();
        ForwardRequest(authHeader, ctx);
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "SessionService.Check(): " << event->Get()->Status);
        NHttp::THttpOutgoingResponsePtr httpResponse;
        if (event->Get()->Status == "400") {
            httpResponse = GetHttpOutgoingResponsePtr(event->Get()->Details, Request, Settings, IsAjaxRequest);
        } else {
            httpResponse = Request->CreateResponse( event->Get()->Status, event->Get()->Message, "text/plain", event->Get()->Details);
        }
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr httpResponse;
        if (event->Get()->Response != nullptr) {
            NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
            if (response->Status == "400" && RequestedPageScheme.empty()) {
                NHttp::THttpOutgoingRequestPtr request = response->GetRequest();
                if (!request->Secure) {
                    LOG_DEBUG_S(ctx, EService::MVP, "Try to send request to HTTPS port");
                    NHttp::THeadersBuilder headers {request->Headers};
                    ForwardRequest(headers.Get(AUTH_HEADER_NAME), ctx, true);
                    return;
                }
            }
            NHttp::THeadersBuilder headers = GetResponseHeaders(response);
            TStringBuf contentType = headers.Get("Content-Type").NextTok(';');
            if (contentType == "text/html") {
                TString newBody = FixReferenceInHtml(response->Body, response->GetRequest()->Host);
                httpResponse = Request->CreateResponse( response->Status, response->Message, headers, newBody);
            } else {
                httpResponse = Request->CreateResponse( response->Status, response->Message, headers, response->Body);
            }
        } else {
            httpResponse = Request->CreateResponseNotFound(NOT_FOUND_HTML_PAGE, "text/html");
        }
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvCheckSessionResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
        }
    }

private:
    bool CheckRequestedHost() {
        size_t pos = ProtectedPageUrl.find('/');
        if (pos == TString::npos) {
            return false;
        }
        TStringBuf scheme, host, uri;
        if (!NHttp::CrackURL(ProtectedPageUrl, scheme, host, uri)) {
            return false;
        }
        if (!scheme.empty() && (scheme != "http" && scheme != "https")) {
            return false;
        }
        RequestedPageScheme = scheme;
        auto it = std::find_if(Settings.AllowedProxyHosts.cbegin(), Settings.AllowedProxyHosts.cend(), [&host] (const TString& wildcard) {
            return NKikimr::IsMatchesWildcard(host, wildcard);
        });
        return it != Settings.AllowedProxyHosts.cend();
    }

    bool IsAuthorizedRequest(TStringBuf authHeader) {
        if (authHeader.empty()) {
            return false;
        }
        return authHeader.StartsWith(IAM_TOKEN_SCHEME);
    }

    void StartOidcProcess(const NHttp::THeaders& headers, const NActors::TActorContext& ctx) {
        TStringBuf cookie = headers.Get("cookie");
        IsAjaxRequest = DetectAjaxRequest(headers);
        yandex::cloud::priv::oauth::v1::CheckSessionRequest request;
        request.Setcookie_header(TString(cookie));

        std::unique_ptr<NYdbGrpc::TServiceConnection<TSessionService>> connection = CreateGRpcServiceConnection<TSessionService>(Settings.SessionServiceEndpoint);

        NActors::TActorSystem* actorSystem = ctx.ActorSystem();
        NActors::TActorId actorId = ctx.SelfID;
        NYdbGrpc::TResponseCallback<yandex::cloud::priv::oauth::v1::CheckSessionResponse> responseCb =
            [actorId, actorSystem](NYdbGrpc::TGrpcStatus&& status, yandex::cloud::priv::oauth::v1::CheckSessionResponse&& response) -> void {
            if (status.Ok()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCheckSessionResponse(std::move(response)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
            }
        };

        NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
        TString token = "";
        if (tokenator) {
            token = tokenator->GetToken(Settings.SessionServiceTokenName);
        }
        NYdbGrpc::TCallMeta meta;
        SetHeader(meta, "authorization", token);
        meta.Timeout = TDuration::Seconds(10);
        connection->DoRequest(request, std::move(responseCb), &yandex::cloud::priv::oauth::v1::SessionService::Stub::AsyncCheck, meta);

    }

    void ForwardRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure = false) {
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequest(Request->Method, ProtectedPageUrl);
        ForwardRequestHeaders(httpRequest);
        if (!authHeader.empty()) {
            httpRequest->Set(AUTH_HEADER_NAME, authHeader);
        }
        if (Request->HaveBody()) {
            httpRequest->SetBody(Request->Body);
        }
        if (RequestedPageScheme.empty()) {
            httpRequest->Secure = secure;
        }
        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    }

    TString FixReferenceInHtml(TStringBuf html, TStringBuf host, TStringBuf findStr) {
        TStringBuilder result;
        size_t n = html.find(findStr);
        if (n == TStringBuf::npos) {
            return TString(html);
        }
        size_t len = findStr.length() + 1;
        size_t pos = 0;
        while (n != TStringBuf::npos) {
            result << html.SubStr(pos, n + len - pos);
            if (html[n + len] == '/') {
                result << "/" << host;
                if (html[n + len + 1] == '\'' || html[n + len + 1] == '\"') {
                    result << "/internal";
                    n++;
                }
            }
            pos = n + len;
            n = html.find(findStr, pos);
        }
        result << html.SubStr(pos);
        return result;
    }

    TString FixReferenceInHtml(TStringBuf html, TStringBuf host) {
        TStringBuf findString = "href=";
        auto result = FixReferenceInHtml(html, host, findString);
        findString = "src=";
        return FixReferenceInHtml(result, host, findString);
    }

    void ForwardRequestHeaders(NHttp::THttpOutgoingRequestPtr& request) {
        static const TVector<TStringBuf> HEADERS_WHITE_LIST = {
            "Connection",
            "Accept-Language",
            "Cache-Control",
            "Sec-Fetch-Dest",
            "Sec-Fetch-Mode",
            "Sec-Fetch-Site",
            "Sec-Fetch-User",
            "Upgrade-Insecure-Requests",
            "Content-Type",
            "Origin"
        };
        NHttp::THeadersBuilder headers(Request->Headers);
        for (const auto& header : HEADERS_WHITE_LIST) {
            if (headers.Has(header)) {
                request->Set(header, headers.Get(header));
            }
        }
        request->Set("Accept-Encoding", "deflate");
    }

    NHttp::THeadersBuilder GetResponseHeaders(const NHttp::THttpIncomingResponsePtr& response) {
        static const TVector<TStringBuf> HEADERS_WHITE_LIST = {
            "Content-Type",
            "Connection",
            "X-Worker-Name",
            "Location",
            "Set-Cookie",
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Credentials",
            "Access-Control-Allow-Headers",
            "Access-Control-Allow-Methods"
        };
        NHttp::THeadersBuilder headers(response->Headers);
        NHttp::THeadersBuilder resultHeaders;
        for (const auto& header : HEADERS_WHITE_LIST) {
            if (headers.Has(header)) {
                resultHeaders.Set(header, headers.Get(header));
            }
        }
        return resultHeaders;
    }
};

class TProtectedPageHandler : public NActors::TActor<TProtectedPageHandler> {
    using TBase = NActors::TActor<TProtectedPageHandler>;

    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TProtectedPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
        : TBase(&TProtectedPageHandler::StateWork)
        , HttpProxyId(httpProxyId)
        , Settings(settings)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        ctx.Register(new THandlerSessionServiceCheck(event->Sender, event->Get()->Request, HttpProxyId, Settings));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}  // NMVP
