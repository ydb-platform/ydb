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

class THandlerSessionServiceCheckN : public NActors::TActorBootstrapped<THandlerSessionServiceCheckN> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerSessionServiceCheckN>;
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
    THandlerSessionServiceCheckN(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings)
        : Sender(sender)
        , Request(request)
        , HttpProxyId(httpProxyId)
        , Settings(settings)
        , ProtectedPageUrl(Request->URL.SubStr(1))
        {}

    void StartOidcProcess(const NActors::TActorContext& ctx) {
        NHttp::THeaders headers(Request->Headers);
        LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Start OIDC process");

        NHttp::TCookies cookies(headers.Get("Cookie"));
        TString sessionToken = Base64Decode(cookies.Get(CreateNameSessionCookie(Settings.ClientId)));

        if (sessionToken) {
            ExchangeSessionToken(sessionToken, ctx);
        } else {
            RequestAuthorizationCode(ctx);
        }
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        if (!CheckRequestedHost()) {
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponseNotFound(NOT_FOUND_HTML_PAGE, "text/html")));
            Die(ctx);
            return;
        }
        NHttp::THeaders headers(Request->Headers);
        IsAjaxRequest = DetectAjaxRequest(headers);
        TStringBuf authHeader = headers.Get(AUTH_HEADER_NAME);
        if (Request->Method == "OPTIONS" || IsAuthorizedRequest(authHeader)) {
            LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Forward request bypass OIDC");
            ForwardUserRequest(TString(authHeader), ctx);
        } else {
            StartOidcProcess(ctx);
        }
    }

    void HandleProxy(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr httpResponse;
        if (event->Get()->Response != nullptr) {
            NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
            LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Incoming response for protected resource: " << response->Status);
            if ((response->Status == "400" || response->Status.empty()) && RequestedPageScheme.empty()) {
                NHttp::THttpOutgoingRequestPtr request = response->GetRequest();
                if (!request->Secure) {
                    LOG_DEBUG_S(ctx, EService::MVP, "Try to send request to HTTPS port");
                    NHttp::THeadersBuilder headers {request->Headers};
                    ForwardUserRequest(headers.Get(AUTH_HEADER_NAME), ctx, true);
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

    void HandleExchange(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        if (!event->Get()->Response) {
            NHttp::THeadersBuilder ResponseHeaders;
            ResponseHeaders.Set("Content-Type", "text/plain");
            NHttp::THttpOutgoingResponsePtr httpResponse = Request->CreateResponse("400", "Bad Request", ResponseHeaders, event->Get()->Error);
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
            Die(ctx);
        } else {
            NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
            LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Incoming response for getting access token: " << response->Status);
            if (response->Status.StartsWith("2")) {
                LOG_DEBUG_S(ctx, EService::MVP, "SessionService.Check(): OK");

                TString iamToken;
                static NJson::TJsonReaderConfig JsonConfig;
                NJson::TJsonValue requestData;
                bool success = NJson::ReadJsonTree(response->Body, &JsonConfig, &requestData);
                if (success) {
                    iamToken = requestData["access_token"].GetStringSafe({});
                }

                const TString authHeader = IAM_TOKEN_SCHEME + iamToken;
                ForwardUserRequest(authHeader, ctx);
            } else {
                if (response->Status == "400" || response->Status == "401") {
                    StartOidcProcess(ctx);
                    RequestAuthorizationCode(ctx);
                } else {
                    NHttp::THttpOutgoingResponsePtr httpResponse;
                    NHttp::THeadersBuilder ResponseHeaders;
                    ResponseHeaders.Parse(response->Headers);
                    httpResponse = Request->CreateResponse(response->Status, response->Message, ResponseHeaders, response->Body);
                    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
                    Die(ctx);
                }
            }
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

    void ExchangeSessionToken(const TString sessionToken, const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Exchange session token");
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetExchangeEndpoint());
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

        Become(&THandlerSessionServiceCheckN::StateExchange);
    }

    void RequestAuthorizationCode(const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Request authorization code");
        NHttp::THttpOutgoingResponsePtr httpResponse = GetHttpOutgoingResponsePtr(TStringBuf(), Request, Settings, IsAjaxRequest);
        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
        Die(ctx);
    }

    TString GetProtectedPageUrlWithPort(bool secure) {
        TString addressPart;
        TIpPort portPart = 0;
        TStringBuf scheme, host, uri;
        NHttp::CrackURL(ProtectedPageUrl, scheme, host, uri);
        NHttp::CrackAddress(TString(host), addressPart, portPart);
        if (portPart == 0) {
            if (scheme == "http") {
                portPart = 80;
            } else if (scheme == "https") {
                portPart = 443;
            } else {
                portPart = secure ? 443 : 80;
            }
        }
        TStringBuilder sb;
        if (!scheme.empty()) {
            sb << scheme << "://";
        }
        sb << addressPart << ":" << portPart;
        if (!uri.empty()) {
            sb << uri;
        }
        return sb;
    }

    void ForwardUserRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure = false) {
        LOG_DEBUG_S(ctx, EService::MVP, TStringBuilder() << "Forward user request");
        NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequest(Request->Method, GetProtectedPageUrlWithPort(secure));
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
        Become(&THandlerSessionServiceCheckN::StateWork);
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

}  // NMVP
