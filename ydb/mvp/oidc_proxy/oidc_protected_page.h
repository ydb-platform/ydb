#pragma once

#include <util/generic/algorithm.h>
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
protected:
    using TBase = NActors::TActorBootstrapped<THandlerSessionServiceCheck>;

    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    TString ProtectedPageUrl;
    TString RequestedPageScheme;
    bool IsAjaxRequest = false;

    const static inline TStringBuf NOT_FOUND_HTML_PAGE = "<html><head><title>404 Not Found</title></head><body bgcolor=\"white\"><center><h1>404 Not Found</h1></center></body></html>";
    const static inline TStringBuf IAM_TOKEN_SCHEME = "Bearer ";
    const static inline TStringBuf IAM_TOKEN_SCHEME_LOWER = "bearer ";
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


    virtual void Bootstrap(const NActors::TActorContext& ctx) {
        if (!CheckRequestedHost()) {
            ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request->CreateResponseNotFound(NOT_FOUND_HTML_PAGE, "text/html")));
            Die(ctx);
            return;
        }
        NHttp::THeaders headers(Request->Headers);
        IsAjaxRequest = DetectAjaxRequest(headers);
        TStringBuf authHeader = headers.Get(AUTH_HEADER_NAME);
        if (Request->Method == "OPTIONS" || IsAuthorizedRequest(authHeader)) {
            ForwardUserRequest(TString(authHeader), ctx);
        } else {
            StartOidcProcess(ctx);
        }
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr httpResponse;
        if (event->Get()->Response != nullptr) {
            NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
            LOG_DEBUG_S(ctx, EService::MVP, "Incoming response for protected resource: " << response->Status);
            if (response->Status == "400" && RequestedPageScheme.empty()) {
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

protected:
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
        return to_lower(ToString(authHeader)).StartsWith(IAM_TOKEN_SCHEME_LOWER);
    }

    virtual void StartOidcProcess(const NActors::TActorContext& ctx) = 0;

    virtual void ForwardUserRequest(TStringBuf authHeader, const NActors::TActorContext& ctx, bool secure = false) {
        LOG_DEBUG_S(ctx, EService::MVP, "Forward user request bypass OIDC");
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

}  // NMVP
