#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/core/util/wildcard.h>
#include "openid_connect.h"
#include "oidc_protected_page.h"

namespace NMVP::NOIDC {

THandlerSessionServiceCheck::THandlerSessionServiceCheck(const NActors::TActorId& sender,
                                                         const NHttp::THttpIncomingRequestPtr& request,
                                                         const NActors::TActorId& httpProxyId,
                                                         const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
    , ProtectedPageUrl(Request->URL.SubStr(1))
{}

void THandlerSessionServiceCheck::Bootstrap(const NActors::TActorContext& ctx) {
    if (!CheckRequestedHost()) {
        return ReplyAndPassAway(CreateResponseForbiddenHost());
    }
    NHttp::THeaders headers(Request->Headers);
    TStringBuf authHeader = headers.Get(AUTH_HEADER_NAME);
    if (Request->Method == "OPTIONS" || IsAuthorizedRequest(authHeader)) {
        ForwardUserRequest(TString(authHeader));
    } else {
        StartOidcProcess(ctx);
    }
}

void THandlerSessionServiceCheck::HandleProxy(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (event->Get()->Response != nullptr) {
        NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
        BLOG_D("Incoming response for protected resource: " << response->Status);
        if (NeedSendSecureHttpRequest(response)) {
            return SendSecureHttpRequest(response);
        }
        NHttp::THeadersBuilder headers = GetResponseHeaders(response);
        TStringBuf contentType = headers.Get("Content-Type").NextTok(';');
        if (contentType == "text/html") {
            TString newBody = FixReferenceInHtml(response->Body, response->GetRequest()->Host);
            return ReplyAndPassAway(Request->CreateResponse(response->Status, response->Message, headers, newBody));
        } else {
            return ReplyAndPassAway(Request->CreateResponse(response->Status, response->Message, headers, response->Body));
        }
    } else {
        static constexpr size_t MAX_LOGGED_SIZE = 1024;
        BLOG_D("Can not process request to protected resource:\n" << event->Get()->Request->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
        return ReplyAndPassAway(CreateResponseForNotExistingResponseFromProtectedResource(event->Get()->GetError()));
    }
}

bool THandlerSessionServiceCheck::CheckRequestedHost() {
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

bool THandlerSessionServiceCheck::IsAuthorizedRequest(TStringBuf authHeader) {
    if (authHeader.empty()) {
        return false;
    }
    return to_lower(ToString(authHeader)).StartsWith(IAM_TOKEN_SCHEME_LOWER);
}

void THandlerSessionServiceCheck::ForwardUserRequest(TStringBuf authHeader, bool secure) {
    BLOG_D("Forward user request bypass OIDC");
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
    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
}

TString THandlerSessionServiceCheck::FixReferenceInHtml(TStringBuf html, TStringBuf host, TStringBuf findStr) {
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

TString THandlerSessionServiceCheck::FixReferenceInHtml(TStringBuf html, TStringBuf host) {
    TStringBuf findString = "href=";
    auto result = FixReferenceInHtml(html, host, findString);
    findString = "src=";
    return FixReferenceInHtml(result, host, findString);
}

void THandlerSessionServiceCheck::ForwardRequestHeaders(NHttp::THttpOutgoingRequestPtr& request) const {
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

NHttp::THeadersBuilder THandlerSessionServiceCheck::GetResponseHeaders(const NHttp::THttpIncomingResponsePtr& response) {
    static const TVector<TStringBuf> HEADERS_WHITE_LIST = {
        "Content-Type",
        "Connection",
        "X-Worker-Name",
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
    static const TString LOCATION_HEADER_NAME = "Location";
    if (headers.Has(LOCATION_HEADER_NAME)) {
        resultHeaders.Set(LOCATION_HEADER_NAME, GetFixedLocationHeader(headers.Get(LOCATION_HEADER_NAME)));
    }
    return resultHeaders;
}

void THandlerSessionServiceCheck::SendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) {
    NHttp::THttpOutgoingRequestPtr request = response->GetRequest();
    BLOG_D("Try to send request to HTTPS port");
    NHttp::THeadersBuilder headers {request->Headers};
    ForwardUserRequest(headers.Get(AUTH_HEADER_NAME), true);
}

TString THandlerSessionServiceCheck::GetFixedLocationHeader(TStringBuf location) {
    TStringBuf scheme, host, uri;
        NHttp::CrackURL(ProtectedPageUrl, scheme, host, uri);
        if (location.StartsWith("//")) {
            return TStringBuilder() << '/' << (scheme.empty() ? "" : TString(scheme) + "://") << location.SubStr(2);
        } else if (location.StartsWith('/')) {
            return TStringBuilder() << '/'
                                    << (scheme.empty() ? "" : TString(scheme) + "://")
                                    << host << location;
        } else {
            TStringBuf locScheme, locHost, locUri;
            NHttp::CrackURL(location, locScheme, locHost, locUri);
            if (!locScheme.empty()) {
                return TStringBuilder() << '/' << location;
            }
        }
        return TString(location);
}

NHttp::THttpOutgoingResponsePtr THandlerSessionServiceCheck::CreateResponseForbiddenHost() {
    NHttp::THeadersBuilder headers;
    headers.Set("Content-Type", "text/html");
    SetCORS(Request, &headers);

    TStringBuf scheme, host, uri;
    NHttp::CrackURL(ProtectedPageUrl, scheme, host, uri);
    TStringBuilder html;
    html << "<html><head><title>403 Forbidden</title></head><body bgcolor=\"white\"><center><h1>";
    html << "403 Forbidden host: " << host;
    html << "</h1></center></body></html>";

    return Request->CreateResponse("403", "Forbidden", headers, html);
}

NHttp::THttpOutgoingResponsePtr THandlerSessionServiceCheck::CreateResponseForNotExistingResponseFromProtectedResource(const TString& errorMessage) {
    NHttp::THeadersBuilder headers;
    headers.Set("Content-Type", "text/html");
    SetCORS(Request, &headers);

    TStringBuilder html;
    html << "<html><head><title>400 Bad Request</title></head><body bgcolor=\"white\"><center><h1>";
    html << "400 Bad Request. Can not process request to protected resource: " << errorMessage;
    html << "</h1></center></body></html>";
    return Request->CreateResponse("400", "Bad Request", headers, html);
}

void THandlerSessionServiceCheck::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

} // NMVP::NOIDC
