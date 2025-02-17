#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_impersonate_start_page_nebius.h"

namespace NMVP::NOIDC {

THandlerImpersonateStart::THandlerImpersonateStart(const NActors::TActorId& sender,
                                                   const NHttp::THttpIncomingRequestPtr& request,
                                                   const NActors::TActorId& httpProxyId,
                                                   const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerImpersonateStart::Bootstrap() {
    BLOG_D("Start impersonation process");

    NHttp::TUrlParameters urlParameters(Request->URL);
    TString serviceAccountId = urlParameters["service_account_id"];

    NHttp::THeaders headers(Request->Headers);
    NHttp::TCookies cookies(headers.Get("Cookie"));

    TStringBuf sessionCookieValue = GetCookie(cookies, CreateNameSessionCookie(Settings.ClientId));
    TString sessionToken = DecodeToken(sessionCookieValue);
    TStringBuf impersonatedCookieValue = GetCookie(cookies, CreateNameImpersonatedCookie(Settings.ClientId));

    if (sessionToken.empty()) {
        return ReplyBadRequestAndPassAway("Wrong impersonate parameter: session cookie not found");
    }
    if (!impersonatedCookieValue.empty()) {
        return ReplyBadRequestAndPassAway("Wrong impersonate parameter: impersonated cookie already exists");
    }
    if (serviceAccountId.empty()) {
        return ReplyBadRequestAndPassAway("Wrong impersonate parameter: service_account_id not found");
    }

    RequestImpersonatedToken(sessionToken, serviceAccountId);
}

void THandlerImpersonateStart::RequestImpersonatedToken(TString& sessionToken, TString& serviceAccountId) {
    BLOG_D("Request impersonated token");
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetImpersonateEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");

    TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString token = "";
    if (tokenator) {
        token = tokenator->GetToken(Settings.SessionServiceTokenName);
    }
    httpRequest->Set("Authorization", token); // Bearer included

    TCgiParameters params;
    params.emplace("session", sessionToken);
    params.emplace("service_account_id", serviceAccountId);
    httpRequest->Set<&NHttp::THttpRequest::Body>(params());

    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    Become(&THandlerImpersonateStart::StateWork);
}

void THandlerImpersonateStart::ProcessImpersonatedToken(const NJson::TJsonValue& jsonValue) {
    const NJson::TJsonValue* jsonImpersonatedToken;
    const NJson::TJsonValue* jsonExpiresIn;
    TString impersonatedToken;
    unsigned long long expiresIn;
    if (!jsonValue.GetValuePointer("impersonation", &jsonImpersonatedToken)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: `impersonation` not found");
    }
    if (!jsonImpersonatedToken->GetString(&impersonatedToken)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: failed to extract `impersonation`");
    }
    if (!jsonValue.GetValuePointer("expires_in", &jsonExpiresIn)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: `expires_in` not found");
    }
    if (!jsonExpiresIn->GetUInteger(&expiresIn)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: failed to extract `expires_in`");
    }
    expiresIn = std::min(expiresIn, static_cast<unsigned long long>(TDuration::Days(7).Seconds())); // clean cookies no less than once a week.
    TString impersonatedCookieName = CreateNameImpersonatedCookie(Settings.ClientId);
    TString impersonatedCookieValue = Base64Encode(impersonatedToken);
    BLOG_D("Set impersonated cookie: (" << impersonatedCookieName << ": " << NKikimr::MaskTicket(impersonatedCookieValue) << ")");

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Set-Cookie", CreateSecureCookie(impersonatedCookieName, impersonatedCookieValue, expiresIn));
    SetCORS(Request, &responseHeaders);
    ReplyAndPassAway(Request->CreateResponse("200", "OK", responseHeaders));
}

void THandlerImpersonateStart::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (event->Get()->Error.empty() && event->Get()->Response) {
        NHttp::THttpIncomingResponsePtr response = std::move(event->Get()->Response);
        BLOG_D("Incoming response from authorization server: " << response->Status);
        if (response->Status == "200") {
            NJson::TJsonValue jsonValue;
            NJson::TJsonReaderConfig jsonConfig;
            if (NJson::ReadJsonTree(response->Body, &jsonConfig, &jsonValue)) {
                return ProcessImpersonatedToken(jsonValue);
            }
            return ReplyBadRequestAndPassAway("Wrong OIDC response");
        } else {
            NHttp::THeadersBuilder responseHeaders;
            NHttp::THeaders headers(response->Headers);
            if (headers.Has("Content-Type")) {
                responseHeaders.Set("Content-Type", headers.Get("Content-Type"));
            }
            SetCORS(Request, &responseHeaders);
            return ReplyAndPassAway(Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body));
        }
    }

    ReplyBadRequestAndPassAway(event->Get()->Error);
}

void THandlerImpersonateStart::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

void THandlerImpersonateStart::ReplyBadRequestAndPassAway(const TString& errorMessage) {
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Content-Type", "text/plain");
    SetCORS(Request, &responseHeaders);
    ReplyAndPassAway(Request->CreateResponse("400", "Bad Request", responseHeaders, errorMessage));
}

TImpersonateStartPageHandler::TImpersonateStartPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TImpersonateStartPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TImpersonateStartPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THandlerImpersonateStart(event->Sender, event->Get()->Request, HttpProxyId, Settings));
}

} // NMVP::NOIDC
