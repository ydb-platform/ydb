#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/mvp_log.h>
#include "openid_connect.h"
#include "context.h"
#include "oidc_protected_page_nebius.h"

namespace NMVP::NOIDC {

THandlerSessionServiceCheckNebius::THandlerSessionServiceCheckNebius(const NActors::TActorId& sender,
                                                                     const NHttp::THttpIncomingRequestPtr& request,
                                                                     const NActors::TActorId& httpProxyId,
                                                                     const TOpenIdConnectSettings& settings)
    : THandlerSessionServiceCheck(sender, request, httpProxyId, settings)
{}

void THandlerSessionServiceCheckNebius::StartOidcProcess(const NActors::TActorContext& ctx) {
    NHttp::THeaders headers(Request->Headers);
    BLOG_D("Start OIDC process");

    NHttp::TCookies cookies(headers.Get("Cookie"));
    TStringBuf sessionCookieValue = GetCookie(cookies, CreateNameSessionCookie(Settings.ClientId));
    TStringBuf impersonatedCookieValue = GetCookie(cookies, CreateNameImpersonatedCookie(Settings.ClientId));

    TString sessionToken = DecodeToken(sessionCookieValue);
    if (sessionToken) {
        TString impersonatedToken = DecodeToken(impersonatedCookieValue);
        if (impersonatedToken) {
            ExchangeImpersonatedToken(sessionToken, impersonatedToken, ctx);
        } else {
            ExchangeSessionToken(sessionToken, ctx);
        }
    } else{
        RequestAuthorizationCode();
    }
}

void THandlerSessionServiceCheckNebius:: HandleExchange(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
    if (!event->Get()->Response) {
        BLOG_D("Getting access token: Bad Request");
        NHttp::THeadersBuilder responseHeaders;
        responseHeaders.Set("Content-Type", "text/plain");
        NHttp::THttpOutgoingResponsePtr httpResponse = Request->CreateResponse("400", "Bad Request", responseHeaders, event->Get()->Error);
        return ReplyAndPassAway(std::move(httpResponse));
    }

    NHttp::THttpIncomingResponsePtr response = event->Get()->Response;
    BLOG_D("Getting access token: " << response->Status << " " << response->Message);
    if (response->Status == "200") {
        TString iamToken;
        static const NJson::TJsonReaderConfig JsonConfig;
        NJson::TJsonValue requestData;
        bool success = NJson::ReadJsonTree(response->Body, &JsonConfig, &requestData);
        if (success) {
            iamToken = requestData["access_token"].GetStringSafe({});
            const TString authHeader = IAM_TOKEN_SCHEME + iamToken;
            return ForwardUserRequest(authHeader);
        }
    } else if (response->Status == "400" || response->Status == "401") {
        BLOG_D("Getting access token: " << response->Body);
        if (tokenExchangeType == ETokenExchangeType::ImpersonatedToken) {
            return ClearImpersonatedCookie();
        } else {
            return RequestAuthorizationCode();
        }
    }
    // don't know what to do, just forward response
    NHttp::THttpOutgoingResponsePtr httpResponse;
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Parse(response->Headers);
    httpResponse = Request->CreateResponse(response->Status, response->Message, responseHeaders, response->Body);
    ReplyAndPassAway(std::move(httpResponse));
}

void THandlerSessionServiceCheckNebius::SendTokenExchangeRequest(const TCgiParameters& params, const ETokenExchangeType exchangeType, const NActors::TActorContext& ctx) {
    tokenExchangeType = exchangeType;
    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetExchangeEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");

    TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString token = "";
    if (tokenator) {
        token = tokenator->GetToken(Settings.SessionServiceTokenName);
    }
    httpRequest->Set("Authorization", token); // Bearer included
    httpRequest->Set<&NHttp::THttpRequest::Body>(params());

    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    Become(&THandlerSessionServiceCheckNebius::StateExchange);
}

void THandlerSessionServiceCheckNebius::ExchangeSessionToken(TString& sessionToken, const NActors::TActorContext& ctx) {
    BLOG_D("Exchange session token");
    TCgiParameters params;
    params.InsertEscaped("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
    params.InsertEscaped("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
    params.InsertEscaped("subject_token_type", "urn:ietf:params:oauth:token-type:session_token");
    params.InsertEscaped("subject_token", sessionToken);

    SendTokenExchangeRequest(params, ETokenExchangeType::SessionToken, ctx);
}

void THandlerSessionServiceCheckNebius::ExchangeImpersonatedToken(TString& sessionToken, TString& impersonatedToken, const NActors::TActorContext& ctx) {
    BLOG_D("Exchange impersonated token");
    TCgiParameters params;
    params.InsertEscaped("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange");
    params.InsertEscaped("requested_token_type", "urn:ietf:params:oauth:token-type:access_token");
    params.InsertEscaped("subject_token_type", "urn:ietf:params:oauth:token-type:jwt");
    params.InsertEscaped("subject_token", impersonatedToken);
    params.InsertEscaped("actor_token", sessionToken);
    params.InsertEscaped("actor_token_type", "urn:ietf:params:oauth:token-type:session_token");

    SendTokenExchangeRequest(params, ETokenExchangeType::ImpersonatedToken, ctx);
}

void THandlerSessionServiceCheckNebius::ClearImpersonatedCookie() {
    TString impersonatedCookieName = CreateNameImpersonatedCookie(Settings.ClientId);
    BLOG_D("Clear impersonated cookie (" << impersonatedCookieName << ") and retry");
    NHttp::THeadersBuilder responseHeaders;
    SetCORS(Request, &responseHeaders);
    responseHeaders.Set("Set-Cookie", ClearSecureCookie(impersonatedCookieName));
    responseHeaders.Set("Location", Request->URL);

    NHttp::THttpOutgoingResponsePtr httpResponse = Request->CreateResponse("307", "Temporary Redirect", responseHeaders);
    ReplyAndPassAway(std::move(httpResponse));
}

void THandlerSessionServiceCheckNebius::RequestAuthorizationCode() {
    BLOG_D("Request authorization code");
    NHttp::THttpOutgoingResponsePtr httpResponse = GetHttpOutgoingResponsePtr(Request, Settings);
    ReplyAndPassAway(std::move(httpResponse));
}

void THandlerSessionServiceCheckNebius::ForwardUserRequest(TStringBuf authHeader, bool secure) {
    THandlerSessionServiceCheck::ForwardUserRequest(authHeader, secure);
    Become(&THandlerSessionServiceCheckNebius::StateWork);
}

bool THandlerSessionServiceCheckNebius::NeedSendSecureHttpRequest(const NHttp::THttpIncomingResponsePtr& response) const {
    if ((response->Status == "400" || response->Status.empty()) && RequestedPageScheme.empty()) {
        return !response->GetRequest()->Secure;
    }
    return false;
}

} // NMVP::NOIDC
