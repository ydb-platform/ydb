#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include "openid_connect.h"
#include "oidc_session_create_nebius.h"
#include <library/cpp/string_utils/base64/base64.h>

namespace NMVP::NOIDC {

THandlerSessionCreateNebius::THandlerSessionCreateNebius(const NActors::TActorId& sender,
                                                         const NHttp::THttpIncomingRequestPtr& request,
                                                         const NActors::TActorId& httpProxyId,
                                                         const TOpenIdConnectSettings& settings)
    : THandlerSessionCreate(sender, request, httpProxyId, settings)
{}

void THandlerSessionCreateNebius::RequestSessionToken(const TString& code) {
    TStringBuf host = Request->Host;

    TCgiParameters params;
    params.emplace("code", code);
    params.emplace("client_id", code);
    params.emplace("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:access_token_bearer");
    params.emplace("grant_type", "authorization_code");
    params.emplace("redirect_uri", TStringBuilder() << (Request->Endpoint->Secure ? "https://" : "http://")
                                                          << host
                                                          << GetAuthCallbackUrl());

    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetTokenEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");

    TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString token = "";
    if (tokenator) {
        token = tokenator->GetToken(Settings.SessionServiceTokenName);
    }
    httpRequest->Set("Authorization", token); // Bearer included
    httpRequest->Set<&NHttp::THttpRequest::Body>(params());

    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    Become(&THandlerSessionCreateNebius::StateWork);
}

void THandlerSessionCreateNebius::ProcessSessionToken(const NJson::TJsonValue& jsonValue) {
    const NJson::TJsonValue* jsonAccessToken;
    const NJson::TJsonValue* jsonExpiresIn;
    TString sessionToken;
    unsigned long long expiresIn;
    if (!jsonValue.GetValuePointer("access_token", &jsonAccessToken)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: `access_token` not found");
    }
    if (!jsonAccessToken->GetString(&sessionToken)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: failed to extract `access_token`");
    }
    if (!jsonValue.GetValuePointer("expires_in", &jsonExpiresIn)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: `expires_in` not found");
    }
    if (!jsonExpiresIn->GetUInteger(&expiresIn)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: failed to extract `expires_in`");
    }
    expiresIn = std::min(expiresIn, static_cast<unsigned long long>(TDuration::Days(7).Seconds())); // clean cookies no less than once a week.
    TString sessionCookieName = CreateNameSessionCookie(Settings.ClientId);
    TString sessionCookieValue = Base64Encode(sessionToken);
    BLOG_D("Set session cookie: (" << sessionCookieName << ": " << NKikimr::MaskTicket(sessionCookieValue) << ")");

    NHttp::THeadersBuilder responseHeaders;
    SetCORS(Request, &responseHeaders);
    responseHeaders.Set("Set-Cookie", CreateSecureCookie(sessionCookieName, sessionCookieValue, expiresIn));
    responseHeaders.Set("Location", Context.GetRequestedAddress());
    ReplyAndPassAway(Request->CreateResponse("302", "Cookie set", responseHeaders));
}

} // NMVP::NOIDC
