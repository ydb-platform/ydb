#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>
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
    params.emplace("grant_type", "authorization_code");
    params.emplace("redirect_uri", TStringBuilder() << (Request->Endpoint->Secure ? "https://" : "http://")
                                                          << host
                                                          << GetAuthCallbackUrl());

    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetTokenEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");
    httpRequest->Set("Authorization", Settings.GetAuthorizationString());
    httpRequest->Set<&NHttp::THttpRequest::Body>(params());

    Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    Become(&THandlerSessionCreateNebius::StateWork);
}

void THandlerSessionCreateNebius::ProcessSessionToken(const NJson::TJsonValue& jsonValue) {
    const NJson::TJsonValue* jsonAccessToken;
    const NJson::TJsonValue* jsonExpiresIn;
    if (!jsonValue.GetValuePointer("access_token", &jsonAccessToken)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: access_token not found");
    }
    if (!jsonValue.GetValuePointer("expires_in", &jsonExpiresIn)) {
        return ReplyBadRequestAndPassAway("Wrong OIDC provider response: expires_in not found");
    }
    TString sessionToken = jsonAccessToken->GetStringRobust();
    long long expiresIn = jsonAccessToken->GetIntegerRobust();
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
