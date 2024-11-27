#include <ydb/library/actors/http/http.h>
#include <ydb/library/security/util.h>
#include "openid_connect.h"
#include "oidc_session_create_nebius.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

namespace NMVP::NOIDC {

THandlerSessionCreateNebius::THandlerSessionCreateNebius(const NActors::TActorId& sender,
                                                         const NHttp::THttpIncomingRequestPtr& request,
                                                         const NActors::TActorId& httpProxyId,
                                                         const TOpenIdConnectSettings& settings)
    : THandlerSessionCreate(sender, request, httpProxyId, settings)
{}

void THandlerSessionCreateNebius::RequestSessionToken(TString& code, const NActors::TActorContext& ctx) {
    TStringBuf host = Request->Host;

    TCgiParameters params;
    params.InsertEscaped("code", code);
    params.InsertEscaped("client_id", code);
    params.InsertEscaped("grant_type", "authorization_code");
    params.InsertEscaped("redirect_uri", TStringBuilder() << (Request->Endpoint->Secure ? "https://" : "http://")
                                                          << host
                                                          << GetAuthCallbackUrl());

    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetTokenEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");
    httpRequest->Set("Authorization", Settings.GetAuthorizationString());
    httpRequest->Set<&NHttp::THttpRequest::Body>(params());

    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    Become(&THandlerSessionCreateNebius::StateWork);
}

void THandlerSessionCreateNebius::ProcessSessionToken(const TString& sessionToken, const NActors::TActorContext& ) {
    TString sessionCookieName = CreateNameSessionCookie(Settings.ClientId);
    TString sessionCookieValue = Base64Encode(sessionToken);
    BLOG_D("Set session cookie: (" << sessionCookieName << ": " << NKikimr::MaskTicket(sessionCookieValue) << ")");

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Set-Cookie", CreateSecureCookie(sessionCookieName, sessionCookieValue));
    responseHeaders.Set("Location", Context.GetRequestedAddress());
    NHttp::THttpOutgoingResponsePtr httpResponse;
    httpResponse = Request->CreateResponse("302", "Cookie set", responseHeaders);
    ReplyAndPassAway(std::move(httpResponse));
}

} // NMVP::NOIDC
