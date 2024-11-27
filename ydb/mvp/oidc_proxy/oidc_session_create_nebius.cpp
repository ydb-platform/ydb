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
    CGIEscape(code);

    TStringBuilder body;
    body << "code=" << code
         << "&client_id=" << code
         << "&grant_type=authorization_code"
         << "&redirect_uri="
         << (Request->Endpoint->Secure ? "https://" : "http://")
         << host
         << GetAuthCallbackUrl();

    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Settings.GetTokenEndpointURL());
    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-www-form-urlencoded");
    httpRequest->Set("Authorization", Settings.GetAuthorizationString());
    httpRequest->Set<&NHttp::THttpRequest::Body>(body);

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
    ReplyAndPassAway(httpResponse);
}

} // NMVP::NOIDC
