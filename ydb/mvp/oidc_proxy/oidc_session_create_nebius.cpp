#include <ydb/library/actors/http/http.h>
#include "openid_connect.h"
#include "oidc_session_create_nebius.h"

namespace NMVP {
namespace NOIDC {

THandlerSessionCreateNebius::THandlerSessionCreateNebius(const NActors::TActorId& sender,
                                                         const NHttp::THttpIncomingRequestPtr& request,
                                                         const NActors::TActorId& httpProxyId,
                                                         const TOpenIdConnectSettings& settings)
    : THandlerSessionCreate(sender, request, httpProxyId, settings)
{}

void THandlerSessionCreateNebius::RequestSessionToken(const TString& code, const NActors::TActorContext& ctx) {
    TStringBuilder body;
    TStringBuf host = Request->Host;
    body << "code=" << code
         << "&client_id=" << Settings.ClientId
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

void THandlerSessionCreateNebius::ProcessSessionToken(const TString& sessionToken, const NActors::TActorContext& ctx) {
    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Set-Cookie", CreateSecureCookie(Settings.ClientId, sessionToken));
    responseHeaders.Set("Location", Context.GetRequestedAddress());
    NHttp::THttpOutgoingResponsePtr httpResponse;
    httpResponse = Request->CreateResponse("302", "Cookie set", responseHeaders);
    ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    Die(ctx);
}

} // NOIDC
} // NMVP
