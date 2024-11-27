#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_impersonate_stop_page_nebius.h"
#include <ydb/library/actors/core/events.h>

namespace NMVP::NOIDC {

THandlerImpersonateStop::THandlerImpersonateStop(const NActors::TActorId& sender,
                                                 const NHttp::THttpIncomingRequestPtr& request,
                                                 const NActors::TActorId& httpProxyId,
                                                 const TOpenIdConnectSettings& settings)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void THandlerImpersonateStop::Bootstrap() {
    TString impersonatedCookieName = CreateNameImpersonatedCookie(Settings.ClientId);
    BLOG_D("Clear impersonated cookie: (" << impersonatedCookieName << ")");

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Set-Cookie", ClearSecureCookie(impersonatedCookieName));
    SetCORS(Request, &responseHeaders);

    NHttp::THttpOutgoingResponsePtr httpResponse;
    httpResponse = Request->CreateResponse("200", "OK", responseHeaders);
    ReplyAndPassAway(httpResponse);
}

void THandlerImpersonateStop::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(httpResponse));
    PassAway();
}

TImpersonateStopPageHandler::TImpersonateStopPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TImpersonateStopPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TImpersonateStopPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
    ctx.Register(new THandlerImpersonateStop(event->Sender, event->Get()->Request, HttpProxyId, Settings));
}

} // NMVP::NOIDC
