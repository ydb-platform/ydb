#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_cleanup_page.h"

namespace NMVP::NOIDC {

THandlerCleanup::THandlerCleanup(const NActors::TActorId& sender,
                                 const NHttp::THttpIncomingRequestPtr& request,
                                 const NActors::TActorId& httpProxyId,
                                 const TOpenIdConnectSettings& settings,
                                 const TString& cookieName)
    : Sender(sender)
    , Request(request)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
    , CookieName(cookieName)
{}

void THandlerCleanup::Bootstrap() {
    BLOG_D("Clear cookie: (" << CookieName << ")");

    NHttp::THeadersBuilder responseHeaders;
    responseHeaders.Set("Set-Cookie", ClearSecureCookie(CookieName));
    SetCORS(Request, &responseHeaders);

    ReplyAndPassAway(Request->CreateResponse("200", "OK", responseHeaders));
}

void THandlerCleanup::ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse) {
    Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(httpResponse)));
    PassAway();
}

TCleanupPageHandler::TCleanupPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TCleanupPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TCleanupPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THandlerCleanup(event->Sender, event->Get()->Request, HttpProxyId, Settings, CreateNameSessionCookie(Settings.ClientId)));
}

} // NMVP::NOIDC
