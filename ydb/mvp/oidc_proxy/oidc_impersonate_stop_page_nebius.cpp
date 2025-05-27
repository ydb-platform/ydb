#include "openid_connect.h"
#include "oidc_session_create.h"
#include "oidc_cleanup_page.h"
#include "oidc_impersonate_stop_page_nebius.h"

namespace NMVP::NOIDC {

TImpersonateStopPageHandler::TImpersonateStopPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TImpersonateStopPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TImpersonateStopPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    Register(new THandlerCleanup(event->Sender, event->Get()->Request, HttpProxyId, Settings, CreateNameImpersonatedCookie(Settings.ClientId)));
}

} // NMVP::NOIDC
