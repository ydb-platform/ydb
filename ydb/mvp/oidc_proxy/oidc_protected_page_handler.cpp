#include "oidc_protected_page_handler.h"
#include "oidc_protected_page_nebius.h"
#include "oidc_protected_page_yandex.h"

namespace NMVP::NOIDC {

TProtectedPageHandler::TProtectedPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TProtectedPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TProtectedPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    switch (Settings.AccessServiceType) {
        case NMvp::yandex_v2:
            Register(new THandlerSessionServiceCheckYandex(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
        case NMvp::nebius_v1:
            Register(new THandlerSessionServiceCheckNebius(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
    }
}

} // NMVP::NOIDC
