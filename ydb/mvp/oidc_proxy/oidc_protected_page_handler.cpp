#include "oidc_protected_page_handler.h"
#include "oidc_protected_page_nebius.h"
#include "oidc_protected_page_yandex.h"

namespace NMVP {
TProtectedPageHandler::TProtectedPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TProtectedPageHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TProtectedPageHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
    switch (Settings.AccessServiceType) {
        case NMVP::EAccessServiceType::YandexV2:
            ctx.Register(new THandlerSessionServiceCheckYandex(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
        case NMVP::EAccessServiceType::NebiusV1:
            ctx.Register(new THandlerSessionServiceCheckNebius(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
    }
}

}  // NMVP
