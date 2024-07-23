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
<<<<<<< HEAD
<<<<<<< HEAD
    switch (Settings.AccessServiceType) {
        case NMvp::yandex_v2:
            ctx.Register(new THandlerSessionServiceCheckYandex(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
        case NMvp::nebius_v1:
=======
    switch (Settings.AuthProfile) {
        case NMVP::EAuthProfile::YandexV2:
            ctx.Register(new THandlerSessionServiceCheckYandex(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
        case NMVP::EAuthProfile::NebiusV1:
>>>>>>> 8e0d57db1b (rewrite GetTableClient)
=======
    switch (Settings.AccessServiceType) {
        case NMVP::EAccessServiceType::YandexV2:
            ctx.Register(new THandlerSessionServiceCheckYandex(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
        case NMVP::EAccessServiceType::NebiusV1:
>>>>>>> b14ae95980 (renamed EAuth profile to EAccessServiceTypeEAccessServiceType)
            ctx.Register(new THandlerSessionServiceCheckNebius(event->Sender, event->Get()->Request, HttpProxyId, Settings));
            break;
    }
}

}  // NMVP
