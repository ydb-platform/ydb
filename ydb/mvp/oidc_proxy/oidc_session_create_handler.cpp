#include "oidc_session_create_handler.h"
#include "oidc_session_create_nebius.h"
#include "oidc_session_create_yandex.h"

namespace NMVP {

TSessionCreateHandler::TSessionCreateHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TSessionCreateHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TSessionCreateHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
    NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
    if (request->Method == "GET") {
<<<<<<< HEAD
        switch (Settings.AccessServiceType) {
            case NMvp::yandex_v2:
                ctx.Register(new THandlerSessionCreateYandex(event->Sender, request, HttpProxyId, Settings));
                return;
            case NMvp::nebius_v1:
=======
        switch (Settings.AuthProfile) {
            case NMVP::EAuthProfile::YandexV2:
                ctx.Register(new THandlerSessionCreateYandex(event->Sender, request, HttpProxyId, Settings));
                return;
            case NMVP::EAuthProfile::NebiusV1:
>>>>>>> 8e0d57db1b (rewrite GetTableClient)
                ctx.Register(new THandlerSessionCreateNebius(event->Sender, request, HttpProxyId, Settings));
                return;
        }
    }
    auto response = request->CreateResponseBadRequest();
    ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
}

}  // NMVP
