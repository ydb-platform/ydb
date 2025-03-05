#include "oidc_session_create_handler.h"
#include "oidc_session_create_nebius.h"
#include "oidc_session_create_yandex.h"

namespace NMVP::NOIDC {

TSessionCreateHandler::TSessionCreateHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TSessionCreateHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TSessionCreateHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
    NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
    if (request->Method == "GET") {
        switch (Settings.AccessServiceType) {
            case NMvp::yandex_v2:
                Register(new THandlerSessionCreateYandex(event->Sender, request, HttpProxyId, Settings));
                return;
            case NMvp::nebius_v1:
                Register(new THandlerSessionCreateNebius(event->Sender, request, HttpProxyId, Settings));
                return;
        }
    }
    auto response = request->CreateResponseBadRequest();
    Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
}

} // NMVP::NOIDC
