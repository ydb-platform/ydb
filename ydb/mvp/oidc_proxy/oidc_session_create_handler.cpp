#include "oidc_session_create_handler.h"
#include "oidc_session_create_nebius.h"
#include "oidc_session_create_yandex.h"

namespace NMVP {
namespace NOIDC {

TSessionCreateHandler::TSessionCreateHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
    : TBase(&TSessionCreateHandler::StateWork)
    , HttpProxyId(httpProxyId)
    , Settings(settings)
{}

void TSessionCreateHandler::Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
    NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
    if (request->Method == "GET") {
        switch (Settings.AccessServiceType) {
            case NMvp::yandex_v2:
                ctx.Register(new THandlerSessionCreateYandex(event->Sender, request, HttpProxyId, Settings));
                return;
            case NMvp::nebius_v1:
                ctx.Register(new THandlerSessionCreateNebius(event->Sender, request, HttpProxyId, Settings));
                return;
        }
    }
    auto response = request->CreateResponseBadRequest();
    ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
}

}  // NOIDC
}  // NMVP
