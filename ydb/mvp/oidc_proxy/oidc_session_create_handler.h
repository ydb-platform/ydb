#pragma once

#include "oidc_session_create_nebius.h"
#include "oidc_session_create_yandex.h"

namespace NMVP {

class TSessionCreateHandler : public NActors::TActor<TSessionCreateHandler> {
    using TBase = NActors::TActor<TSessionCreateHandler>;

    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TSessionCreateHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
        : TBase(&TSessionCreateHandler::StateWork)
        , HttpProxyId(httpProxyId)
        , Settings(settings)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            switch (Settings.AuthProfile) {
                case NMVP::EAuthProfile::Yandex:
                    ctx.Register(new THandlerSessionCreateYandex(event->Sender, request, HttpProxyId, Settings));
                    return;
                case NMVP::EAuthProfile::Nebius:
                    ctx.Register(new THandlerSessionCreateNebius(event->Sender, request, HttpProxyId, Settings));
                    return;
            }
        }
        auto response = request->CreateResponseBadRequest();
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}  // NMVP
