#pragma once

#include "oidc_protected_page_nebius.h"
#include "oidc_protected_page_yandex.h"

namespace NMVP {

class TProtectedPageHandler : public NActors::TActor<TProtectedPageHandler> {
    using TBase = NActors::TActor<TProtectedPageHandler>;

    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TProtectedPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings)
        : TBase(&TProtectedPageHandler::StateWork)
        , HttpProxyId(httpProxyId)
        , Settings(settings)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        switch (Settings.AuthProfile) {
            case NMVP::EAuthProfile::yandex:
                ctx.Register(new THandlerSessionServiceCheckYandex(event->Sender, event->Get()->Request, HttpProxyId, Settings));
                break;
            case NMVP::EAuthProfile::nebius:
                ctx.Register(new THandlerSessionServiceCheckNebius(event->Sender, event->Get()->Request, HttpProxyId, Settings));
                break;
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}  // NMVP
