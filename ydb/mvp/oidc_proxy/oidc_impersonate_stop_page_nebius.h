#pragma once

#include "oidc_settings.h"
#include "context.h"

namespace NMVP {
namespace NOIDC {

class THandlerImpersonateStop : public NActors::TActorBootstrapped<THandlerImpersonateStop> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerImpersonateStop>;

protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    THandlerImpersonateStop(const NActors::TActorId& sender,
                            const NHttp::THttpIncomingRequestPtr& request,
                            const NActors::TActorId& httpProxyId,
                            const TOpenIdConnectSettings& settings);

    void Bootstrap(const NActors::TActorContext& ctx);
};

class TImpersonateStopPageHandler : public NActors::TActor<TImpersonateStopPageHandler> {
    using TBase = NActors::TActor<TImpersonateStopPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TImpersonateStopPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}  // NOIDC
}  // NMVP
