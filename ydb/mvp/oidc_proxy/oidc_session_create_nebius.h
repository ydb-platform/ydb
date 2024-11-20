#pragma once

#include "oidc_session_create.h"

namespace NMVP::NOIDC {

using namespace NActors;

class THandlerSessionCreateNebius : public THandlerSessionCreate {
private:
    using TBase = THandlerSessionCreate;

public:
    THandlerSessionCreateNebius(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings);

    void RequestSessionToken(const TString& code, const NActors::TActorContext& ctx) override;
    void ProcessSessionToken(const TString& sessionToken, const NActors::TActorContext& ctx) override;

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
