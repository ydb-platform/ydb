#pragma once

#include "oidc_session_create.h"

namespace NMVP {
namespace NOIDC {

class TContextStorage;

class THandlerSessionCreateNebius : public THandlerSessionCreate {
private:
    using TBase = THandlerSessionCreate;

public:
    THandlerSessionCreateNebius(const NActors::TActorId& sender,
                                const NHttp::THttpIncomingRequestPtr& request,
                                const NActors::TActorId& httpProxyId,
                                const TOpenIdConnectSettings& settings,
                                TContextStorage* const contextStorage);

    void RequestSessionToken(const TString& code, const NActors::TActorContext& ctx) override;
    void ProcessSessionToken(const TString& sessionToken, const NActors::TActorContext& ctx) override;

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        }
    }
};

}  // NOIDC
}  // NMVP
