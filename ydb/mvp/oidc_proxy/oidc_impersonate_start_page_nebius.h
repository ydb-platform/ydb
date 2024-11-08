#pragma once

#include "oidc_settings.h"
#include "context.h"

namespace NMVP {
namespace NOIDC {

class THandlerImpersonateStart : public NActors::TActorBootstrapped<THandlerImpersonateStart> {
private:
    using TBase = NActors::TActorBootstrapped<THandlerImpersonateStart>;

protected:
    const NActors::TActorId Sender;
    const NHttp::THttpIncomingRequestPtr Request;
    NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;
    TContext Context;

public:
    TString DecodeToken(const TStringBuf& cookie, const NActors::TActorContext& ctx);
    THandlerImpersonateStart(const NActors::TActorId& sender,
                             const NHttp::THttpIncomingRequestPtr& request,
                             const NActors::TActorId& httpProxyId,
                             const TOpenIdConnectSettings& settings);
    void RequestImpersonatedToken(const TString&, const TString&, const NActors::TActorContext&);
    void ProcessImpersonatedToken(const TString& impersonatedToken, const NActors::TActorContext& ctx);

    void Bootstrap(const NActors::TActorContext& ctx);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        }
    }
};

class TImpersonateStartPageHandler : public NActors::TActor<TImpersonateStartPageHandler> {
    using TBase = NActors::TActor<TImpersonateStartPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TImpersonateStartPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}  // NOIDC
}  // NMVP
