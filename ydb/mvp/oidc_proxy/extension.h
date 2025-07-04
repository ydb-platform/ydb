#pragma once

#include "openid_connect.h"

namespace NMVP::NOIDC {

class TExtension : public NActors::TActorBootstrapped<TExtension> {
protected:
    const TOpenIdConnectSettings Settings;
    TIntrusivePtr<TExtensionContext> Context;

public:
    TExtension(const TOpenIdConnectSettings& settings)
        : Settings(settings)
    {}
    virtual void Bootstrap();
    virtual void Handle(TEvPrivate::TEvExtensionRequest::TPtr event) = 0;
    void ReplyAndPassAway(NHttp::THttpOutgoingResponsePtr httpResponse);
    void ReplyAndPassAway();
    void ContinueAndPassAway();

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvExtensionRequest, Handle);
        }
    }

};

} // NMVP::NOIDC
