#pragma once

#include <ydb/library/actors/core/actor.h>
#include "openid_connect.h"

namespace NMVP {

class TProtectedPageHandler : public NActors::TActor<TProtectedPageHandler> {
    using TBase = NActors::TActor<TProtectedPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TProtectedPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}  // NMVP
