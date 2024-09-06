#pragma once

#include <ydb/library/actors/core/actor.h>
#include "openid_connect.h"

namespace NMVP {

class TSessionCreateHandler : public NActors::TActor<TSessionCreateHandler> {
    using TBase = NActors::TActor<TSessionCreateHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TSessionCreateHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

}  // NMVP
