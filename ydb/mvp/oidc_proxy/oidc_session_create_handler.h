#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include "oidc_settings.h"

namespace NMVP {
namespace NOIDC {

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

}  // NOIDC
}  // NMVP
