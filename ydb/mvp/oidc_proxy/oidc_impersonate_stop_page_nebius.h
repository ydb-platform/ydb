#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
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
    TContext Context;

public:
    THandlerImpersonateStop(const NActors::TActorId& sender,
                          const NHttp::THttpIncomingRequestPtr& request,
                          const NActors::TActorId& httpProxyId,
                          const TOpenIdConnectSettings& settings);

    void Bootstrap(const NActors::TActorContext& ctx);
};

}  // NOIDC
}  // NMVP
