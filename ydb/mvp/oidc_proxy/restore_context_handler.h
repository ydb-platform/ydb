#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP {
namespace NOIDC {

class TContextStorage;

class TRestoreContextHandler : public NActors::TActor<TRestoreContextHandler> {
    using TBase = NActors::TActor<TRestoreContextHandler>;

    const NActors::TActorId HttpProxyId;
    TContextStorage* const ContextStorage;

public:
    TRestoreContextHandler(const NActors::TActorId& httpProxyId, TContextStorage* const contextStorage);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // NOIDC
} // NMVP
