#pragma once

#include "grpc_endpoint.h"


#include "grpc_request_proxy_handle_methods.h"

#include <library/cpp/actors/core/actor.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimrConfig {
class TAppConfig;
}

namespace NKikimr {

struct TAppData;

namespace NGRpcService {

TString DatabaseFromDomain(const TAppData* appdata);
IActor* CreateGRpcRequestProxy(const NKikimrConfig::TAppConfig& appConfig);
IActor* CreateGRpcRequestProxySimple(const NKikimrConfig::TAppConfig& appConfig);

class TGRpcRequestProxy : public TGRpcRequestProxyHandleMethods, public IFacilityProvider {
public:
    enum EEv {
        EvRefreshTokenResponse = EventSpaceBegin(TKikimrEvents::ES_GRPC_REQUEST_PROXY),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_REQUEST_PROXY),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_REQUEST_PROXY)");

    struct TEvRefreshTokenResponse : public TEventLocal<TEvRefreshTokenResponse, EvRefreshTokenResponse> {
        bool Authenticated;
        TString InternalToken;
        bool Retryable;
        NYql::TIssues Issues;

        TEvRefreshTokenResponse(bool ok, const TString& token, bool retryable, const NYql::TIssues& issues)
            : Authenticated(ok)
            , InternalToken(token)
            , Retryable(retryable)
            , Issues(issues)
        {}
    };

protected:
    void Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx);

    TActorId DiscoveryCacheActorID;
};

inline TActorId CreateGRpcRequestProxyId(int n = 0) {
    if (n == 0) {
        const auto actorId = TActorId(0, "GRpcReqProxy");
        return actorId;
    }

    const auto actorId = TActorId(0, TStringBuilder() << "GRpcReqPro" << n);
    return actorId;
}

} // namespace NGRpcService
} // namespace NKikimr
