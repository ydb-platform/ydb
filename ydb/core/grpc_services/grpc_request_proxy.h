#pragma once

#include "grpc_endpoint.h"


#include "grpc_request_proxy_handle_methods.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_control.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimrConfig {
class TAppConfig;
}

namespace NKikimr {

struct TAppData;

namespace NGRpcService {

TString DatabaseFromDomain(const TAppData* appdata);
IActor* CreateGRpcRequestProxy(const NKikimrConfig::TAppConfig& appConfig, TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> tracingControl);
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
        TIntrusiveConstPtr<NACLib::TUserToken> InternalToken;
        bool Retryable;
        NYql::TIssues Issues;

        TEvRefreshTokenResponse(bool ok, const TIntrusiveConstPtr<NACLib::TUserToken>& token, bool retryable, const NYql::TIssues& issues)
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

} // namespace NGRpcService
} // namespace NKikimr
