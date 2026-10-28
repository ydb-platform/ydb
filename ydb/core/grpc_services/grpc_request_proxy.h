#pragma once

#include "grpc_endpoint.h"


#include "grpc_request_proxy_handle_methods.h"

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimrConfig {
class TAppConfig;
}

namespace NKikimr {

struct TAppData;

namespace NGRpcService {

IActor* CreateGRpcRequestProxy(const NKikimrConfig::TAppConfig& appConfig);
IActor* CreateGRpcRequestProxySimple(const NKikimrConfig::TAppConfig& appConfig);

class IRequestProxyCtx;

// True when SecurityConfig.UnauthenticatedGrpcMethods lists this non-modifying method.
// A presented token or client certificate is still checked. Missing credentials skip
// only the token requirement; the check actor still resolves the database and rate limits.
bool IsAnonymousGrpcMethodAllowed(const IRequestProxyCtx& ctx);

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
    using TGRpcRequestProxyHandleMethods::Handle;
    void Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx);

    TActorId DiscoveryCacheActorID;
};

} // namespace NGRpcService
} // namespace NKikimr
