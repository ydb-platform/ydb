#pragma once

#include "grpc_endpoint.h"

#include "rpc_calls.h"

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

class TGRpcRequestProxy : public IFacilityProvider {
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

    void Handle(TEvBiStreamPingRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvStreamPQMigrationReadRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvStreamTopicWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvStreamTopicReadRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCommitOffsetRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQReadInfoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQDropTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQCreateTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQAlterTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQAddReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQRemoveReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQDescribeTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscoverPQClustersRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLoginRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvNodeCheckRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCoordinationSessionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDropTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCreateTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAlterTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeConsumerRequest::TPtr& ev, const TActorContext& ctx);

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
