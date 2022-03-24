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

    void Handle(TEvCreateCoordinationNode::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAlterCoordinationNode::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDropCoordinationNode::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeCoordinationNode::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvReadColumnsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvGetShardLocationsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKikhouseDescribeTableRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvS3ListingRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBiStreamPingRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExperimentalStreamQueryRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvStreamPQReadRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQReadInfoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQDropTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQCreateTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQAlterTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQAddReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQRemoveReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQDescribeTopicRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExportToYtRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExportToS3Request::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvImportFromS3Request::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvImportDataRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscoverPQClustersRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCreateRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAlterRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDropRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvListRateLimiterResources::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAcquireRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKikhouseCreateSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKikhouseRefreshSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKikhouseDiscardSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLoginRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCoordinationSessionRequest::TPtr& ev, const TActorContext& ctx);

    TActorId DiscoveryCacheActorID;
};

inline TActorId CreateGRpcRequestProxyId() {
    const auto actorId = TActorId(0, "GRpcReqProxy");
    return actorId;
}

} // namespace NGRpcService
} // namespace NKikimr
