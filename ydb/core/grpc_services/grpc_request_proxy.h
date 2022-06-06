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
    void Handle(TEvAlterTableRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCreateTableRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDropTableRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCopyTableRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCopyTablesRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvRenameTablesRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeTableRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvGetOperationRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCancelOperationRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvForgetOperationRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvListOperationsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCreateSessionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKeepAliveRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDeleteSessionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvReadTableRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExplainDataQueryRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrepareDataQueryRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExecuteDataQueryRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvExecuteSchemeQueryRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCreateTenantRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAlterTenantRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvGetTenantStatusRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvListTenantsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvRemoveTenantRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvBeginTransactionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCommitTransactionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvRollbackTransactionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeTenantOptionsRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeTableOptionsRequest::TPtr& ev, const TActorContext& ctx);
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
    void Handle(TEvBulkUpsertRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvWhoAmIRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCreateRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAlterRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDropRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvListRateLimiterResources::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDescribeRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvAcquireRateLimiterResource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKikhouseCreateSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKikhouseRefreshSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKikhouseDiscardSnapshotRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvSelfCheckRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLoginRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvStreamExecuteScanQueryRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCoordinationSessionRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLongTxBeginRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLongTxCommitRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLongTxRollbackRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLongTxWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvLongTxReadRequest::TPtr& ev, const TActorContext& ctx);

    TActorId DiscoveryCacheActorID;
};

inline TActorId CreateGRpcRequestProxyId() {
    const auto actorId = TActorId(0, "GRpcReqProxy");
    return actorId;
}

} // namespace NGRpcService
} // namespace NKikimr
