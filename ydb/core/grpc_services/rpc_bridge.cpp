#include "rpc_bridge_base.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/bridge.h>

namespace NKikimr::NGRpcService {

using TEvSwitchClusterStateRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::SwitchClusterStateRequest,
        Ydb::Bridge::SwitchClusterStateResponse>;
using TEvGetClusterStateRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
        Ydb::Bridge::GetClusterStateResponse>;

using namespace NActors;
using namespace Ydb;

void CopyToInternalClusterState(const Ydb::Bridge::ClusterState& from, NKikimrBridge::TClusterState& to) {
    to.SetGeneration(from.generation());
    to.SetPrimaryPile(from.primary_pile());
    to.SetPromotedPile(from.promoted_pile());

    for (int i = 0, e = from.per_pile_state_size(); i < e; ++i) {
        NKikimrBridge::TClusterState::EPileState state;
        switch (from.per_pile_state(i)) {
            case Ydb::Bridge::ClusterState::DISCONNECTED:
                state = NKikimrBridge::TClusterState::DISCONNECTED;
                break;
            case Ydb::Bridge::ClusterState::NOT_SYNCHRONIZED:
                state = NKikimrBridge::TClusterState::NOT_SYNCHRONIZED;
                break;
            case Ydb::Bridge::ClusterState::SYNCHRONIZED:
                state = NKikimrBridge::TClusterState::SYNCHRONIZED;
                break;
            default:
                state = NKikimrBridge::TClusterState::DISCONNECTED;
                break;
        }
        to.AddPerPileState(state);
    }
}

void CopyFromBridgeInfo(const TBridgeInfo& from, const NKikimrBlobStorage::TStorageConfig& config, Ydb::Bridge::ClusterState& to) {
    if (config.HasClusterState()) {
        to.set_generation(config.GetClusterState().GetGeneration());
    }

    if (from.PrimaryPile) {
        to.set_primary_pile(from.PrimaryPile->BridgePileId.GetRawId());
    }
    if (from.BeingPromotedPile) {
        to.set_promoted_pile(from.BeingPromotedPile->BridgePileId.GetRawId());
    } else if (from.PrimaryPile) {
        to.set_promoted_pile(from.PrimaryPile->BridgePileId.GetRawId());
    }

    for (const auto& pile : from.Piles) {
        Ydb::Bridge::ClusterState::PileState state;
        switch (pile.State) {
            case NKikimrBridge::TClusterState::DISCONNECTED:
                state = Ydb::Bridge::ClusterState::DISCONNECTED;
                break;
            case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED:
                state = Ydb::Bridge::ClusterState::NOT_SYNCHRONIZED;
                break;
            case NKikimrBridge::TClusterState::SYNCHRONIZED:
                state = Ydb::Bridge::ClusterState::SYNCHRONIZED;
                break;
            default:
                state = Ydb::Bridge::ClusterState::DISCONNECTED;
                break;
        }
        to.add_per_pile_state(state);
    }
}

class TSwitchClusterStateRequest : public TBridgeRequestGrpc<TSwitchClusterStateRequest, TEvSwitchClusterStateRequest,
    Ydb::Bridge::SwitchClusterStateResult> {
    using TBase = TBridgeRequestGrpc<TSwitchClusterStateRequest, TEvSwitchClusterStateRequest, Ydb::Bridge::SwitchClusterStateResult>;

public:
    using TBase::TBase;

    void SendRequest(const TActorContext& ctx) {
        const auto* req = GetProtoRequest();
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto* cmd = request->Record.MutableSwitchBridgeClusterState();
        CopyToInternalClusterState(req->cluster_state(), *cmd->MutableNewClusterState());
        ctx.Send(MakeBlobStorageNodeWardenID(this->SelfId().NodeId()), request.release());
    }

    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        auto* self = Self();
        const auto& response = ev->Get()->Record;

        if (response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
            Ydb::Bridge::SwitchClusterStateResult result;
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
        } else {
            self->Reply(Ydb::StatusIds::INTERNAL_ERROR, response.GetErrorReason(),
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        const auto& request = *GetProtoRequest();

        if (!request.has_cluster_state()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("cluster_state field is required");
            return false;
        }

        const auto& clusterState = request.cluster_state();

        if (clusterState.per_pile_state_size() == 0) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("per_pile_state must not be empty");
            return false;
        }

        if (clusterState.primary_pile() >= (ui32)clusterState.per_pile_state_size()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("primary_pile index is out of bounds");
            return false;
        }

        if (clusterState.promoted_pile() >= (ui32)clusterState.per_pile_state_size()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("promoted_pile index is out of bounds");
            return false;
        }

        if (!IsAdministrator(AppData(), Request_->GetSerializedToken())) {
            status = Ydb::StatusIds::UNAUTHORIZED;
            issues.AddIssue("Bridge cluster state operations require administrator privileges");
            return false;
        }

        return true;
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
        }
    }
};

class TGetClusterStateRequest : public TBridgeRequestGrpc<TGetClusterStateRequest, TEvGetClusterStateRequest,
    Ydb::Bridge::GetClusterStateResult> {
    using TBase = TBridgeRequestGrpc<TGetClusterStateRequest, TEvGetClusterStateRequest, Ydb::Bridge::GetClusterStateResult>;

public:
    using TBase::TBase;

    void SendRequest(const TActorContext& ctx) {
        ctx.Send(MakeBlobStorageNodeWardenID(this->SelfId().NodeId()),
            new TEvNodeWardenQueryStorageConfig(false));
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) {
        return true;
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvNodeWardenStorageConfig, Handle);
        }
    }

protected:
    void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
        auto* self = Self();
        const auto& config = ev->Get()->Config;
        const auto& bridgeInfo = ev->Get()->BridgeInfo;

        if (!config->HasSelfManagementConfig() || !config->GetSelfManagementConfig().GetEnabled()) {
            self->Reply(Ydb::StatusIds::UNSUPPORTED, "Bridge operations require self-management mode to be enabled",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }

        if (bridgeInfo) {
            Ydb::Bridge::GetClusterStateResult result;
            CopyFromBridgeInfo(*bridgeInfo, *config, *result.mutable_cluster_state());
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
        } else {
            self->Reply(Ydb::StatusIds::NOT_FOUND, "Bridge cluster state is not configured",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
        }
    }
};

void DoSwitchClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TSwitchClusterStateRequest(p.release()));
}

void DoGetClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TGetClusterStateRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
