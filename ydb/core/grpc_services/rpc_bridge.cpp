#include "rpc_bridge_base.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/bridge.h>

namespace NKikimr::NGRpcService {

using TEvGetClusterStateRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
        Ydb::Bridge::GetClusterStateResponse>;
using TEvUpdateClusterStateRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::UpdateClusterStateRequest,
        Ydb::Bridge::UpdateClusterStateResponse>;

using namespace NActors;
using namespace Ydb;

namespace {
    Ydb::Bridge::PileState GetPublicState(const NKikimrBridge::TClusterState& from, ui32 pileId) {
        if (pileId == from.GetPrimaryPile()) {
            return Ydb::Bridge::PRIMARY;
        } else if (pileId == from.GetPromotedPile()) {
            return Ydb::Bridge::PROMOTE;
        } else {
            switch (from.GetPerPileState(pileId)) {
                case NKikimrBridge::TClusterState::DISCONNECTED:
                    return Ydb::Bridge::DISCONNECTED;
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED:
                    return Ydb::Bridge::NOT_SYNCHRONIZED;
                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                default:
                    return Ydb::Bridge::SYNCHRONIZED;
            }
        }
    }
} // namespace

void CopyFromInternalClusterState(const NKikimrBridge::TClusterState& from, Ydb::Bridge::GetClusterStateResult& to) {
    for (ui32 i = 0; i < from.PerPileStateSize(); ++i) {
        auto* update = to.add_per_pile_state();
        update->set_pile_id(i);
        update->set_state(GetPublicState(from, i));
    }
}

class TUpdateClusterStateRequest : public TBridgeRequestGrpc<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest,
    Ydb::Bridge::UpdateClusterStateResult> {
    using TBase = TBridgeRequestGrpc<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest, Ydb::Bridge::UpdateClusterStateResult>;

public:
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;
        auto* self = Self();
        if (!ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }

        self->Become(&TThis::StateWaitForConfig);

        ctx.Send(MakeBlobStorageNodeWardenID(self->SelfId().NodeId()),
            new TEvNodeWardenQueryStorageConfig(false));
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        if (!IsAdministrator(AppData(), Request_->GetSerializedToken())) {
            status = Ydb::StatusIds::UNAUTHORIZED;
            issues.AddIssue("Bridge operations require administrator privileges");
            return false;
        }

        const auto& updates = GetProtoRequest()->updates();

        if (updates.empty()) {
            status = Ydb::StatusIds::BAD_REQUEST;
            issues.AddIssue("Updates list cannot be empty");
            return false;
        }

        std::optional<ui32> primary, promoted;
        THashSet<ui32> updatedPiles;

        for (const auto& update : updates) {
            if (!updatedPiles.insert(update.pile_id()).second) {
                status = Ydb::StatusIds::BAD_REQUEST;
                issues.AddIssue(TStringBuilder() << "duplicate update for pile id# " << update.pile_id());
                return false;
            }

            switch (update.state()) {
                case Ydb::Bridge::PRIMARY:
                    if (primary) {
                        status = Ydb::StatusIds::BAD_REQUEST;
                        issues.AddIssue("multiple primary piles are not allowed in a single request");
                        return false;
                    }
                    primary = update.pile_id();
                    break;
                case Ydb::Bridge::PROMOTE:
                    if (promoted) {
                        status = Ydb::StatusIds::BAD_REQUEST;
                        issues.AddIssue("multiple promoted piles are not allowed in a single request");
                        return false;
                    }
                    promoted = update.pile_id();
                    break;
                default:
                    break;
            }
        }
        return true;
    }

    STFUNC(StateWaitForConfig) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvNodeWardenStorageConfig, Handle);
        }
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
        auto* self = Self();
        const auto* config = ev->Get()->Config.get();
        if (!config || !config->HasClusterState()) {
            self->Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Bridge cluster state is not configured or not available", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }

        const auto& currentClusterState = config->GetClusterState();
        for (const auto& update : GetProtoRequest()->updates()) {
            if (update.pile_id() >= currentClusterState.PerPileStateSize()) {
                 self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid pile id# " << update.pile_id(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                 return;
            }
        }

        NKikimrBridge::TClusterState newClusterState = currentClusterState;

        THashMap<ui32, Ydb::Bridge::PileState> updates;
        for (const auto& update : GetProtoRequest()->updates()) {
            updates[update.pile_id()] = update.state();
        }

        std::optional<ui32> finalPrimary;
        std::optional<ui32> finalPromoted;

        for (ui32 pileId = 0; pileId < currentClusterState.PerPileStateSize(); ++pileId) {
            Ydb::Bridge::PileState publicState;
            if (auto it = updates.find(pileId); it != updates.end()) {
                publicState = it->second;
            } else {
                publicState = GetPublicState(currentClusterState, pileId);
            }

            if (publicState == Ydb::Bridge::PRIMARY) {
                if (finalPrimary) {
                    self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Multiple primary piles are not allowed, found " << *finalPrimary << " and " << pileId, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
                }
                finalPrimary = pileId;
            } else if (publicState == Ydb::Bridge::PROMOTE) {
                if (finalPromoted) {
                    self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Multiple promoted piles are not allowed, found " << *finalPromoted << " and " << pileId, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
                }
                finalPromoted = pileId;
            }

            NKikimrBridge::TClusterState::EPileState internalState;
            switch (publicState) {
                case Ydb::Bridge::PRIMARY:
                case Ydb::Bridge::PROMOTE:
                case Ydb::Bridge::SYNCHRONIZED:
                    internalState = NKikimrBridge::TClusterState::SYNCHRONIZED;
                    break;
                case Ydb::Bridge::NOT_SYNCHRONIZED:
                    internalState = NKikimrBridge::TClusterState::NOT_SYNCHRONIZED;
                    break;
                case Ydb::Bridge::DISCONNECTED:
                    internalState = NKikimrBridge::TClusterState::DISCONNECTED;
                    break;
                default:
                    self->Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported pile state", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
            }
            newClusterState.SetPerPileState(pileId, internalState);
        }

        if (!finalPrimary) {
            self->Reply(Ydb::StatusIds::BAD_REQUEST, "Request must result in a state with one primary pile", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }

        newClusterState.SetPrimaryPile(*finalPrimary);
        newClusterState.SetPromotedPile(finalPromoted.value_or(*finalPrimary));
        newClusterState.SetGeneration(currentClusterState.GetGeneration() + 1);

        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(newClusterState);

        self->ActorContext().Send(MakeBlobStorageNodeWardenID(self->SelfId().NodeId()), request.release());
        self->Become(&TThis::StateWaitForPropose);
    }

private:
public:
    STFUNC(StateWaitForPropose) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
        }
    }

    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        auto* self = Self();
        if (response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
            Ydb::Bridge::UpdateClusterStateResult result;
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
            return;
        }
        self->Reply(Ydb::StatusIds::INTERNAL_ERROR, response.GetErrorReason(),
            NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
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

        if (!config->HasSelfManagementConfig() || !config->GetSelfManagementConfig().GetEnabled()) {
            self->Reply(Ydb::StatusIds::UNSUPPORTED, "Bridge operations require self-management mode to be enabled",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }

        if (config->HasClusterState()) {
            Ydb::Bridge::GetClusterStateResult result;
            CopyFromInternalClusterState(config->GetClusterState(), result);
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
        } else {
            self->Reply(Ydb::StatusIds::NOT_FOUND, "Bridge cluster state is not configured",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
        }
    }
};

void DoUpdateClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TUpdateClusterStateRequest(p.release()));
}

void DoGetClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TGetClusterStateRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
