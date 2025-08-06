#include "rpc_bridge_base.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/bridge.h>
#include <ydb/core/base/blobstorage_common.h>

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
    Ydb::Bridge::PileState::State GetPublicState(const NKikimrBridge::TClusterState& from, TBridgePileId pileId) {
        if (pileId == TBridgePileId::FromProto(&from, &NKikimrBridge::TClusterState::GetPrimaryPile)) {
            return Ydb::Bridge::PileState::PRIMARY;
        } else if (pileId == TBridgePileId::FromProto(&from, &NKikimrBridge::TClusterState::GetPromotedPile)) {
            return Ydb::Bridge::PileState::PROMOTE;
        } else {
            switch (from.GetPerPileState(pileId.GetPileIndex())) {
                case NKikimrBridge::TClusterState::DISCONNECTED:
                    return Ydb::Bridge::PileState::DISCONNECTED;
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                    return Ydb::Bridge::PileState::NOT_SYNCHRONIZED;
                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                    return Ydb::Bridge::PileState::SYNCHRONIZED;
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
            }
        }
    }
} // namespace

void CopyFromInternalClusterState(const NKikimrBridge::TClusterState& from, const TBridgeInfo& bridgeInfo, Ydb::Bridge::GetClusterStateResult& to) {
    for (ui32 i = 0; i < from.PerPileStateSize(); ++i) {
        auto* state = to.add_pile_states();
        const auto* pile = bridgeInfo.GetPile(TBridgePileId::FromPileIndex(i));
        state->set_pile_name(pile->Name);
        state->set_state(GetPublicState(from, TBridgePileId::FromPileIndex(i)));
    }
}

class TUpdateClusterStateRequest : public TBridgeRequestGrpc<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest,
    Ydb::Bridge::UpdateClusterStateResult> {
    using TBase = TBridgeRequestGrpc<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest, Ydb::Bridge::UpdateClusterStateResult>;
    using TRpcBase = TRpcOperationRequestActor<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest>;

public:
    using TBase::TBase;

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();
        TRpcBase::Bootstrap(ctx);

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

        std::optional<TString> primary, promoted;
        THashSet<TString> updatedPiles;

        for (const auto& update : updates) {
            if (!updatedPiles.insert(update.pile_name()).second) {
                status = Ydb::StatusIds::BAD_REQUEST;
                issues.AddIssue(TStringBuilder() << "duplicate update for pile name# " << update.pile_name());
                return false;
            }

            switch (update.state()) {
                case Ydb::Bridge::PileState::PRIMARY:
                    if (primary) {
                        status = Ydb::StatusIds::BAD_REQUEST;
                        issues.AddIssue("multiple primary piles are not allowed in a single request");
                        return false;
                    }
                    primary = update.pile_name();
                    break;
                case Ydb::Bridge::PileState::PROMOTE:
                    if (promoted) {
                        status = Ydb::StatusIds::BAD_REQUEST;
                        issues.AddIssue("multiple promoted piles are not allowed in a single request");
                        return false;
                    }
                    promoted = update.pile_name();
                    break;
                default:
                    break;
            }
        }
        return true;
    }

private:
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

        THashMap<TString, TBridgePileId> nameToId;
        const auto& bridgeInfo = ev->Get()->BridgeInfo;
        for (const auto& pile : bridgeInfo->Piles) {
            nameToId.emplace(pile.Name, pile.BridgePileId);
        }

        const auto& currentClusterState = config->GetClusterState();

        for (const auto& update : GetProtoRequest()->updates()) {
            if (nameToId.find(update.pile_name()) == nameToId.end()) {
                 self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid pile name# " << update.pile_name(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                 return;
            }
        }

        NKikimrBridge::TClusterState newClusterState = currentClusterState;

        THashMap<TBridgePileId, Ydb::Bridge::PileState::State> updates;
        for (const auto& update : GetProtoRequest()->updates()) {
            updates[nameToId.at(update.pile_name())] = update.state();
        }

        std::optional<TBridgePileId> finalPrimary;
        std::optional<TBridgePileId> finalPromoted;

        for (ui32 i = 0; i < currentClusterState.PerPileStateSize(); ++i) {
            const TBridgePileId pileId = TBridgePileId::FromPileIndex(i);
            Ydb::Bridge::PileState::State publicState;
            if (auto it = updates.find(pileId); it != updates.end()) {
                publicState = it->second;
            } else {
                publicState = GetPublicState(currentClusterState, pileId);
            }

            if (publicState == Ydb::Bridge::PileState::PRIMARY) {
                if (finalPrimary) {
                    self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Multiple primary piles are not allowed, found " << *finalPrimary << " and " << pileId, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
                }
                finalPrimary = pileId;
            } else if (publicState == Ydb::Bridge::PileState::PROMOTE) {
                if (finalPromoted) {
                    self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Multiple promoted piles are not allowed, found " << *finalPromoted << " and " << pileId, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
                }
                finalPromoted = pileId;
            }

            NKikimrBridge::TClusterState::EPileState internalState;
            switch (publicState) {
                case Ydb::Bridge::PileState::PRIMARY:
                case Ydb::Bridge::PileState::PROMOTE:
                case Ydb::Bridge::PileState::SYNCHRONIZED:
                    internalState = NKikimrBridge::TClusterState::SYNCHRONIZED;
                    break;
                case Ydb::Bridge::PileState::NOT_SYNCHRONIZED:
                    internalState = NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1;
                    break;
                case Ydb::Bridge::PileState::DISCONNECTED:
                    internalState = NKikimrBridge::TClusterState::DISCONNECTED;
                    break;
                default:
                    self->Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported pile state", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
            }
            newClusterState.SetPerPileState(pileId.GetPileIndex(), internalState);
        }

        if (!finalPrimary) {
            self->Reply(Ydb::StatusIds::BAD_REQUEST, "Request must result in a state with one primary pile", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }
        if (!finalPromoted) {
            finalPromoted = finalPrimary;
        }

        finalPrimary->CopyToProto(&newClusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
        finalPromoted->CopyToProto(&newClusterState, &NKikimrBridge::TClusterState::SetPromotedPile);
        newClusterState.SetGeneration(currentClusterState.GetGeneration() + 1);

        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto *cmd = request->Record.MutableSwitchBridgeClusterState();
        cmd->MutableNewClusterState()->CopyFrom(newClusterState);

        for (const auto& name : GetProtoRequest()->quorum_piles()) {
            if (const auto it = nameToId.find(name); it != nameToId.end()) {
                it->second.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::AddSpecificBridgePileIds);
            } else {
                self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Unknown pile name in quorum: " << name,
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                return;
            }
        }

        self->ActorContext().Send(MakeBlobStorageNodeWardenID(self->SelfId().NodeId()), request.release());
        self->Become(&TThis::StateWaitForPropose);
    }

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

private:

    void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
        auto* self = Self();
        const auto& config = ev->Get()->Config;
        const auto& bridgeInfo = ev->Get()->BridgeInfo;

        if (!config->HasSelfManagementConfig() || !config->GetSelfManagementConfig().GetEnabled()) {
            self->Reply(Ydb::StatusIds::UNSUPPORTED, "Bridge operations require self-management mode to be enabled",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }

        if (config->HasClusterState()) {
            Ydb::Bridge::GetClusterStateResult result;
            CopyFromInternalClusterState(config->GetClusterState(), *bridgeInfo, result);
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
