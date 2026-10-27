#include "rpc_bridge_base.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/base/bridge.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>

namespace NKikimr::NGRpcService {

using TEvGetClusterStateRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
        Ydb::Bridge::GetClusterStateResponse>;
using TEvUpdateClusterStateRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::UpdateClusterStateRequest,
        Ydb::Bridge::UpdateClusterStateResponse>;
using TEvConvertToBridgeRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::ConvertToBridgeRequest,
        Ydb::Bridge::ConvertToBridgeResponse>;
using TEvAddPileRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::AddPileRequest,
        Ydb::Bridge::AddPileResponse>;
using TEvRemovePileRequest =
    TGrpcRequestOperationCall<Ydb::Bridge::RemovePileRequest,
        Ydb::Bridge::RemovePileResponse>;

using namespace NActors;
using namespace Ydb;

namespace {
    // cluster got converted into bridged one while this node was running without BridgeConfig
    const TString NoBridgeConfigError = "Bridge cluster state is available, but this node runs without BridgeConfig;"
        " send the request to another node or restart this one with BridgeConfig";

    Ydb::Bridge::PileState::State GetPublicState(const NKikimrBridge::TClusterState& from, TBridgePileId pileId) {
        if (pileId == TBridgePileId::FromProto(&from, &NKikimrBridge::TClusterState::GetPrimaryPile)) {
            return Ydb::Bridge::PileState::PRIMARY;
        } else if (pileId == TBridgePileId::FromProto(&from, &NKikimrBridge::TClusterState::GetPromotedPile)) {
            return Ydb::Bridge::PileState::PROMOTED;
        } else {
            switch (from.GetPerPileState(pileId.GetPileIndex())) {
                case NKikimrBridge::TClusterState::SUSPENDED:
                    return Ydb::Bridge::PileState::SUSPENDED;
                case NKikimrBridge::TClusterState::DISCONNECTED:
                    return Ydb::Bridge::PileState::DISCONNECTED;
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                    return Ydb::Bridge::PileState::NOT_SYNCHRONIZED;
                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                    return Ydb::Bridge::PileState::SYNCHRONIZED;
                case NKikimrBridge::TClusterState::INACTIVE:
                    return Ydb::Bridge::PileState::INACTIVE;
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
            }
        }
    }
} // namespace

void CopyFromInternalClusterState(const NKikimrBridge::TClusterState& from, const TBridgeInfo& bridgeInfo, Ydb::Bridge::GetClusterStateResult& to) {
    for (ui32 i = 0; i < from.PerPileStateSize(); ++i) {
        const auto* pile = bridgeInfo.FindPile(TBridgePileId::FromPileIndex(i));
        if (!pile || pile->IsFreeSlot) {
            continue;
        }
        auto* state = to.add_pile_states();
        state->set_pile_name(pile->Name);
        state->set_state(GetPublicState(from, TBridgePileId::FromPileIndex(i)));
    }
    to.set_generation(from.GetGeneration());
}

void CopyFromInternalClusterStateDetails(const NKikimrBridge::TClusterStateDetails& from, const TBridgeInfo& bridgeInfo,
        Ydb::Bridge::GetClusterStateResult& to) {
    to.set_conversion_in_progress(from.GetBscConversionPending());
    if (from.HasPendingPileOperation()) {
        const auto& op = from.GetPendingPileOperation();
        auto *pb = to.mutable_pending_pile_operation();
        using TOperation = NKikimrBridge::TClusterStateDetails::TPendingPileOperation;
        if (const auto *pile = bridgeInfo.FindPile(TBridgePileId::FromProto(&op, &TOperation::GetBridgePileId))) {
            pb->set_pile_name(pile->Name);
        }
        switch (op.GetKind()) {
            case TOperation::ADD:
                pb->set_kind(Ydb::Bridge::PendingPileOperation::ADD);
                break;

            case TOperation::REMOVE:
                pb->set_kind(Ydb::Bridge::PendingPileOperation::REMOVE);
                break;

            case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
        pb->set_error_reason(op.GetErrorReason());
    }
}

class TUpdateClusterStateRequest : public TBridgeRequestGrpc<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest,
    Ydb::Bridge::UpdateClusterStateResult> {
    using TBase = TBridgeRequestGrpc<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest, Ydb::Bridge::UpdateClusterStateResult>;
    using TRpcBase = TRpcOperationRequestActor<TUpdateClusterStateRequest, TEvUpdateClusterStateRequest>;

    bool IsDisconnectingRequest = false;

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
        if (!CheckAdministrator(status, issues)) {
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
                case Ydb::Bridge::PileState::PROMOTED:
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
    void PassAway() override {
        if (IsDisconnectingRequest) {
            IsDisconnectingRequest = false;
            Send(NKikimr::MakeGRpcServersManagerId(SelfId().NodeId()),
                new NKikimr::TEvGRpcServersManager::TEvDisconnectRequestFinished());
        }
        TBase::PassAway();
    }

    STFUNC(StateWaitForConfig) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvNodeWardenStorageConfig, Handle);
            default: return TBase::StateFuncBase(ev); // operation timeout and client cancellation
        }
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
        auto* self = Self();
        const auto* config = ev->Get()->Config.get();
        if (!config || !config->HasClusterState()) {
            self->Reply(Ydb::StatusIds::PRECONDITION_FAILED, "Bridge cluster state is not configured or not available", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }

        const auto& bridgeInfo = ev->Get()->BridgeInfo;
        if (!bridgeInfo) {
            self->Reply(Ydb::StatusIds::UNAVAILABLE, NoBridgeConfigError, NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                self->ActorContext());
            return;
        }

        THashMap<TString, TBridgePileId> nameToId;
        for (const auto& pile : bridgeInfo->Piles) {
            if (!pile.IsFreeSlot) {
                nameToId.emplace(pile.Name, pile.BridgePileId);
            }
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
            const auto pileId = nameToId.at(update.pile_name());
            updates[pileId] = update.state();
        }

        // prohibit updates to suspended piles and to the ones not added to the cluster
        for (const auto& [pileId, state] : updates) {
            if (currentClusterState.GetPerPileState(pileId.GetPileIndex()) == NKikimrBridge::TClusterState::SUSPENDED) {
                self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "cannot update suspended pile: " << pileId,
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                return;
            } else if (currentClusterState.GetPerPileState(pileId.GetPileIndex()) == NKikimrBridge::TClusterState::INACTIVE) {
                self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "pile is not added to the cluster: "
                    << bridgeInfo->GetPile(pileId)->Name, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                return;
            }
        }

        std::optional<TBridgePileId> finalPrimary;
        std::optional<TBridgePileId> finalPromoted;

        for (ui32 i = 0; i < currentClusterState.PerPileStateSize(); ++i) {
            const TBridgePileId pileId = TBridgePileId::FromPileIndex(i);
            Ydb::Bridge::PileState::State publicState;
            const auto it = updates.find(pileId);
            if (it != updates.end()) {
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
            } else if (publicState == Ydb::Bridge::PileState::PROMOTED) {
                if (finalPromoted) {
                    self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Multiple promoted piles are not allowed, found " << *finalPromoted << " and " << pileId, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
                }
                finalPromoted = pileId;
            }

            if (it == updates.end()) {
                continue; // piles not mentioned in the request keep their exact internal state
            }

            NKikimrBridge::TClusterState::EPileState internalState;
            switch (publicState) {
                case Ydb::Bridge::PileState::PRIMARY:
                case Ydb::Bridge::PileState::PROMOTED:
                case Ydb::Bridge::PileState::SYNCHRONIZED:
                    internalState = NKikimrBridge::TClusterState::SYNCHRONIZED;
                    break;
                case Ydb::Bridge::PileState::NOT_SYNCHRONIZED:
                    internalState = NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1;
                    break;
                case Ydb::Bridge::PileState::DISCONNECTED:
                    internalState = NKikimrBridge::TClusterState::DISCONNECTED;
                    break;
                case Ydb::Bridge::PileState::SUSPENDED:
                    internalState = NKikimrBridge::TClusterState::SUSPENDED;
                    break;
                case Ydb::Bridge::PileState::INACTIVE:
                    self->Reply(Ydb::StatusIds::BAD_REQUEST, "Pile can't be switched to INACTIVE state, it has to be"
                        " removed from the cluster instead", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
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

        if (finalPrimary && newClusterState.GetPerPileState(finalPrimary->GetPileIndex()) == NKikimrBridge::TClusterState::SUSPENDED) {
            self->Reply(Ydb::StatusIds::BAD_REQUEST, "primary pile cannot be suspended",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }
        if (finalPromoted && newClusterState.GetPerPileState(finalPromoted->GetPileIndex()) == NKikimrBridge::TClusterState::SUSPENDED) {
            self->Reply(Ydb::StatusIds::BAD_REQUEST, "promoted pile cannot be suspended",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }

        finalPrimary->CopyToProto(&newClusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
        finalPromoted->CopyToProto(&newClusterState, &NKikimrBridge::TClusterState::SetPromotedPile);
        newClusterState.SetGeneration(currentClusterState.GetGeneration() + 1);

        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto *cmd = request->Record.MutableSwitchBridgeClusterState();
        cmd->MutableNewClusterState()->CopyFrom(newClusterState);

        for (const auto& name : GetProtoRequest()->quorum_piles()) {
            if (const auto it = nameToId.find(name); it != nameToId.end()) {
                if (newClusterState.GetPerPileState(it->second.GetPileIndex()) == NKikimrBridge::TClusterState::SUSPENDED) {
                    self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "quorum cannot include suspended pile: " << name,
                        NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
                }
                it->second.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::AddSpecificBridgePileIds);
            } else {
                self->Reply(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Unknown pile name in quorum: " << name,
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                return;
            }
        }

        const auto selfNodePileId = bridgeInfo->SelfNodePile->BridgePileId;
        if (selfNodePileId.GetPileIndex() < newClusterState.PerPileStateSize()) {
            const auto newSelfState = newClusterState.GetPerPileState(selfNodePileId.GetPileIndex());
            if (newSelfState == NKikimrBridge::TClusterState::DISCONNECTED ||
                newSelfState == NKikimrBridge::TClusterState::SUSPENDED) {
                IsDisconnectingRequest = true;
                self->ActorContext().Send(NKikimr::MakeGRpcServersManagerId(self->SelfId().NodeId()),
                    new NKikimr::TEvGRpcServersManager::TEvDisconnectRequestStarted());
            }
        }

        self->ActorContext().Send(MakeBlobStorageNodeWardenID(self->SelfId().NodeId()), request.release());
        self->Become(&TThis::StateWaitForPropose);
    }

    STFUNC(StateWaitForPropose) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
            default: return TBase::StateFuncBase(ev); // operation timeout and client cancellation
        }
    }

    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        auto* self = Self();
        const bool success = response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK;
        if (!success) {
            self->Reply(Ydb::StatusIds::INTERNAL_ERROR, response.GetErrorReason(),
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
            return;
        }
        Ydb::Bridge::UpdateClusterStateResult result;
        self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
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
            default: return TBase::StateFuncBase(ev); // operation timeout and client cancellation
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

        if (config->HasClusterState() && !bridgeInfo) {
            self->Reply(Ydb::StatusIds::UNAVAILABLE, NoBridgeConfigError, NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                self->ActorContext());
        } else if (config->HasClusterState()) {
            Ydb::Bridge::GetClusterStateResult result;
            CopyFromInternalClusterState(config->GetClusterState(), *bridgeInfo, result);
            CopyFromInternalClusterStateDetails(config->GetClusterStateDetails(), *bridgeInfo, result);
            self->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, self->ActorContext());
        } else {
            self->Reply(Ydb::StatusIds::NOT_FOUND, "Bridge cluster state is not configured",
                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
        }
    }
};

class TConvertToBridgeRequest : public TBridgeRequestGrpc<TConvertToBridgeRequest, TEvConvertToBridgeRequest,
    Ydb::Bridge::ConvertToBridgeResult> {
    using TBase = TBridgeRequestGrpc<TConvertToBridgeRequest, TEvConvertToBridgeRequest, Ydb::Bridge::ConvertToBridgeResult>;

public:
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        return CheckAdministrator(status, issues);
    }

    void SendRequest(const TActorContext& ctx) {
        // all the preconditions are checked by distconf on the root node
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        request->Record.MutableConvertToBridge()->SetSkipStoragePoolConversion(
            GetProtoRequest()->skip_storage_pool_conversion());
        ctx.Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
            default: return TBase::StateFuncBase(ev); // operation timeout and client cancellation
        }
    }

private:
    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        const auto& record = ev->Get()->Record;
        if (record.GetStatus() == TResult::OK) {
            ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Bridge::ConvertToBridgeResult(), ActorContext());
        } else {
            ReplyWithInvokeError(record);
        }
    }
};

// request of the operation changing the set of piles; it waits (watching storage config on this node) until BSC
// finishes its part of the operation, or reports an error
template<typename TDerived, typename TEvRequest, typename TResultRecord>
class TPileOperationRequest : public TBridgeRequestGrpc<TDerived, TEvRequest, TResultRecord> {
    using TBase = TBridgeRequestGrpc<TDerived, TEvRequest, TResultRecord>;

    bool Subscribed = false;

public:
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
        return this->CheckAdministrator(status, issues);
    }

    void SendRequest(const TActorContext& ctx) {
        // all the preconditions are checked by distconf on the root node
        auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        static_cast<TDerived*>(this)->FillCommand(request->Record, this->GetProtoRequest()->pile_name());
        ctx.Send(MakeBlobStorageNodeWardenID(this->SelfId().NodeId()), request.release());
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
            hFunc(TEvNodeWardenStorageConfig, Handle);
            default: return TBase::StateFuncBase(ev); // operation timeout and client cancellation
        }
    }

protected:
    // reply when the operation on the pile is finished as the config tells; pile is nullptr when it isn't listed there
    void CheckFinished(const NKikimrBlobStorage::TStorageConfig& config, const TBridgeInfo::TPile *pile, bool finished,
            const char *what) {
        const auto& details = config.GetClusterStateDetails();
        if (pile && NBridge::GetPendingPileOperationPileId(details) == pile->BridgePileId) {
            if (const auto& op = details.GetPendingPileOperation(); op.HasErrorReason()) {
                this->Reply(Ydb::StatusIds::GENERIC_ERROR, TStringBuilder() << what << " (this is retried until it"
                    " succeeds): " << op.GetErrorReason(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
            }
        } else if (finished) {
            this->ReplyWithResult(Ydb::StatusIds::SUCCESS, TResultRecord(), this->ActorContext());
        }
    }

private:
    void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        const auto& record = ev->Get()->Record;
        if (record.GetStatus() == TResult::OK) {
            // the operation has started, wait for BSC to perform its part watching the config
            Subscribed = true;
            this->Send(MakeBlobStorageNodeWardenID(this->SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true));
        } else {
            this->ReplyWithInvokeError(record);
        }
    }

    void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
        if (const auto& bridgeInfo = ev->Get()->BridgeInfo) { // wait for the config with bridge info otherwise
            static_cast<TDerived*>(this)->Check(*ev->Get()->Config,
                bridgeInfo->ResolvePile(this->GetProtoRequest()->pile_name()));
        }
    }

    void PassAway() override {
        if (Subscribed) {
            this->Send(MakeBlobStorageNodeWardenID(this->SelfId().NodeId()), new TEvents::TEvUnsubscribe);
        }
        TBase::PassAway();
    }
};

class TAddPileRequest : public TPileOperationRequest<TAddPileRequest, TEvAddPileRequest, Ydb::Bridge::AddPileResult> {
public:
    using TPileOperationRequest::TPileOperationRequest;

    void FillCommand(NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot& record, const TString& pileName) {
        record.MutableAddBridgePile()->SetPileName(pileName);
    }

    void Check(const NKikimrBlobStorage::TStorageConfig& config, const TBridgeInfo::TPile *pile) {
        // INACTIVE pile means this node has not got the config with the pile added yet
        CheckFinished(config, pile, pile && pile->State != NKikimrBridge::TClusterState::INACTIVE,
            "Pile is added to the cluster, but its parts of storage groups can't be created");
    }
};

class TRemovePileRequest : public TPileOperationRequest<TRemovePileRequest, TEvRemovePileRequest,
        Ydb::Bridge::RemovePileResult> {
public:
    using TPileOperationRequest::TPileOperationRequest;

    void FillCommand(NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot& record, const TString& pileName) {
        record.MutableRemoveBridgePile()->SetPileName(pileName);
    }

    void Check(const NKikimrBlobStorage::TStorageConfig& config, const TBridgeInfo::TPile *pile) {
        // the pile can be dropped from the config only when it is INACTIVE
        CheckFinished(config, pile, !pile || pile->State == NKikimrBridge::TClusterState::INACTIVE,
            "Pile is being removed from the cluster, but its parts of storage groups can't be deleted");
    }
};

void DoUpdateClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TUpdateClusterStateRequest(p.release()));
}

void DoGetClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TGetClusterStateRequest(p.release()));
}

void DoConvertToBridge(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TConvertToBridgeRequest(p.release()));
}

void DoAddPile(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TAddPileRequest(p.release()));
}

void DoRemovePile(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TRemovePileRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
