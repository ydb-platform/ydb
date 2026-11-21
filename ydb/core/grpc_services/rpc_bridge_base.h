#pragma once

#include "defs.h"
#include "rpc_deferrable.h"

#include <ydb/public/api/protos/draft/ydb_bridge.pb.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/base/bridge.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/protos/bridge.pb.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr::NGRpcService {

// cluster got converted into bridged one while this node was running without BridgeConfig
inline constexpr char NoBridgeConfigError[] = "Bridge cluster state is available, but this node runs without BridgeConfig;"
    " send the request to another node or restart this one with BridgeConfig";

inline Ydb::Bridge::PileState::State GetPublicBridgePileState(const NKikimrBridge::TClusterState& from, TBridgePileId pileId) {
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

template <typename TDerived, typename TRequest, typename TResultRecord>
class TBridgeRequestGrpc : public TRpcOperationRequestActor<TDerived, TRequest> {
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

public:
    TBridgeRequestGrpc(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap() {
        const auto& ctx = TActivationContext::AsActorContext();
        TBase::Bootstrap(ctx);
        auto *self = Self();

        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;
        if (!self->ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }

        self->Become(&TDerived::StateFunc);
        self->SendRequest(ctx);
    }

protected:
    const TDerived *Self() const { return static_cast<const TDerived*>(this); }
    TDerived *Self() { return static_cast<TDerived*>(this); }

    // reply to the failed distconf invocation with the matching status
    void ReplyWithInvokeError(const NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& record) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        auto *self = Self();
        switch (record.GetStatus()) {
            case TResult::NO_QUORUM:
                self->Reply(Ydb::StatusIds::UNAVAILABLE, record.GetErrorReason(),
                    NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, self->ActorContext());
                break;

            case TResult::RACE:
                self->Reply(Ydb::StatusIds::ABORTED, record.GetErrorReason(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                    self->ActorContext());
                break;

            default:
                self->Reply(Ydb::StatusIds::GENERIC_ERROR, record.GetErrorReason(),
                    NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                break;
        }
    }

    // operations changing the bridge cluster state are allowed to administrators only
    bool CheckAdministrator(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) const {
        if (!IsAdministrator(AppData(), this->Request_->GetSerializedToken())) {
            status = Ydb::StatusIds::UNAUTHORIZED;
            issues.AddIssue("Bridge operations require administrator privileges");
            return false;
        }
        return true;
    }
};

} // namespace NKikimr::NGRpcService
