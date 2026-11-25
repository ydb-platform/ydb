#include "rpc_bridge_base.h"
#include "service_bridge.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>

namespace NKikimr::NGRpcService {
    namespace {

        using namespace NActors;

        using TEvGetClusterStateRequest = TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
                                                                    Ydb::Bridge::GetClusterStateResponse>;

        void CopyFromInternalClusterState(const NKikimrBridge::TClusterState& from, const TBridgeInfo& bridgeInfo, Ydb::Bridge::GetClusterStateResult& to) {
            for (ui32 i = 0; i < from.PerPileStateSize(); ++i) {
                const auto* pile = bridgeInfo.FindPile(TBridgePileId::FromPileIndex(i));
                if (!pile || pile->IsFreeSlot) {
                    continue;
                }
                auto* state = to.add_pile_states();
                state->set_pile_name(pile->Name);
                state->set_state(GetPublicBridgePileState(from, TBridgePileId::FromPileIndex(i)));
            }
            to.set_generation(from.GetGeneration());
        }

        void CopyFromInternalClusterStateDetails(const NKikimrBridge::TClusterStateDetails& from, const TBridgeInfo& bridgeInfo,
                                                 Ydb::Bridge::GetClusterStateResult& to) {
            to.set_conversion_in_progress(from.GetBscConversionPending());
            if (from.HasPendingPileOperation()) {
                const auto& op = from.GetPendingPileOperation();
                auto* pb = to.mutable_pending_pile_operation();
                using TOperation = NKikimrBridge::TClusterStateDetails::TPendingPileOperation;
                if (const auto* pile = bridgeInfo.FindPile(TBridgePileId::FromProto(&op, &TOperation::GetBridgePileId))) {
                    pb->set_pile_name(pile->Name);
                }
                switch (op.GetKind()) {
                    case TOperation::ADD:
                        pb->set_kind(Ydb::Bridge::PendingPileOperation::ADD);
                        break;

                    case TOperation::REMOVE:
                        pb->set_kind(Ydb::Bridge::PendingPileOperation::REMOVE);
                        break;

                    case TOperation::ROLLBACK:
                        pb->set_kind(Ydb::Bridge::PendingPileOperation::ROLLBACK);
                        break;

                    case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MIN_SENTINEL_DO_NOT_USE_:
                    case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MAX_SENTINEL_DO_NOT_USE_:
                        Y_ABORT();
                }
                pb->set_error_reason(op.GetErrorReason());
            }
        }

        class TGetClusterStateRequest: public TBridgeRequestGrpc<TGetClusterStateRequest, TEvGetClusterStateRequest,
                                                                 Ydb::Bridge::GetClusterStateResult> {
            using TBase = TBridgeRequestGrpc<TGetClusterStateRequest, TEvGetClusterStateRequest, Ydb::Bridge::GetClusterStateResult>;

            enum class EPhase { LocalConfig, PileConnectivity, UnbridgedStatus };

            Ydb::Bridge::GetClusterStateResult Result;
            EPhase Phase = EPhase::LocalConfig;

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
                    hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle);
                    default:
                        return TBase::StateFuncBase(ev); // operation timeout and client cancellation
                }
            }

        private:
            void Handle(TEvNodeWardenStorageConfig::TPtr& ev) {
                if (Phase != EPhase::LocalConfig) {
                    return;
                }
                auto* self = Self();
                const auto& config = ev->Get()->Config;
                const auto& bridgeInfo = ev->Get()->BridgeInfo;

                if (!config->HasSelfManagementConfig() || !config->GetSelfManagementConfig().GetEnabled()) {
                    self->Reply(Ydb::StatusIds::UNSUPPORTED, "Bridge operations require self-management mode to be enabled",
                                NKikimrIssues::TIssuesIds::DEFAULT_ERROR, self->ActorContext());
                    return;
                }

                if (!config->HasClusterState()) {
                    if (!GetProtoRequest()->include_unbridged_status()) {
                        Reply(Ydb::StatusIds::NOT_FOUND, "Bridge cluster state is not configured",
                              NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
                        return;
                    }
                    Ydb::StatusIds::StatusCode status;
                    NYql::TIssues issues;
                    if (!CheckAdministrator(status, issues)) {
                        Reply(status, issues, ActorContext());
                        return;
                    }
                    auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
                    request->Record.MutableQueryBridgeRollbackStatus();
                    Phase = EPhase::UnbridgedStatus;
                    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
                    return;
                }

                if (!bridgeInfo) {
                    self->Reply(Ydb::StatusIds::UNAVAILABLE, NoBridgeConfigError, NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                                self->ActorContext());
                    return;
                }
                CopyFromInternalClusterState(config->GetClusterState(), *bridgeInfo, Result);
                CopyFromInternalClusterStateDetails(config->GetClusterStateDetails(), *bridgeInfo, Result);
                if (GetProtoRequest()->include_pile_connectivity()) {
                    auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
                    request->Record.MutableQueryPileConnectivity();
                    Phase = EPhase::PileConnectivity;
                    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
                } else {
                    self->ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, self->ActorContext());
                }
            }

            void Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
                if (Phase == EPhase::LocalConfig) {
                    return;
                }
                using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
                const auto& record = ev->Get()->Record;
                if (record.GetStatus() != TResult::OK) {
                    ReplyWithInvokeError(record);
                    return;
                }
                if (Phase == EPhase::UnbridgedStatus) {
                    if (!record.HasBridgeRollbackStatus()) {
                        return Reply(Ydb::StatusIds::INTERNAL_ERROR, "Missing unbridged status report",
                                     NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
                    }
                    const auto& report = record.GetBridgeRollbackStatus();
                    if (report.GetHasClusterState()) {
                        return Reply(Ydb::StatusIds::ABORTED, "The root reports a bridge cluster state; retry the request",
                                     NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
                    }
                    auto* result = Result.mutable_unbridged_status();
                    result->mutable_bridge_config_node_ids()->CopyFrom(report.GetBridgeConfigNodeIds());
                    result->mutable_unreported_static_node_ids()->CopyFrom(report.GetUnreportedStaticNodeIds());
                    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ActorContext());
                }
                if (!record.HasPileConnectivity()) {
                    Reply(Ydb::StatusIds::GENERIC_ERROR, "Missing pile connectivity report",
                          NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
                    return;
                }
                const auto& report = record.GetPileConnectivity();
                auto* result = Result.mutable_pile_connectivity();
                result->set_reporter_node_id(report.GetReporterNodeId());
                result->set_reporter_is_root(report.GetHasScepter());
                result->set_binding_in_progress(report.GetBindingInProgress());
                result->set_generation(report.GetClusterStateGeneration());
                for (const auto& pile : report.GetPiles()) {
                    auto* item = result->add_piles();
                    // Names travel with the measurements: the local registry may have changed or reused a slot.
                    item->set_pile_name(pile.GetPileName());
                    item->set_static_nodes(pile.GetNumStaticNodes());
                    item->set_connected_static_nodes(pile.GetNumConnectedStaticNodes());
                    item->set_has_node_quorum(pile.GetHasNodeQuorum());
                    item->set_in_config_quorum(pile.GetRequiresConfigQuorum());
                    item->set_in_data_quorum(pile.GetRequiresDataQuorum());
                }
                ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ActorContext());
            }
        };

    } // namespace

    void DoGetClusterState(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
        TActivationContext::AsActorContext().Register(new TGetClusterStateRequest(request.release()));
    }

} // namespace NKikimr::NGRpcService
