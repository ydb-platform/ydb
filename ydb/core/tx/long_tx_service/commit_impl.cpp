#include "long_tx_service_impl.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/columnshard/columnshard.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NLongTxService {

    class TLongTxServiceActor::TCommitActor : public TActorBootstrapped<TCommitActor> {
    public:
        struct TParams {
            TLongTxId TxId;
            TString DatabaseName;
            THashMap<ui64, ui64> ColumnShardWrites;
        };

    public:
        TCommitActor(TActorId parent, TParams&& params)
            : Parent(parent)
            , Params(std::move(params))
            , LogPrefix("LongTxService.Commit ")
        { }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::LONG_TX_SERVICE_COMMIT;
        }

        void Bootstrap() {
            LogPrefix = TStringBuilder() << LogPrefix << SelfId() << " ";
            SendAllocateTxId();
        }

    private:
        void PassAway() override {
            if (Services.LeaderPipeCache) {
                Send(Services.LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
            }
            TActor::PassAway();
        }

        void SendAllocateTxId() {
            TXLOG_DEBUG("Allocating TxId");
            Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
            Become(&TThis::StateAllocateTxId);
        }

        STFUNC(StateAllocateTxId) {
            Y_UNUSED(ctx);
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
            }
        }

        void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
            const auto* msg = ev->Get();
            TxId = msg->TxId;
            Services = msg->Services;
            LogPrefix = TStringBuilder() << LogPrefix << " TxId# " << TxId;
            TXLOG_DEBUG("Allocated TxId");
            PrepareTransaction();
        }

    private:
        void PrepareTransaction() {
            for (const auto& pr : Params.ColumnShardWrites) {
                const ui64 tabletId = pr.first;
                const ui64 writeId = pr.second;
                NKikimrTxColumnShard::TCommitTxBody tx;
                tx.AddWriteIds(writeId);
                TString txBody;
                Y_VERIFY(tx.SerializeToString(&txBody));

                TXLOG_DEBUG("Sending TEvProposeTransaction to ColumnShard# " << tabletId << " WriteId# " << writeId);
                SendToTablet(tabletId, MakeHolder<TEvColumnShard::TEvProposeTransaction>(
                    NKikimrTxColumnShard::TX_KIND_COMMIT,
                    SelfId(),
                    TxId,
                    std::move(txBody)));
                WaitingShards.insert(tabletId);
            }
            Become(&TThis::StatePrepare);
        }

        STFUNC(StatePrepare) {
            Y_UNUSED(ctx);
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvColumnShard::TEvProposeTransactionResult, HandlePrepare);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePrepare);
            }
        }

        bool UpdateCoordinators(ui64 tabletId, const google::protobuf::RepeatedField<ui64>& domainCoordinators) {
            ui64 privateCoordinator =
                TCoordinators(TVector<ui64>(domainCoordinators.begin(), domainCoordinators.end()))
                .Select(TxId);

            if (!SelectedCoordinator) {
                SelectedCoordinator = privateCoordinator;
                TXLOG_DEBUG("Selected coordinator " << SelectedCoordinator);
            }

            if (!SelectedCoordinator || SelectedCoordinator != privateCoordinator) {
                TString error = "Unable to choose coordinator for all participants";
                TXLOG_ERROR(error
                    << ": previous coordinator " << SelectedCoordinator
                    << ", current coordinator " << privateCoordinator
                    << ", Tablet# " << tabletId);
                NYql::TIssues issues;
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR, error));
                FinishWithError(Ydb::StatusIds::INTERNAL_ERROR, std::move(issues));
                return false;
            }

            return true;
        }

        void HandlePrepare(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui64 tabletId = msg->Record.GetOrigin();
            const auto status = msg->Record.GetStatus();

            TXLOG_DEBUG("Received TEvProposeTransactionResult from"
                << " ColumnShard# " << tabletId
                << " Status# " << NKikimrTxColumnShard::EResultStatus_Name(status));

            switch (status) {
                case NKikimrTxColumnShard::PREPARED: {
                    if (!WaitingShards.contains(tabletId)) {
                        // ignore unexpected results
                        return;
                    }

                    WaitingShards.erase(tabletId);
                    MinStep = Max(MinStep, msg->Record.GetMinStep());
                    MaxStep = Min(MaxStep, msg->Record.GetMaxStep());
                    if (!UpdateCoordinators(tabletId, msg->Record.GetDomainCoordinators())) {
                        return;
                    }
                    if (!WaitingShards.empty()) {
                        return;
                    }
                    return PlanTransaction();
                }

                default: {
                    // Cancel transaction, since we didn't plan it
                    CancelProposal();

                    NYql::TIssues issues;
                    issues.AddIssue("TODO: need mapping from shard errors to api errors");
                    return FinishWithError(Ydb::StatusIds::GENERIC_ERROR, std::move(issues));
                }
            }
        }

        void HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui64 tabletId = msg->TabletId;

            TXLOG_DEBUG("Delivery problem"
                << " TabletId# " << tabletId
                << " NotDelivered# " << msg->NotDelivered);

            // Cancel transaction, since we didn't plan it
            CancelProposal();

            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Shard " << tabletId << " is not available");
            return FinishWithError(Ydb::StatusIds::UNAVAILABLE, std::move(issues));
        }

    private:
        void PlanTransaction() {
            Y_VERIFY(SelectedCoordinator);
            Y_VERIFY(WaitingShards.empty());

            auto req = MakeHolder<TEvTxProxy::TEvProposeTransaction>(
                SelectedCoordinator, TxId, 0, MinStep, MaxStep);
            auto* reqAffectedSet = req->Record.MutableTransaction()->MutableAffectedSet();
            reqAffectedSet->Reserve(Params.ColumnShardWrites.size());

            for (const auto& pr : Params.ColumnShardWrites) {
                const ui64 tabletId = pr.first;
                auto* x = reqAffectedSet->Add();
                x->SetTabletId(tabletId);
                x->SetFlags(/* write */ 2);
                WaitingShards.insert(tabletId);
            }

            TXLOG_DEBUG("Sending TEvProposeTransaction to SelectedCoordinator# " << SelectedCoordinator);
            SendToTablet(SelectedCoordinator, std::move(req));
            Become(&TThis::StatePlan);
        }

        STFUNC(StatePlan) {
            Y_UNUSED(ctx);
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxy::TEvProposeTransactionStatus, HandlePlan);
                hFunc(TEvColumnShard::TEvProposeTransactionResult, HandlePlan);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePlan);
            }
        }

        void HandlePlan(TEvTxProxy::TEvProposeTransactionStatus::TPtr& ev) {
            const auto* msg = ev->Get();

            switch (msg->GetStatus()) {
                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
                    // This is just an informational status
                    TXLOG_DEBUG("Received TEvProposeTransactionStatus from coordinator Status# " << msg->GetStatus());
                    break;

                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
                    TXLOG_DEBUG("Received TEvProposeTransactionStatus from coordinator Status# " << msg->GetStatus());
                    PlanStep = msg->Record.GetStepId();
                    break;

                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
                case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting: // TODO: may retry
                    // We only cancel for known cases where transaction is not planned
                    CancelProposal();
                    // fall through to generic errors

                default: {
                    TXLOG_ERROR("Received TEvProposeTransactionStatus from coordinator Status# " << msg->GetStatus());
                    NYql::TIssues issues;
                    issues.AddIssue("Coordinator did not accept this transaction");
                    return FinishWithError(Ydb::StatusIds::UNAVAILABLE, std::move(issues));
                }
            }
        }

        void HandlePlan(TEvColumnShard::TEvProposeTransactionResult::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui64 tabletId = msg->Record.GetOrigin();
            const auto status = msg->Record.GetStatus();

            TXLOG_DEBUG("Received TEvProposeTransactionResult from"
                << " ColumnShard# " << tabletId
                << " Status# " << NKikimrTxColumnShard::EResultStatus_Name(status));

            if (!WaitingShards.contains(tabletId)) {
                // ignore unexpected messages
                return;
            }

            switch (status) {
                case NKikimrTxColumnShard::SUCCESS: {
                    WaitingShards.erase(tabletId);
                    if (!WaitingShards.empty()) {
                        return;
                    }
                    return Finish();
                }

                default: {
                    NYql::TIssues issues;
                    issues.AddIssue("TODO: need mapping from shard errors to api errors");
                    return FinishWithError(Ydb::StatusIds::GENERIC_ERROR, std::move(issues));
                }
            }
        }

        void HandlePlan(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui64 tabletId = msg->TabletId;

            TXLOG_DEBUG("Delivery problem"
                << " TabletId# " << tabletId
                << " NotDelivered# " << msg->NotDelivered);

            if (tabletId == SelectedCoordinator) {
                if (PlanStep) {
                    // We have already received coordinator reply
                    return;
                }
                if (msg->NotDelivered) {
                    // Plan request was not delivered, so cancel the transaction
                    CancelProposal();

                    NYql::TIssues issues;
                    issues.AddIssue("Coordinator not available, transaction was not committed");
                    return FinishWithError(Ydb::StatusIds::UNAVAILABLE, std::move(issues));
                }
            } else if (!WaitingShards.contains(tabletId)) {
                // We are not waiting for results from this shard
                return;
            }

            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Shard " << tabletId << " is not available");
            return FinishWithError(Ydb::StatusIds::UNDETERMINED, std::move(issues));
        }

    private:
        void CancelProposal() {
            for (const auto& pr : Params.ColumnShardWrites) {
                const ui64 tabletId = pr.first;
                SendToTablet(tabletId, MakeHolder<TEvColumnShard::TEvCancelTransactionProposal>(TxId), false);
            }
        }

    private:
        void Finish() {
            Send(Parent, new TEvPrivate::TEvCommitFinished(Params.TxId, Ydb::StatusIds::SUCCESS));
            PassAway();
        }

        void FinishWithError(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
            Send(Parent, new TEvPrivate::TEvCommitFinished(Params.TxId, status, std::move(issues)));
            PassAway();
        }

    private:
        void SendToTablet(ui64 tabletId, THolder<IEventBase> event, bool subscribe = true) {
            Send(Services.LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), tabletId, subscribe));
        }

    private:
        const TActorId Parent;
        const TParams Params;
        TString LogPrefix;
        ui64 TxId = 0;
        NTxProxy::TTxProxyServices Services;
        THashSet<ui64> WaitingShards;
        ui64 SelectedCoordinator = 0;
        ui64 MinStep = 0;
        ui64 MaxStep = Max<ui64>();
        ui64 PlanStep = 0;
    };

    void TLongTxServiceActor::StartCommitActor(TTransaction& tx) {
        TCommitActor::TParams params;
        params.TxId = tx.TxId;
        params.DatabaseName = tx.DatabaseName;
        params.ColumnShardWrites = tx.ColumnShardWrites;
        tx.CommitActor = Register(new TCommitActor(SelfId(), std::move(params)));
        tx.State = ETxState::Committing;
    }

} // namespace NLongTxService
} // namespace NKikimr
