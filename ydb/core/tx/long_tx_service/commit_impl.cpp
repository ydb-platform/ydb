#include "long_tx_service_impl.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/actorlib_impl/long_timer.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#define TXLOG_LOG(priority, stream) \
    LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::LONG_TX_SERVICE, LogPrefix << stream)
#define TXLOG_DEBUG(stream) TXLOG_LOG(NActors::NLog::PRI_DEBUG, stream)
#define TXLOG_NOTICE(stream) TXLOG_LOG(NActors::NLog::PRI_NOTICE, stream)
#define TXLOG_ERROR(stream) TXLOG_LOG(NActors::NLog::PRI_ERROR, stream)

namespace NKikimr {
namespace NLongTxService {
    struct TRetryData {
        static constexpr ui32 MaxPrepareRetriesPerShard = 100;  // ~30 sec
        static constexpr ui32 MaxPlanRetriesPerShard = 1000;    // ~5 min
        static constexpr ui32 RetryDelayMs = 300;

        std::vector<ui64> WriteIds;
        TString TxBody;
        ui32 NumRetries = 0;
        ui32 WritePartId = 0;

        TString GetWriteIdsStr() const {
            return JoinSeq(", ", WriteIds);
        }
    };

    class TLongTxServiceActor::TCommitActor : public TActorBootstrapped<TCommitActor> {
    public:
        struct TParams {
            TLongTxId TxId;
            TString DatabaseName;
            THashMap<ui64, TTransaction::TShardWriteIds> ColumnShardWrites;
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
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
            }
        }

        void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
            const auto* msg = ev->Get();
            TxId = msg->TxId;
            Services = msg->Services;
            LogPrefix = TStringBuilder() << LogPrefix << " TxId# " << TxId << " ";
            TXLOG_DEBUG("Allocated TxId");
            PrepareTransaction();
        }

    private:
        void PrepareTransaction() {
            for (const auto& pr : Params.ColumnShardWrites) {
                const ui64 tabletId = pr.first;
                NKikimrTxColumnShard::TCommitTxBody tx;
                std::vector<ui64> writeIds;
                for (auto&& wId : pr.second) {
                    tx.AddWriteIds(wId);
                    writeIds.emplace_back(wId);
                }
                TString txBody;
                Y_ABORT_UNLESS(tx.SerializeToString(&txBody));

                WaitingShards.emplace(tabletId, TRetryData{ writeIds, txBody, 0 });
                SendPrepareTransaction(tabletId);
            }
            Become(&TThis::StatePrepare);
        }

        bool SendPrepareTransaction(ui64 tabletId, bool delayed = false) {
            auto it = WaitingShards.find(tabletId);
            if (it == WaitingShards.end()) {
                return false;
            }

            auto& data = it->second;
            if (delayed) {
                if (data.NumRetries >= TRetryData::MaxPrepareRetriesPerShard) {
                    return false;
                }
                ++data.NumRetries;
                if (ToRetry.empty()) {
                    TimeoutTimerActorId = CreateLongTimer(TDuration::MilliSeconds(TRetryData::RetryDelayMs),
                        new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup()));
                }
                ToRetry.insert(tabletId);
                return true;
            }

            TXLOG_DEBUG("Sending TEvProposeTransaction to ColumnShard# " << tabletId << " WriteId# " << data.GetWriteIdsStr());

            SendToTablet(tabletId, MakeHolder<TEvColumnShard::TEvProposeTransaction>(
                    NKikimrTxColumnShard::TX_KIND_COMMIT,
                    SelfId(),
                    TxId,
                    data.TxBody));
            return true;
        }

        STFUNC(StatePrepare) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvColumnShard::TEvProposeTransactionResult, HandlePrepare);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePrepare);
                CFunc(TEvents::TSystem::Wakeup, HandlePrepareTimeout);
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
            const NKikimrTxColumnShard::EResultStatus status = msg->Record.GetStatus();

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

                    auto ydbStatus = NColumnShard::ConvertToYdbStatus(status);
                    NYql::TIssues issues;
                    issues.AddIssue(TStringBuilder() << "Cannot prepare transaction at shard " << tabletId);
                    if (msg->Record.HasStatusMessage()) {
                        issues.AddIssue(msg->Record.GetStatusMessage());
                    }
                    return FinishWithError(ydbStatus, std::move(issues));
                }
            }
        }

        void HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            const auto* msg = ev->Get();
            const ui64 tabletId = msg->TabletId;
            Y_ABORT_UNLESS(tabletId != SelectedCoordinator);

            TXLOG_DEBUG("Delivery problem"
                << " TabletId# " << tabletId
                << " NotDelivered# " << msg->NotDelivered);

            if (!WaitingShards.count(tabletId)) {
                return;
            }

            // Delayed retry
            if (SendPrepareTransaction(tabletId, true)) {
                return;
            }

            // Cancel transaction, since we didn't plan it
            CancelProposal();

            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Shard " << tabletId << " is not available");
            return FinishWithError(Ydb::StatusIds::UNAVAILABLE, std::move(issues));
        }

        void HandlePrepareTimeout(const TActorContext& /*ctx*/) {
            TimeoutTimerActorId = {};
            for (ui64 tabletId : ToRetry) {
                SendPrepareTransaction(tabletId);
            }
            ToRetry.clear();
        }

    private:
        void PlanTransaction() {
            Y_ABORT_UNLESS(SelectedCoordinator);
            Y_ABORT_UNLESS(WaitingShards.empty());
            ToRetry.clear();

            auto req = MakeHolder<TEvTxProxy::TEvProposeTransaction>(
                SelectedCoordinator, TxId, 0, MinStep, MaxStep);
            auto* reqAffectedSet = req->Record.MutableTransaction()->MutableAffectedSet();
            reqAffectedSet->Reserve(Params.ColumnShardWrites.size());

            for (const auto& pr : Params.ColumnShardWrites) {
                const ui64 tabletId = pr.first;
                auto* x = reqAffectedSet->Add();
                x->SetTabletId(tabletId);
                x->SetFlags(/* write */ 2);
                WaitingShards.emplace(tabletId, TRetryData{});
            }

            TXLOG_DEBUG("Sending TEvProposeTransaction to SelectedCoordinator# " << SelectedCoordinator);
            SendToTablet(SelectedCoordinator, std::move(req));
            Become(&TThis::StatePlan);
        }

        STFUNC(StatePlan) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxy::TEvProposeTransactionStatus, HandlePlan);
                hFunc(TEvColumnShard::TEvProposeTransactionResult, HandlePlan);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePlan);
                CFunc(TEvents::TSystem::Wakeup, HandlePlanTimeout);
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
                    // fall through to handle generic errors
                    [[fallthrough]];

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

                case NKikimrTxColumnShard::OUTDATED: {
                    if (!WaitingShards.count(tabletId)) {
                        return;
                    }
                    NYql::TIssues issues;
                    issues.AddIssue(TStringBuilder() << "Shard " << tabletId << " has no info about tx " << TxId);
                    return FinishWithError(Ydb::StatusIds::UNDETERMINED, std::move(issues));
                }

                default: {
                    auto ydbStatus = NColumnShard::ConvertToYdbStatus(status);
                    NYql::TIssues issues;
                    issues.AddIssue(TStringBuilder() << "Cannot plan transaction at shard " << tabletId);
                    if (msg->Record.HasStatusMessage()) {
                        issues.AddIssue(msg->Record.GetStatusMessage());
                    }
                    return FinishWithError(ydbStatus, std::move(issues));
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
            } else {
                if (!PlanStep) {
                    // Waiting for PlanStep. Do not break transaction.
                    return;
                }
                // It's planned, not completed. We could check TEvProposeTransactionResult by PlanStep.
                // - tx is completed if PlanStep:TxId at tablet is greater then ours
                // - tx is not completed otherwise. Keep waiting TEvProposeTransactionResult for it.
                if (SendCheckPlannedTransaction(tabletId, true)) {
                    return;
                }
            }

            NYql::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Shard " << tabletId << " is not available");
            return FinishWithError(Ydb::StatusIds::UNDETERMINED, std::move(issues));
        }

        void HandlePlanTimeout(const TActorContext& /*ctx*/) {
            TimeoutTimerActorId = {};
            for (ui64 tabletId : ToRetry) {
                SendCheckPlannedTransaction(tabletId);
            }
            ToRetry.clear();
        }

        bool SendCheckPlannedTransaction(ui64 tabletId, bool delayed = false) {
            Y_ABORT_UNLESS(PlanStep);

            if (delayed) {
                auto it = WaitingShards.find(tabletId);
                if (it == WaitingShards.end()) {
                    // We are not waiting for results from this shard
                    return true;
                }

                auto& numRetries = it->second.NumRetries;
                if (numRetries >= TRetryData::MaxPlanRetriesPerShard) {
                    return false;
                }
                ++numRetries;

                if (ToRetry.empty()) {
                    TimeoutTimerActorId = CreateLongTimer(TDuration::MilliSeconds(TRetryData::RetryDelayMs),
                        new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup()));
                }
                ToRetry.insert(tabletId);
                return true;
            }

            TXLOG_DEBUG("Ask TEvProposeTransactionResult from ColumnShard# " << tabletId
                << " for PlanStep# " << PlanStep << " TxId# " << TxId);

            SendToTablet(tabletId, MakeHolder<TEvColumnShard::TEvCheckPlannedTransaction>(
                SelfId(),
                PlanStep,
                TxId));
            return true;
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
        THashMap<ui64, TRetryData> WaitingShards;
        ui64 SelectedCoordinator = 0;
        ui64 MinStep = 0;
        ui64 MaxStep = Max<ui64>();
        ui64 PlanStep = 0;
        THashSet<ui64> ToRetry;
        TActorId TimeoutTimerActorId;
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
