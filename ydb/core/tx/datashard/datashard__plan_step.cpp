#include "datashard_txs.h"

#include <util/string/vector.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

TDataShard::TTxPlanStep::TTxPlanStep(TDataShard *self, TEvTxProcessing::TEvPlanStep::TPtr ev)
    : TBase(self)
    , Ev(ev)
    , IsAccepted(false)
    , RequestStartTime(TAppData::TimeProvider->Now())
{
    Y_ENSURE(Ev);
}

bool TDataShard::TTxPlanStep::Execute(TTransactionContext &txc, const TActorContext &ctx) {
    Y_ENSURE(Ev);

    // TEvPlanStep are strictly ordered by mediator so this Tx must not be retried not to break this ordering!
    txc.DB.NoMoreReadsForTx();

    TxByAck.clear();
    IsAccepted = false;

    const ui64 step = Ev->Get()->Record.GetStep();
    Self->LastKnownMediator = Ev->Get()->Record.GetMediatorID();

    TVector<ui64> txIds;
    txIds.reserve(Ev->Get()->Record.TransactionsSize());
    for (const auto& tx : Ev->Get()->Record.GetTransactions()) {
        Y_ENSURE(tx.HasTxId());

        txIds.push_back(tx.GetTxId());

        // Note: we plan to remove AckTo in the future
        if (tx.HasAckTo()) {
            TActorId txOwner = ActorIdFromProto(tx.GetAckTo());
            // Note: when mediators ack transactions on their own they also
            // specify an empty AckTo. Sends to empty actors are a no-op anyway.
            if (txOwner) {
                TxByAck[txOwner].push_back(tx.GetTxId());
            }
        }
    }

    if (Self->State != TShardState::Offline && Self->State != TShardState::PreOffline) {
        // The DS is completing Drop, so we just ignore PlanStep assuming that it might only contain
        // transactions that have already been executed.
        // NOTE: There is a scenario when because of retries the Coordinator might send some old Tx with
        // a new Step.
        IsAccepted = Self->Pipeline.PlanTxs(step, txIds, txc, ctx);
    }

    if (! IsAccepted) {
        YDB_LOG_CTX_ERROR(ctx, "Ignore old txIds [ ] for step outdated step at tablet",
            {"#_num_0", JoinStrings(txIds.begin(), txIds.end(), ", ")},
            {"step", step},
            {"OutdatedCleanupStep", Self->Pipeline.OutdatedCleanupStep()},
            {"TabletID", Self->TabletID()});
        Self->IncCounter(COUNTER_PLAN_STEP_IGNORED);
        return true;
    }

    for (ui64 txId : txIds) {
        YDB_LOG_CTX_DEBUG(ctx, "Planned transaction txId at step at tablet",
            {"txId", txId},
            {"step", step},
            {"TabletID", Self->TabletID()},
            {"#_Ev->Get()->Record", Ev->Get()->Record});
    }

    // We already know that max observed step is at least this step, avoid
    // waiting for a TEvNotifyPlanStep which would be delayed until mediator
    // processes all acks in the same time cast bucket.
    Self->SendAfterMediatorStepActivate(step, ctx);
    Self->Pipeline.ActivateWaitingTxOps(ctx);
    Self->CheckMediatorStateRestored();

    Self->PlanQueue.Progress(ctx);
    Self->IncCounter(COUNTER_PLAN_STEP_ACCEPTED);
    return true;
}

void TDataShard::TTxPlanStep::Complete(const TActorContext &ctx) {
    Y_ENSURE(Ev);
    ui64 step = Ev->Get()->Record.GetStep();

    for (auto& kv : TxByAck) {
        THolder<TEvTxProcessing::TEvPlanStepAck> ack =
            MakeHolder<TEvTxProcessing::TEvPlanStepAck>(Self->TabletID(), step, kv.second.begin(), kv.second.end());
        YDB_LOG_CTX_DEBUG(ctx, "Sending '",
            {"ack", ack->ToString()});

        ctx.Send(kv.first, ack.Release()); // Ack to Tx coordinator
    }

    THolder<TEvTxProcessing::TEvPlanStepAccepted> accepted =
        MakeHolder<TEvTxProcessing::TEvPlanStepAccepted>(Self->TabletID(), step);
    YDB_LOG_CTX_DEBUG(ctx, "Sending '",
        {"accepted", accepted->ToString()});

    ctx.Send(Ev->Sender, accepted.Release()); // Reply to the mediator

    if (IsAccepted) {
        TDuration duration = TAppData::TimeProvider->Now() - RequestStartTime;
        Self->IncCounter(COUNTER_ACCEPTED_PLAN_STEP_COMPLETE_LATENCY, duration);
    }
}

class TDataShard::TTxPlanPredictedTxs : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxPlanPredictedTxs(TDataShard* self)
        : TTransactionBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_PLAN_PREDICTED_TXS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Self->ScheduledPlanPredictedTxs = false;

        ui64 step = Self->MediatorTimeCastEntry->Get(Self->TabletID());
        bool planned = Self->Pipeline.PlanPredictedTxs(step, txc, ctx);

        if (Self->Pipeline.HasPredictedPlan()) {
            ui64 nextStep = Self->Pipeline.NextPredictedPlanStep();
            Y_ENSURE(step < nextStep);
            Self->WaitPredictedPlanStep(nextStep);
        }

        if (planned) {
            Self->PlanQueue.Progress(ctx);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }
};

void TDataShard::Handle(TEvPrivate::TEvPlanPredictedTxs::TPtr&, const TActorContext& ctx) {
    Y_ENSURE(ScheduledPlanPredictedTxs);
    Execute(new TTxPlanPredictedTxs(this), ctx);
}

void TDataShard::SchedulePlanPredictedTxs() {
    if (!ScheduledPlanPredictedTxs) {
        ScheduledPlanPredictedTxs = true;
        Send(SelfId(), new TEvPrivate::TEvPlanPredictedTxs());
    }
}

}}
