#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"

#include <util/string/vector.h>

#define YDB_LOG_THIS_FILE_COMPONENT TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxPlanStep: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxPlanStep(TColumnShard* self, TEvTxProcessing::TEvPlanStep::TPtr& ev)
        : TBase(self)
        , Ev(ev)
        , TabletTxNo(++Self->TabletTxCounter)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

    TTxType GetTxType() const override {
        return TXTYPE_PLANSTEP;
    }

private:
    TEvTxProcessing::TEvPlanStep::TPtr Ev;
    const ui32 TabletTxNo;
    THashMap<TActorId, std::vector<ui64>> TxAcks;
    std::unique_ptr<TEvTxProcessing::TEvPlanStepAccepted> Result;

    TStringBuilder TxPrefix() const {
        return TStringBuilder() << "TxPlanStep[" << ToString(TabletTxNo) << "] ";
    }

    TString TxSuffix() const {
        return TStringBuilder() << " at tablet " << Self->TabletID();
    }
};

bool TTxPlanStep::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_ABORT_UNLESS(Ev);
    YDB_LOG_DEBUG("Execute",
        {"txPrefix", TxPrefix()},
        {"txSuffix", TxSuffix()});

    txc.DB.NoMoreReadsForTx();
    NIceDb::TNiceDb db(txc.DB);

    auto& record = Ev->Get()->Record;
    ui64 step = record.GetStep();

    std::vector<ui64> txIds;
    for (const auto& tx : record.GetTransactions()) {
        Y_ABORT_UNLESS(tx.HasTxId());

        txIds.push_back(tx.GetTxId());

        // Note: we plan to remove AckTo in the future
        if (tx.HasAckTo()) {
            TActorId txOwner = ActorIdFromProto(tx.GetAckTo());
            // Note: when mediators ack transactions on their own they also
            // specify an empty AckTo. Sends to empty actors are a no-op anyway.
            TxAcks[txOwner].push_back(tx.GetTxId());
        }
    }

    size_t plannedCount = 0;
    if (step > Self->LastPlannedStep) {
        ui64 lastTxId = 0;
        for (ui64 txId : txIds) {
            Y_ABORT_UNLESS(lastTxId < txId, "Transactions must be sorted and unique");
            auto planResult = Self->ProgressTxController->PlanTx(step, txId, txc);
            switch (planResult) {
                case TTxController::EPlanResult::Skipped: {
                    YDB_LOG_WARN("Ignoring step for unknown txId",
                        {"txPrefix", TxPrefix()},
                        {"step", step},
                        {"txId", txId},
                        {"txSuffix", TxSuffix()});
                    break;
                }
                case TTxController::EPlanResult::AlreadyPlanned: {
                    YDB_LOG_WARN("Ignoring step for txId which is already planned for step",
                        {"txPrefix", TxPrefix()},
                        {"step", step},
                        {"txId", txId},
                        {"#_dup_step", step},
                        {"txSuffix", TxSuffix()});
                    break;
                }
                case TTxController::EPlanResult::Planned: {
                    ++plannedCount;
                    break;
                }
            }
            lastTxId = txId;
        }
        Self->LastPlannedStep = step;
        Self->LastPlannedTxId = lastTxId;
        Schema::SaveSpecialValue(db, Schema::EValueIds::LastPlannedStep, Self->LastPlannedStep);
        Schema::SaveSpecialValue(db, Schema::EValueIds::LastPlannedTxId, Self->LastPlannedTxId);
        Self->RescheduleWaitingReads();
    } else {
        YDB_LOG_ERROR("Ignore old txIds for step last planned step",
            {"txPrefix", TxPrefix()},
            {"#_num_0", JoinStrings(txIds.begin(), txIds.end(), ", ")},
            {"step", step},
            {"#_Self->LastPlannedStep", Self->LastPlannedStep},
            {"txSuffix", TxSuffix()});
    }

    Result = std::make_unique<TEvTxProcessing::TEvPlanStepAccepted>(Self->TabletID(), step);

    Self->Counters.GetTabletCounters()->IncCounter(COUNTER_PLAN_STEP_ACCEPTED);

    if (plannedCount > 0 || Self->ProgressTxController->HaveOutdatedTxs()) {
        Self->EnqueueProgressTx(ctx);
    }
    return true;
}

void TTxPlanStep::Complete(const TActorContext& ctx) {
    Y_ABORT_UNLESS(Ev);
    Y_ABORT_UNLESS(Result);
    YDB_LOG_DEBUG("Complete",
        {"txPrefix", TxPrefix()},
        {"txSuffix", TxSuffix()});

    ui64 step = Ev->Get()->Record.GetStep();
    for (auto& kv : TxAcks) {
        ctx.Send(kv.first, new TEvTxProcessing::TEvPlanStepAck(Self->TabletID(), step, kv.second.begin(), kv.second.end()));
    }

    ctx.Send(Ev->Sender, Result.release());
}

void TColumnShard::Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx) {
    ui64 step = ev->Get()->Record.GetStep();
    ui64 mediatorId = ev->Get()->Record.GetMediatorID();
    YDB_LOG_DEBUG("PlanStep at tablet mediator",
        {"step", step},
        {"tabletID", TabletID()},
        {"mediatorId", mediatorId});

    Execute(new TTxPlanStep(this, ev), ctx);
}

}   // namespace NKikimr::NColumnShard
