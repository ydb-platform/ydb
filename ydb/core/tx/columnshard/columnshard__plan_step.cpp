#include "columnshard_impl.h"
#include "columnshard_private_events.h"
#include "columnshard_schema.h"

#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

using namespace NTabletFlatExecutor;

class TTxPlanStep : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxPlanStep(TColumnShard* self, TEvTxProcessing::TEvPlanStep::TPtr& ev)
        : TBase(self)
        , Ev(ev)
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_PLANSTEP; }

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
    LOG_S_DEBUG(TxPrefix() << "execute" << TxSuffix());

    txc.DB.NoMoreReadsForTx();
    NIceDb::TNiceDb db(txc.DB);

    auto& record = Ev->Get()->Record;
    ui64 step = record.GetStep();

    std::vector<ui64> txIds;
    for (const auto& tx : record.GetTransactions()) {
        Y_ABORT_UNLESS(tx.HasTxId());
        Y_ABORT_UNLESS(tx.HasAckTo());

        txIds.push_back(tx.GetTxId());

        TActorId txOwner = ActorIdFromProto(tx.GetAckTo());
        TxAcks[txOwner].push_back(tx.GetTxId());
    }

    size_t plannedCount = 0;
    if (step > Self->LastPlannedStep) {
        ui64 lastTxId = 0;
        for (ui64 txId : txIds) {
            Y_ABORT_UNLESS(lastTxId < txId, "Transactions must be sorted and unique");
            auto planResult = Self->ProgressTxController->PlanTx(step, txId, txc);
            switch (planResult) {
                case TTxController::EPlanResult::Skipped:
                {
                    LOG_S_WARN(TxPrefix() << "Ignoring step " << step
                    << " for unknown txId " << txId
                    << TxSuffix());
                    break;
                }
                case TTxController::EPlanResult::AlreadyPlanned:
                {
                    LOG_S_WARN(TxPrefix() << "Ignoring step " << step
                        << " for txId " << txId
                        << " which is already planned for step " << step
                        << TxSuffix());
                    break;
                }
                case TTxController::EPlanResult::Planned:
                {
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
        LOG_S_ERROR(TxPrefix() << "Ignore old txIds ["
            << JoinStrings(txIds.begin(), txIds.end(), ", ")
            << "] for step " << step
            << " last planned step " << Self->LastPlannedStep
            << TxSuffix());
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
    LOG_S_DEBUG(TxPrefix() << "complete" << TxSuffix());

    ui64 step = Ev->Get()->Record.GetStep();
    for (auto& kv : TxAcks) {
        ctx.Send(kv.first, new TEvTxProcessing::TEvPlanStepAck(Self->TabletID(), step, kv.second.begin(), kv.second.end()));
    }

    ctx.Send(Ev->Sender, Result.release());
}


void TColumnShard::Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx) {
    ui64 step = ev->Get()->Record.GetStep();
    ui64 mediatorId = ev->Get()->Record.GetMediatorID();
    LOG_S_DEBUG("PlanStep " << step << " at tablet " << TabletID() << ", mediator " << mediatorId);

    Execute(new TTxPlanStep(this, ev), ctx);
}

}
