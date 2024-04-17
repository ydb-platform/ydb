#include "columnshard_impl.h"
#include "columnshard_schema.h"

#include <ydb/core/tx/columnshard/operations/write.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NColumnShard {

class TColumnShard::TTxProgressTx : public TTransactionBase<TColumnShard> {
public:
    TTxProgressTx(TColumnShard* self)
        : TTransactionBase(self)
        , TabletTxNo(++Self->TabletTxCounter)
    {}

    TTxType GetTxType() const override { return TXTYPE_PROGRESS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "execute");
        Y_ABORT_UNLESS(Self->ProgressTxInFlight);
        Self->TabletCounters->Simple()[COUNTER_TX_COMPLETE_LAG].Set(Self->GetTxCompleteLag().MilliSeconds());

        size_t removedCount = Self->ProgressTxController->CleanExpiredTxs(txc);
        if (removedCount > 0) {
            // We cannot continue with this transaction, start a new transaction
            Self->Execute(new TTxProgressTx(Self), ctx);
            return true;
        }

        // Process a single transaction at the front of the queue
        auto plannedItem = Self->ProgressTxController->StartPlannedTx();
        if (!!plannedItem) {
            ui64 step = plannedItem->PlanStep;
            ui64 txId = plannedItem->TxId;
            LastCompletedTx = NOlap::TSnapshot(step, txId);
            if (LastCompletedTx > Self->LastCompletedTx) {
                NIceDb::TNiceDb db(txc.DB);
                Schema::SaveSpecialValue(db, Schema::EValueIds::LastCompletedStep, LastCompletedTx->GetPlanStep());
                Schema::SaveSpecialValue(db, Schema::EValueIds::LastCompletedTxId, LastCompletedTx->GetTxId());
            }

            TxOperator = Self->ProgressTxController->GetVerifiedTxOperator(txId);
            AFL_VERIFY(TxOperator->Progress(*Self, NOlap::TSnapshot(step, txId), txc));
            Self->ProgressTxController->FinishPlannedTx(txId, txc);
            Self->RescheduleWaitingReads();
        }

        Self->ProgressTxInFlight = false;
        if (!!Self->ProgressTxController->GetPlannedTx()) {
            Self->EnqueueProgressTx(ctx);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", Self->TabletID())("tx_state", "complete");
        if (TxOperator) {
            TxOperator->Complete(*Self, ctx);
        }
        if (LastCompletedTx) {
            Self->LastCompletedTx = std::max(*LastCompletedTx, Self->LastCompletedTx);
        }
        Self->SetupIndexation();
    }

private:
    TTxController::ITransactionOperatior::TPtr TxOperator;
    const ui32 TabletTxNo;
    std::optional<NOlap::TSnapshot> LastCompletedTx;
};

void TColumnShard::EnqueueProgressTx(const TActorContext& ctx) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "EnqueueProgressTx")("tablet_id", TabletID());
    if (!ProgressTxInFlight) {
        ProgressTxInFlight = true;
        Execute(new TTxProgressTx(this), ctx);
    }
}

void TColumnShard::Handle(TEvColumnShard::TEvCheckPlannedTransaction::TPtr& ev, const TActorContext& ctx) {
    auto& record = Proto(ev->Get());
    ui64 step = record.GetStep();
    ui64 txId = record.GetTxId();
    LOG_S_DEBUG("CheckTransaction planStep " << step << " txId " << txId << " at tablet " << TabletID());

    auto frontTx = ProgressTxController->GetFrontTx();
    bool finished = step < frontTx.Step || (step == frontTx.Step && txId < frontTx.TxId);
    if (finished) {
        auto txKind = NKikimrTxColumnShard::ETransactionKind::TX_KIND_COMMIT;
        auto status = NKikimrTxColumnShard::SUCCESS;
        auto result = MakeHolder<TEvColumnShard::TEvProposeTransactionResult>(TabletID(), txKind, txId, status);
        result->Record.SetStep(step);

        ctx.Send(ev->Get()->GetSource(), result.Release());
    }

    // For now do not return result for not finished tx. It would be sent in TTxProgressTx::Complete()
}

}
