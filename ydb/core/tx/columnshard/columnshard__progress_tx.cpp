#include "columnshard_impl.h"
#include "columnshard_schema.h"

#include <ydb/core/tx/columnshard/operations/write.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NColumnShard {

class TColumnShard::TTxProgressTx: public TTransactionBase<TColumnShard> {
private:
    bool AbortedThroughRemoveExpired = false;
    TTxController::ITransactionOperator::TPtr TxOperator;
    const ui32 TabletTxNo;
    std::optional<NOlap::TSnapshot> LastCompletedTx;
    std::optional<TTxController::TPlanQueueItem> PlannedQueueItem;
    std::optional<TMonotonic> StartExecution;
    const TMonotonic ConstructionInstant = TMonotonic::Now();
    static inline TAtomicCounter Id = 0;
    const ui64 Identifier = Id.Inc();

public:
    TTxProgressTx(TColumnShard* self)
        : TTransactionBase(self)
        , TabletTxNo(++Self->TabletTxCounter) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_TX)(
            "tablet_id", Self->TabletID())("tx_state", "TTxProgressTx::Execute")("tx_current", Self->ProgressTxInFlight)("this", Identifier));
        if (!Self->ProgressTxInFlight) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "Aborted");
            AbortedThroughRemoveExpired = true;
            return true;
        }
        Self->Counters.GetTabletCounters()->SetCounter(COUNTER_TX_COMPLETE_LAG, Self->GetTxCompleteLag().MilliSeconds());

        const size_t removedCount = Self->ProgressTxController->CleanExpiredTxs(txc);
        if (removedCount > 0) {
            // We cannot continue with this transaction, start a new transaction
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "Aborted");
            AbortedThroughRemoveExpired = true;
            Self->Execute(new TTxProgressTx(Self), ctx);
            return true;
        }

        // Process a single transaction at the front of the queue
        const auto plannedItem = Self->ProgressTxController->GetFirstPlannedTx();
        if (!!plannedItem) {
            const NActors::TLogContextGuard logGuard1(NActors::TLogContextBuilder::Build()("tx_id", plannedItem->TxId));
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "PlannedItemStart");
            PlannedQueueItem.emplace(*Self->ProgressTxController->GetPlannedTx());
            const ui64 step = plannedItem->PlanStep;
            const ui64 txId = plannedItem->TxId;
            AFL_VERIFY(Self->ProgressTxInFlight == txId || !*Self->ProgressTxInFlight)("in_flight", Self->ProgressTxInFlight);
            NActors::TLogContextGuard logGuardTx = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_TX);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "PlannedItemStart");
            TxOperator = Self->ProgressTxController->GetTxOperatorVerified(txId);
            AFL_VERIFY(TxOperator->GetTxId() == txId)("op", TxOperator->GetTxId());
            if (auto txPrepare = TxOperator->BuildTxPrepareForProgress(Self)) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "PlannedItemStart")("details", "BuildTxPrepareForProgress");
                AbortedThroughRemoveExpired = true;
                Self->ProgressTxInFlight = txId;
                Self->Execute(txPrepare.release(), ctx);
                return true;
            } else if (TxOperator->IsInProgress()) {
                AbortedThroughRemoveExpired = true;
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "PlannedItemContinue");
                AFL_VERIFY(Self->ProgressTxInFlight == txId);
                return true;
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "PlannedItemStart")("details", "PopFirstPlannedTx");
                Self->ProgressTxController->PopFirstPlannedTx();
            }
            StartExecution = TMonotonic::Now();

            LastCompletedTx = NOlap::TSnapshot(step, txId);
            if (LastCompletedTx > Self->LastCompletedTx) {
                NIceDb::TNiceDb db(txc.DB);
                Schema::SaveSpecialValue(db, Schema::EValueIds::LastCompletedStep, LastCompletedTx->GetPlanStep());
                Schema::SaveSpecialValue(db, Schema::EValueIds::LastCompletedTxId, LastCompletedTx->GetTxId());
            }

            AFL_VERIFY(TxOperator->ProgressOnExecute(*Self, NOlap::TSnapshot(step, txId), txc));
            Self->ProgressTxController->ProgressOnExecute(txId, txc);
            Self->Counters.GetTabletCounters()->IncCounter(COUNTER_PLANNED_TX_COMPLETED);
        } else {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "NoPlannedItem");
        }
        Self->ProgressTxInFlight = std::nullopt;
        if (!!Self->ProgressTxController->GetPlannedTx()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "NoPlannedItem")("tx_id", Self->ProgressTxController->GetPlannedTx()->TxId);
            Self->EnqueueProgressTx(ctx, std::nullopt);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD_TX)(
            "tablet_id", Self->TabletID())("tx_state", "TTxProgressTx::Complete")("tx_current", Self->ProgressTxInFlight)("this", Identifier);
        if (AbortedThroughRemoveExpired) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "AbortedThroughRemoveExpired");
            return;
        }
        if (TxOperator) {
            TxOperator->ProgressOnComplete(*Self, ctx);
            Self->RescheduleWaitingReads();
        }
        if (PlannedQueueItem) {
            AFL_VERIFY(TxOperator);
            Self->GetProgressTxController().GetCounters().OnTxProgressLag(
                TxOperator->GetOpType(), TMonotonic::Now() - TMonotonic::MilliSeconds(PlannedQueueItem->Step));
            Self->GetProgressTxController().ProgressOnComplete(*PlannedQueueItem);
        }
        if (LastCompletedTx) {
            Self->LastCompletedTx = std::max(*LastCompletedTx, Self->LastCompletedTx);
        }
        if (StartExecution) {
            Self->GetProgressTxController().GetCounters().OnTxExecuteDuration(TxOperator->GetOpType(), TMonotonic::Now() - *StartExecution);
            Self->GetProgressTxController().GetCounters().OnTxLiveDuration(TxOperator->GetOpType(), TMonotonic::Now() - ConstructionInstant);
        }
    }
};

void TColumnShard::EnqueueProgressTx(const TActorContext& ctx, const std::optional<ui64> continueTxId) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "EnqueueProgressTx")("tablet_id", TabletID())("tx_id", continueTxId);
    if (continueTxId) {
        AFL_VERIFY(!ProgressTxInFlight || ProgressTxInFlight == continueTxId)("current", ProgressTxInFlight)("expected", continueTxId);
    }
    if (!ProgressTxInFlight || ProgressTxInFlight == continueTxId) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("event", "EnqueueProgressTxStart")("tablet_id", TabletID())("tx_id", continueTxId)(
            "tx_current", ProgressTxInFlight);
        ProgressTxInFlight = continueTxId.value_or(0);
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

}   // namespace NKikimr::NColumnShard
