#include "columnshard_impl.h"
#include "columnshard_schema.h"

#include <ydb/core/tx/columnshard/operations/write.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_TX

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

public:
    TTxProgressTx(TColumnShard* self)
        : TTransactionBase(self)
        , TabletTxNo(++Self->TabletTxCounter)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD_TX,
            {"tabletId", Self->TabletID()},
            {"txState", "TTxProgressTx::Execute"},
            {"txCurrent", Self->ProgressTxInFlight});
        if (!Self->ProgressTxInFlight) {
            AbortedThroughRemoveExpired = true;
            return true;
        }
        Self->Counters.GetTabletCounters()->SetCounter(COUNTER_TX_COMPLETE_LAG, Self->GetTxCompleteLag().MilliSeconds());

        const size_t removedCount = Self->ProgressTxController->CleanExpiredTxs();
        if (removedCount > 0) {
            // We cannot continue with this transaction, start a new transaction
            AbortedThroughRemoveExpired = true;
            Self->Execute(new TTxProgressTx(Self), ctx);
            return true;
        }

        // Process a single transaction at the front of the queue
        const auto plannedItem = Self->ProgressTxController->GetFirstPlannedTx();
        if (!!plannedItem) {
            PlannedQueueItem.emplace(plannedItem->PlanStep, plannedItem->TxId);
            const ui64 step = plannedItem->PlanStep;
            const ui64 txId = plannedItem->TxId;
            YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD_TX,
                {"txId", txId});
            TxOperator = Self->ProgressTxController->GetTxOperatorVerified(txId);
            YDB_LOG_DEBUG("",
                {"event", "PlannedItemStart"},
                {"opType", TxOperator->GetOpType()});
            if (auto txPrepare = TxOperator->BuildTxPrepareForProgress(Self)) {
                YDB_LOG_DEBUG("",
                    {"event", "PlannedItemStart"},
                    {"details", "BuildTxPrepareForProgress"},
                    {"opType", TxOperator->GetOpType()});
                AbortedThroughRemoveExpired = true;
                Self->ProgressTxInFlight = txId;
                Self->Execute(txPrepare.release(), ctx);
                return true;
            } else if (TxOperator->IsInProgress()) {
                AbortedThroughRemoveExpired = true;
                YDB_LOG_DEBUG("",
                    {"event", "PlannedItemContinue"},
                    {"opType", TxOperator->GetOpType()});
                AFL_VERIFY(Self->ProgressTxInFlight == txId);
                return true;
            } else {
                YDB_LOG_DEBUG("",
                    {"event", "PlannedItemStart"},
                    {"details", "PopFirstPlannedTx"},
                    {"opType", TxOperator->GetOpType()});
                Self->ProgressTxController->PopFirstPlannedTx();
            }
            StartExecution = TMonotonic::Now();

            LastCompletedTx = NOlap::TSnapshot(step, txId);
            AFL_VERIFY(LastCompletedTx > Self->LastCompletedTx)("self.last_completed_tx", Self->LastCompletedTx)(
                "last_completed_step", LastCompletedTx);
            NIceDb::TNiceDb db(txc.DB);
            Schema::SaveSpecialValue(db, Schema::EValueIds::LastCompletedStep, LastCompletedTx->GetPlanStep());
            Schema::SaveSpecialValue(db, Schema::EValueIds::LastCompletedTxId, LastCompletedTx->GetTxId());

            AFL_VERIFY(TxOperator->ProgressOnExecute(*Self, NOlap::TSnapshot(step, txId), txc));
            Self->ProgressTxController->ProgressOnExecute(txId, txc);
            Self->Counters.GetTabletCounters()->IncCounter(COUNTER_PLANNED_TX_COMPLETED);
        }
        Self->ProgressTxInFlight = std::nullopt;
        if (!!Self->ProgressTxController->GetPlannedTx()) {
            Self->EnqueueProgressTx(ctx, std::nullopt);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (AbortedThroughRemoveExpired) {
            return;
        }
        YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD_TX,
            {"tabletId", Self->TabletID()},
            {"txState", "TTxProgressTx::Complete"});
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
    YDB_LOG_DEBUG("",
        {"event", "EnqueueProgressTx"},
        {"tabletId", TabletID()},
        {"txId", continueTxId});
    if (continueTxId) {
        AFL_VERIFY(!ProgressTxInFlight || ProgressTxInFlight == continueTxId)("current", ProgressTxInFlight)("expected", continueTxId);
    }
    if (!ProgressTxInFlight || ProgressTxInFlight == continueTxId) {
        YDB_LOG_DEBUG("",
            {"event", "EnqueueProgressTxStart"},
            {"tabletId", TabletID()},
            {"txId", continueTxId},
            {"txCurrent", ProgressTxInFlight});
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
