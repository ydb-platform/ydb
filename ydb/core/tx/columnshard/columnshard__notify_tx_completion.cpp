#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

class TTxNotifyTxCompletion: public TTransactionBase<TColumnShard> {
public:
    TTxNotifyTxCompletion(TColumnShard* self, TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        LOG_S_DEBUG("TTxNotifyTxCompletion.Execute at tablet " << Self->TabletID());

        const ui64 txId = Ev->Get()->Record.GetTxId();
        if (const auto* backupTx = Self->LastCompletedBackupTransactionsByTxId.FindPtr(txId)) {
            // The persisted result is authoritative: the tx has already been completed.
            // A retry propose (e.g. schemeshard ConfigureParts after reboot) may have re-registered
            // an operator for the same txId after completion. Such an operator will never be planned,
            // so a subscriber registered on it would never be notified. Reply right away and drop the zombie.
            Result.reset(new TEvColumnShard::TEvNotifyTxCompletionResult(Self->TabletID(), txId));
            *Result->Record.MutableOpResult() = backupTx->GetOpResult();

            auto txOperator = Self->ProgressTxController->GetTxOperator(txId, ETxOperatorStatus::InProgress, /*optional*/ true);
            if (txOperator && !txOperator->IsPlanned()) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("event", "cancel_completed_tx_duplicate")("tx_id", txId);
                Self->ProgressTxController->ExecuteOnCancel(txId, txc);
                CancelOnComplete = true;
            }
            return true;
        }

        auto txOperator = Self->ProgressTxController->GetTxOperator(txId, ETxOperatorStatus::Any, /*optional*/ true);
        if (txOperator) {
            txOperator->RegisterSubscriber(Ev->Sender);
            return true;
        }

        Result.reset(new TEvColumnShard::TEvNotifyTxCompletionResult(Self->TabletID(), txId));
        auto& opResult = *Result->Record.MutableOpResult();
        // We need to fill in op result in this case because
        // it can be an export or import tx and we need to propagate these fields anyway
        opResult.SetSuccess(false);
        opResult.SetExplain("Internal error. No information was found about the transaction");
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (CancelOnComplete) {
            Self->ProgressTxController->CompleteOnCancel(Ev->Get()->Record.GetTxId(), ctx);
        }
        if (Result) {
            ctx.Send(Ev->Sender, Result.release());
        }
    }

    TTxType GetTxType() const override {
        return TXTYPE_NOTIFY_TX_COMPLETION;
    }

private:
    TEvColumnShard::TEvNotifyTxCompletion::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvNotifyTxCompletionResult> Result;
    bool CancelOnComplete = false;
};

void TColumnShard::Handle(TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxNotifyTxCompletion(this, ev), ctx);
}

}   // namespace NKikimr::NColumnShard
