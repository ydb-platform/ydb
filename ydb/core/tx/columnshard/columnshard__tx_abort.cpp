#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

class TTxTxAbort: public TTransactionBase<TColumnShard> {
public:
    TTxTxAbort(TColumnShard* self, ui64 txId, const TActorId& subscriber)
        : TBase(self)
        , TxId(txId)
        , Subscriber(subscriber)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        LOG_S_DEBUG("TTxTxAbort.Execute at tablet " << Self->TabletID());

        auto txOperator = Self->ProgressTxController->GetTxOperatorOptional(TxId);
        if (txOperator) {
            return txOperator->ExecuteOnAbort(*Self, txc);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto txOperator = Self->ProgressTxController->GetTxOperatorOptional(TxId);
        if (txOperator) {
            txOperator->CompleteOnAbort(*Self, ctx);
        }

        if (const auto* backupTx = Self->LastCompletedBackupTransactionsByTxId.FindPtr(TxId)) {
            auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(Self->TabletID(), TxId);
            auto& opResult = *event->Record.MutableOpResult();
            opResult = backupTx->GetOpResult();
            ctx.Send(Subscriber, event.Release(), 0, 0);
        } else {   // for backward compatibility (remove it after stable-26.3.1)
            auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(Self->TabletID(), TxId);
            auto& opResult = *event->Record.MutableOpResult();
            opResult.SetSuccess(false);
            opResult.SetExplain("Cancelled manually");
            ctx.Send(Subscriber, event.Release(), 0, 0);
        }
    }

    TTxType GetTxType() const override {
        return TXTYPE_TX_ABORT;
    }

private:
    ui64 TxId = 0;
    TActorId Subscriber;
};

void TColumnShard::Handle(TEvDataShard::TEvCancelBackup::TPtr& ev, const TActorContext& ctx) {
    const ui64 txId = ev->Get()->Record.GetBackupTxId();
    Execute(new TTxTxAbort(this, txId, ev->Sender), ctx);
}

void TColumnShard::Handle(TEvDataShard::TEvCancelRestore::TPtr& ev, const TActorContext& ctx) {
    const ui64 txId = ev->Get()->Record.GetRestoreTxId();
    Execute(new TTxTxAbort(this, txId, ev->Sender), ctx);
}

}   // namespace NKikimr::NColumnShard
