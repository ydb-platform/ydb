#include "columnshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT TX_COLUMNSHARD

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
        YDB_LOG_DEBUG("TTxTxAbort.Execute at tablet",
            {"#_Self->TabletID", Self->TabletID()});

        auto txOperator = Self->ProgressTxController->GetTxOperator(TxId, ETxOperatorStatus::InProgress, /*optional*/ true);
        if (txOperator) {
            DoComplete = true;
            return txOperator->ExecuteOnAbort(*Self, txc);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!DoComplete) {
            return;
        }

        auto txOperator = Self->ProgressTxController->GetTxOperator(TxId, ETxOperatorStatus::Any, /*optional*/ true);
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
    bool DoComplete = false;
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
