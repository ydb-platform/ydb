#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

class TTxTxAbort : public TTransactionBase<TColumnShard> {
public:
    TTxTxAbort(TColumnShard* self, ui64 txId)
        : TBase(self)
        , TxId(txId)
    {}

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
    }

    TTxType GetTxType() const override { return TXTYPE_TX_ABORT; }

private:
    ui64 TxId = 0;
};

void TColumnShard::Handle(TEvDataShard::TEvCancelBackup::TPtr& ev, const TActorContext& ctx) {
    const ui64 txId = ev->Get()->Record.GetBackupTxId();
    Execute(new TTxTxAbort(this, txId), ctx);
}

void TColumnShard::Handle(TEvDataShard::TEvCancelRestore::TPtr& ev, const TActorContext& ctx) {
    const ui64 txId = ev->Get()->Record.GetRestoreTxId();
    Execute(new TTxTxAbort(this, txId), ctx);
}

}
