#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

class TTxNotifyTxCompletion : public TTransactionBase<TColumnShard> {
public:
    TTxNotifyTxCompletion(TColumnShard* self, TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Y_UNUSED(txc);
        LOG_S_DEBUG("TTxNotifyTxCompletion.Execute at tablet " << Self->TabletID());

        const ui64 txId = Ev->Get()->Record.GetTxId();
        auto txOperator = Self->ProgressTxController->GetTxOperatorOptional(txId);
        if (txOperator) {
            txOperator->RegisterSubscriber(Ev->Sender);
            return true;
        }
        Result.reset(new TEvColumnShard::TEvNotifyTxCompletionResult(Self->TabletID(), txId));
        auto& opResult = *Result->Record.MutableOpResult();
        if (Self->LastCompletedBackupTransaction.GetTxId() == txId) {
            opResult = Self->LastCompletedBackupTransaction.GetOpResult();
            return true;
        }

        // We need to fill in op result in this case because 
        // it can be an export or import tx and we need to propagate these fields anyway
        opResult.SetSuccess(false);
        opResult.SetExplain("Internal error. There weren't found information about transaction");
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Result) {
            ctx.Send(Ev->Sender, Result.release());
        }
    }

    TTxType GetTxType() const override { return TXTYPE_NOTIFY_TX_COMPLETION; }

private:
    TEvColumnShard::TEvNotifyTxCompletion::TPtr Ev;
    std::unique_ptr<TEvColumnShard::TEvNotifyTxCompletionResult> Result;
};

void TColumnShard::Handle(TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxNotifyTxCompletion(this, ev), ctx);
}

}
