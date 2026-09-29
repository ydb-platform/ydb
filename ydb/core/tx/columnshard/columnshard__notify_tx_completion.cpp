#include "columnshard_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

class TTxNotifyTxCompletion: public TTransactionBase<TColumnShard> {
public:
    TTxNotifyTxCompletion(TColumnShard* self, TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Y_UNUSED(txc);
        YDB_LOG_DEBUG("TTxNotifyTxCompletion.Execute at tablet",
            {"tabletId", Self->TabletID()});

        const ui64 txId = Ev->Get()->Record.GetTxId();
        auto txOperator = Self->ProgressTxController->GetTxOperator(txId, ETxOperatorStatus::Any, /*optional*/ true);
        if (txOperator) {
            txOperator->RegisterSubscriber(Ev->Sender);
            return true;
        }
        Result.reset(new TEvColumnShard::TEvNotifyTxCompletionResult(Self->TabletID(), txId));
        auto& opResult = *Result->Record.MutableOpResult();
        if (const auto* backupTx = Self->LastCompletedBackupTransactionsByTxId.FindPtr(txId)) {
            opResult = backupTx->GetOpResult();
            return true;
        }

        // We need to fill in op result in this case because
        // it can be an export or import tx and we need to propagate these fields anyway
        opResult.SetSuccess(false);
        opResult.SetExplain("Internal error. No information was found about the transaction");
        return true;
    }

    void Complete(const TActorContext& ctx) override {
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
};

void TColumnShard::Handle(TEvColumnShard::TEvNotifyTxCompletion::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxNotifyTxCompletion(this, ev), ctx);
}

}   // namespace NKikimr::NColumnShard
