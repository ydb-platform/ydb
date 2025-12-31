#include "columnshard_impl.h"
#include "columnshard_schema.h"

namespace NKikimr::NColumnShard {

class TColumnShard::TTxProposeCancel : public TTransactionBase<TColumnShard> {
public:
    TTxProposeCancel(TColumnShard* self, TEvDataShard::TEvCancelTransactionProposal::TPtr& ev)
        : TTransactionBase(self)
        , Ev(ev)
    { }

    TTxType GetTxType() const override { return TXTYPE_PROPOSE_CANCEL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        LOG_S_DEBUG("TTxProposeCancel.Execute");

        NIceDb::TNiceDb db(txc.DB);

        const auto* msg = Ev->Get();
        const ui64 txId = msg->Record.GetTxId();
        Self->ProgressTxController->ExecuteOnCancel(txId, txc);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_S_DEBUG("TTxProposeCancel.Complete");
        const auto* msg = Ev->Get();
        const ui64 txId = msg->Record.GetTxId();
        Self->ProgressTxController->CompleteOnCancel(txId, ctx);
    }

private:
    const TEvDataShard::TEvCancelTransactionProposal::TPtr Ev;
};

void TColumnShard::Handle(TEvDataShard::TEvCancelTransactionProposal::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxProposeCancel(this, ev), ctx);
}

}
