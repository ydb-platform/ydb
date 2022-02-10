#include "datashard_txs.h"

namespace NKikimr {

namespace NDataShard {

    TDataShard::TTxProgressResendRS::TTxProgressResendRS(TDataShard *self, ui64 seqno)
        : TBase(self)
        , Seqno(seqno)
    {
    }

    bool TDataShard::TTxProgressResendRS::Execute(TTransactionContext &txc, const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "Start TTxProgressResendRS at tablet %" PRIu64, Self->TabletID());
        return Self->OutReadSets.ResendRS(txc, ctx, Seqno);
    }

    void TDataShard::TTxProgressResendRS::Complete(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        /* no-op */
    }
}

}
