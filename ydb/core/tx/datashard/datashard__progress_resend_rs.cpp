#include "datashard_txs.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {

namespace NDataShard {

    TDataShard::TTxProgressResendRS::TTxProgressResendRS(TDataShard *self, ui64 seqno)
        : TBase(self)
        , Seqno(seqno)
    {
    }

    bool TDataShard::TTxProgressResendRS::Execute(TTransactionContext &txc, const TActorContext &ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Start TTxProgressResendRS at tablet",
            {"#_Self->TabletID", Self->TabletID()});
        return Self->OutReadSets.ResendRS(txc, ctx, Seqno);
    }

    void TDataShard::TTxProgressResendRS::Complete(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        /* no-op */
    }
}

}
