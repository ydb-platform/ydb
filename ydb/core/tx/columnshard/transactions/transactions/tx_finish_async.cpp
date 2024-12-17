#include "tx_finish_async.h"

namespace NKikimr::NColumnShard {

bool TTxFinishAsyncTransaction::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    Self->GetProgressTxController().FinishProposeOnExecute(TxId, txc);
    return true;
}

void TTxFinishAsyncTransaction::Complete(const TActorContext& ctx) {
    Self->GetProgressTxController().FinishProposeOnComplete(TxId, ctx);
}

}
