#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

TDataShard::TTxCancelTransactionProposal::TTxCancelTransactionProposal(TDataShard *self,
                                                                              ui64 txId)
    : TBase(self)
    , TxId(txId)
{
}

bool TDataShard::TTxCancelTransactionProposal::Execute(TTransactionContext &txc,
                                                              const TActorContext &ctx)
{
    if (Self->IsFollower()) {
        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Unexpected TTxCancelTransactionProposal at tablet follower "
                    << Self->TabletID() << " txId " << TxId);
        return true;
    }

    if (Self->State == TShardState::Offline || Self->State == TShardState::PreOffline) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Ignoring TTxCancelTransactionProposal at tablet " << Self->TabletID()
                    << " txId " << TxId << " because the tablet is going offline");
        return true;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "Start TTxCancelTransactionProposal at tablet " << Self->TabletID()
                << " txId " << TxId);

    NIceDb::TNiceDb db(txc.DB);
    return Self->Pipeline.CancelPropose(db, ctx, TxId);
}

void TDataShard::TTxCancelTransactionProposal::Complete(const TActorContext &ctx)
{
    Self->CheckSplitCanStart(ctx);
    Self->CheckMvccStateChangeCanStart(ctx);
}

} // namespace NDataShard
} // namespace NKikimr
