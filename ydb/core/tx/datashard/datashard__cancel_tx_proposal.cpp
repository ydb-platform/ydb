#include "datashard_txs.h"

namespace NKikimr {
namespace NDataShard {

class TDataShard::TTxCancelTransactionProposal : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxCancelTransactionProposal(TDataShard *self, ui64 txId);
    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override;
    void Complete(const TActorContext &ctx) override;
    TTxType GetTxType() const override { return TXTYPE_CANCEL_TX_PROPOSAL; }
private:
    const ui64 TxId;
    std::vector<std::unique_ptr<IEventHandle>> Replies;
    TMonotonic ReplyTs;
};

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
    if (!Self->Pipeline.CancelPropose(db, ctx, TxId, Replies)) {
        // Page fault, try again
        return false;
    }

    if (!Replies.empty() && !txc.DB.HasChanges()) {
        // We want to send confirmed replies when cleaning up volatile transactions
        ReplyTs = Self->ConfirmReadOnlyLease();
    }

    return true;
}

void TDataShard::TTxCancelTransactionProposal::Complete(const TActorContext &ctx)
{
    if (ReplyTs) {
        Self->SendConfirmedReplies(ReplyTs, std::move(Replies));
    } else {
        Self->SendCommittedReplies(std::move(Replies));
    }
    Self->CheckSplitCanStart(ctx);
    Self->CheckMvccStateChangeCanStart(ctx);
}

void TDataShard::Handle(TEvDataShard::TEvCancelTransactionProposal::TPtr &ev, const TActorContext &ctx) {
    ui64 txId = ev->Get()->Record.GetTxId();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Got TEvDataShard::TEvCancelTransactionProposal " << TabletID()
                << " txId " <<  txId);

    // Mark any queued proposals as cancelled
    ProposeQueue.Cancel(txId);

    // Cancel transactions that have already been proposed
    Execute(new TTxCancelTransactionProposal(this, txId), ctx);
}

} // namespace NDataShard
} // namespace NKikimr
