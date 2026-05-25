#include "datashard_txs.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

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
        YDB_LOG_CTX_ERROR(ctx, "Unexpected TTxCancelTransactionProposal at tablet follower txId",
            {"TabletID", Self->TabletID()},
            {"TxId", TxId});
        return true;
    }

    if (Self->State == TShardState::Offline || Self->State == TShardState::PreOffline) {
        YDB_LOG_CTX_DEBUG(ctx, "Ignoring TTxCancelTransactionProposal at tablet txId because the tablet is going offline",
            {"TabletID", Self->TabletID()},
            {"TxId", TxId});
        return true;
    }

    YDB_LOG_CTX_DEBUG(ctx, "Start TTxCancelTransactionProposal at tablet txId",
        {"TabletID", Self->TabletID()},
        {"TxId", TxId});

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
}

void TDataShard::Handle(TEvDataShard::TEvCancelTransactionProposal::TPtr &ev, const TActorContext &ctx) {
    ui64 txId = ev->Get()->Record.GetTxId();
    YDB_LOG_CTX_DEBUG(ctx, "Got TEvDataShard::TEvCancelTransactionProposal txId",
        {"TabletID", TabletID()},
        {"txId", txId});

    // Mark any queued proposals as cancelled
    ProposeQueue.Cancel(txId);

    // Cancel transactions that have already been proposed
    Execute(new TTxCancelTransactionProposal(this, txId), ctx);
}

} // namespace NDataShard
} // namespace NKikimr
