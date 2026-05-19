#include "datashard_txs.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

class TDataShard::TTxCleanupTransaction : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxCleanupTransaction(TDataShard* self)
        : TTransactionBase(self)
    { }

    TTxType GetTxType() const override { return TXTYPE_CLEANUP; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (!Self->IsStateActive()) {
            YDB_LOG_CTX_INFO(ctx, "Cleanup tx at non-ready tablet state",
                {"TabletID", Self->TabletID()},
                {"State", Self->State});
            Self->CleanupQueue.Reset(ctx);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto cleanupStatus = Self->Pipeline.Cleanup(db, ctx, Replies);
        switch (cleanupStatus) {
            case ECleanupStatus::None:
                break;
            case ECleanupStatus::Restart:
                Self->IncCounter(COUNTER_TX_WAIT_DATA);
                return false;
            case ECleanupStatus::Success:
                if (!Replies.empty() && !txc.DB.HasChanges()) {
                    // We want to send confirmed replies when cleaning up volatile transactions
                    ReplyTs = Self->ConfirmReadOnlyLease();
                }
                YDB_LOG_CTX_INFO(ctx, "Cleaned up old txs at TxInFly",
                    {"TabletID", Self->TabletID()},
                    {"TxInFly", Self->TxInFly()});
                Self->IncCounter(COUNTER_TX_PROGRESS_CLEANUP);
                Self->ExecuteCleanupTx(ctx);
                return true;
        }

        // Allow scheduling of new cleanup transactions
        Self->CleanupQueue.Reset(ctx);

        const bool expireSnapshotsAllowed = (
                Self->State == TShardState::Ready ||
                Self->State == TShardState::SplitSrcWaitForNoTxInFlight ||
                Self->State == TShardState::SplitSrcMakeSnapshot);

        if (expireSnapshotsAllowed && Self->GetSnapshotManager().RemoveExpiredSnapshots(ctx.Now(), txc)) {
            YDB_LOG_CTX_DEBUG(ctx, "Removed expired snapshots at",
                {"TabletID", Self->TabletID()});
        }

        const bool needFutureCleanup = Self->TxInFly() > 0 || expireSnapshotsAllowed;

        if (needFutureCleanup) {
            Self->PlanCleanup(ctx);
        }

        // FIXME: this is a historic crutch
        // Cleanup is regularly executed, and an extra progress is used
        // as a workaround for possible bugs with missing progress calls
        if (Self->Pipeline.CanRunAnotherOp()) {
            YDB_LOG_CTX_DEBUG(ctx, "Can run another op at, scheduling plan queue progress",
                {"TabletID", Self->TabletID()});
            Self->PlanQueue.Progress(ctx);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (ReplyTs) {
            Self->SendConfirmedReplies(ReplyTs, std::move(Replies));
        } else {
            Self->SendCommittedReplies(std::move(Replies));
        }
        Self->CheckSplitCanStart(ctx);
    }

private:
    std::vector<std::unique_ptr<IEventHandle>> Replies;
    TMonotonic ReplyTs;
};

void TDataShard::ExecuteCleanupTx(const TActorContext& ctx) {
    Execute(new TTxCleanupTransaction(this), ctx);
}

void TDataShard::Handle(TEvPrivate::TEvCleanupTransaction::TPtr&, const TActorContext& ctx) {
    IncCounter(COUNTER_TX_CLEANUP_SCHEDULED);
    ExecuteCleanupTx(ctx);
}

class TDataShard::TTxCleanupVolatileTransaction : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxCleanupVolatileTransaction(TDataShard* self, ui64 txId)
        : TTransactionBase(self)
        , TxId(txId)
    {}

    TTxType GetTxType() const override { return TXTYPE_CLEANUP_VOLATILE; }

    bool Execute(TTransactionContext&, const TActorContext& ctx) override {
        if (!Self->IsStateActive()) {
            YDB_LOG_CTX_INFO(ctx, "Cleanup volatile tx at non-ready tablet state",
                {"TabletID", Self->TabletID()},
                {"State", Self->State});
            return true;
        }

        if (Self->Pipeline.CleanupVolatile(TxId, ctx, Replies)) {
            YDB_LOG_CTX_INFO(ctx, "Cleaned up volatile tx at TxInFly",
                {"TxId", TxId},
                {"TabletID", Self->TabletID()},
                {"TxInFly", Self->TxInFly()});
            Self->IncCounter(COUNTER_TX_PROGRESS_CLEANUP);

            if (!Replies.empty()) {
                // We want to send confirmed replies when cleaning up volatile transactions
                ReplyTs = Self->ConfirmReadOnlyLease();
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (ReplyTs) {
            Self->SendConfirmedReplies(ReplyTs, std::move(Replies));
        } else {
            Self->SendCommittedReplies(std::move(Replies));
        }
        Self->CheckSplitCanStart(ctx);
    }

private:
    const ui64 TxId;
    std::vector<std::unique_ptr<IEventHandle>> Replies;
    TMonotonic ReplyTs;
};

void TDataShard::ExecuteCleanupVolatileTx(ui64 txId, const TActorContext& ctx) {
    Execute(new TTxCleanupVolatileTransaction(this, txId), ctx);
}

} // namespace NDataShard
} // namespace NKikimr
