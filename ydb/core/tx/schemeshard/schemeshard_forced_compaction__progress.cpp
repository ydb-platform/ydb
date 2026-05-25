
#include "schemeshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxProgress: public TRwTxBase {
    explicit TTxProgress(TSelf* self)
        : TRwTxBase(self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_PROGRESS_FORCED_COMPACTION;
    }

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        YDB_LOG_CTX_NOTICE(ctx, "][ForcedCompaction] TForcedCompaction::TTxProgress DoExecute, ForcedCompactionsDoneShardsToPersist, CancellingForcedCompactions",
            {"SelfTabletId", Self->SelfTabletId()},
            {"size", Self->ForcedCompactionsDoneShardsToPersist.size()},
            {"#_size", Self->CancellingForcedCompactions.size()});
        THashSet<TForcedCompactionInfo::TPtr> compactionsToPersist;
        compactionsToPersist.reserve(Self->ForcedCompactionsDoneShardsToPersist.size() + Self->CancellingForcedCompactions.size());
        NIceDb::TNiceDb db(txc.DB);
        for (auto& [shardIdx, forcedCompactionInfo] : Self->ForcedCompactionsDoneShardsToPersist) {
            if (Self->InProgressForcedCompactionsByShard.erase(shardIdx) && forcedCompactionInfo) {
                forcedCompactionInfo->DoneShardCount++;
            }
            if (forcedCompactionInfo) {
                compactionsToPersist.emplace(forcedCompactionInfo);
            }
            Self->PersistForcedCompactionDoneShard(db, shardIdx);
        }

        for (auto cancelling : Self->CancellingForcedCompactions) {
            compactionsToPersist.emplace(cancelling.Info);
            if (cancelling.Waiter) {
                auto cancelResponse = MakeHolder<TEvForcedCompaction::TEvCancelResponse>(cancelling.Waiter->TxId);
                cancelResponse->Record.SetStatus(Ydb::StatusIds::SUCCESS);
                SideEffects.Send(
                    cancelling.Waiter->ActorId,
                    cancelResponse.Release(),
                    cancelling.Waiter->Cookie
                );
            }
        }

        for (auto& forcedCompactionInfo : compactionsToPersist) {
            if (Self->IsForcedCompactionCompleted(*forcedCompactionInfo)) {
                for (const auto& tablePathId : forcedCompactionInfo->TablesToCompact) {
                    Self->ForcedCompactionShardsByTable.erase(tablePathId);
                    Self->ForcedCompactionTablesQueue.Remove(tablePathId);
                    Self->InProgressForcedCompactionsByTable.erase(tablePathId);
                }
                forcedCompactionInfo->EndTime = ctx.Now();

                TransitToFinalState(*forcedCompactionInfo);
            }
            Self->PersistForcedCompactionState(db, *forcedCompactionInfo);
            SendNotificationsIfFinished(*forcedCompactionInfo, ctx);
        }
        Self->ForcedCompactionsDoneShardsToPersist.clear();
        Self->CancellingForcedCompactions.clear();
        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        YDB_LOG_CTX_NOTICE(ctx, "][ForcedCompaction] TForcedCompaction::TTxProgress DoComplete, ForcedCompactionsDoneShardsToPersist, CancellingForcedCompactions",
            {"SelfTabletId", Self->SelfTabletId()},
            {"size", Self->ForcedCompactionsDoneShardsToPersist.size()},
            {"#_size", Self->CancellingForcedCompactions.size()});
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    void SendNotificationsIfFinished(TForcedCompactionInfo& info, const TActorContext& ctx) {
        if (!info.IsFinished()) {
            return;
        }

        YDB_LOG_CTX_TRACE(ctx, "][ForcedCompaction] TForcedCompaction::TTxProgress SendNotifications:, subscribers",
            {"SelfTabletId", Self->SelfTabletId()},
            {"id", info.Id},
            {"count", info.Subscribers.size()});

        TSet<TActorId> toAnswer;
        toAnswer.swap(info.Subscribers);
        for (auto& actorId: toAnswer) {
            SideEffects.Send(actorId, MakeHolder<TEvSchemeShard::TEvNotifyTxCompletionResult>(info.Id));
        }
    }

    void TransitToFinalState(TForcedCompactionInfo& info) {
        switch (info.State) {
            case TForcedCompactionInfo::EState::InProgress:
                info.State = TForcedCompactionInfo::EState::Done;
                break;
            case TForcedCompactionInfo::EState::Cancelling:
                info.State = TForcedCompactionInfo::EState::Cancelled;
                break;
            default:
                break;
        }
    }

private:
    TSideEffects SideEffects;
};

ITransaction* TSchemeShard::CreateTxProgressForcedCompaction() {
    return new TForcedCompaction::TTxProgress(this);
}

} // namespace NKikimr::NSchemeShard
