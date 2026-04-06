
#include "schemeshard_impl.h"

#define LOG_T(stream) LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)

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
        LOG_N("TForcedCompaction::TTxProgress DoExecute"
            << ", DoneShardsToPersist size: " << Self->DoneShardsToPersist.size()
            << ", CancellingForcedCompactions size: " << Self->CancellingForcedCompactions.size());
        THashSet<TForcedCompactionInfo::TPtr> compactionsToPersist;
        compactionsToPersist.reserve(Self->DoneShardsToPersist.size() + Self->CancellingForcedCompactions.size());
        NIceDb::TNiceDb db(txc.DB);
        for (auto& [shardIdx, forcedCompactionInfo] : Self->DoneShardsToPersist) {
            if (Self->InProgressForcedCompactionsByShard.erase(shardIdx)) {
                forcedCompactionInfo->DoneShardCount++;
            }
            compactionsToPersist.emplace(forcedCompactionInfo);
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
            const auto* shardsQueue = Self->ForcedCompactionShardsByTable.FindPtr(forcedCompactionInfo->TablePathId);
            bool compactionCompleted = (!shardsQueue || shardsQueue->Empty()) && forcedCompactionInfo->ShardsInFlight.empty();
            if (compactionCompleted) {
                if (!shardsQueue || shardsQueue->Empty()) {
                    Self->ForcedCompactionShardsByTable.erase(forcedCompactionInfo->TablePathId);
                    Self->ForcedCompactionTablesQueue.Remove(forcedCompactionInfo->TablePathId);
                }
                Self->InProgressForcedCompactionsByTable.erase(forcedCompactionInfo->TablePathId);
                forcedCompactionInfo->EndTime = ctx.Now();

                TransitToFinalState(*forcedCompactionInfo);
            }
            Self->PersistForcedCompactionState(db, *forcedCompactionInfo);
            SendNotificationsIfFinished(*forcedCompactionInfo, ctx);
        }
        Self->DoneShardsToPersist.clear();
        Self->CancellingForcedCompactions.clear();
        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_N("TForcedCompaction::TTxProgress DoComplete"
            << ", DoneShardsToPersist size: " << Self->DoneShardsToPersist.size()
            << ", CancellingForcedCompactions size: " << Self->CancellingForcedCompactions.size());
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    void SendNotificationsIfFinished(TForcedCompactionInfo& info, const TActorContext& ctx) {
        if (!info.IsFinished()) {
            return;
        }

        LOG_T("TForcedCompaction::TTxProgress SendNotifications: "
            << ": id# " << info.Id
            << ", subscribers count# " << info.Subscribers.size());

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
