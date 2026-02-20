
#include "schemeshard_impl.h"

#define LOG_T(stream) LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxProgress: public TRwTxBase {
    explicit TTxProgress(TSelf* self)
        : TRwTxBase(self)
    {}

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_N("TForcedCompaction::TTxProgress DoExecute");
        NIceDb::TNiceDb db(txc.DB);
        THashSet<TForcedCompactionInfo::TPtr> compactionsToPersist;
        for (auto& [shardIdx, forcedCompactionInfo] : Self->DoneShardsToPersist) {
            if (Self->InProgressForcedCompactionsByShard.erase(shardIdx)) {
                forcedCompactionInfo->DoneShardCount++;
            }
            compactionsToPersist.insert(forcedCompactionInfo);
            Self->PersistForcedCompactionDoneShard(db, shardIdx);
        }

        for (auto& forcedCompactionInfo : compactionsToPersist) {
            const auto* shardsQueue = Self->ForcedCompactionShardsByTable.FindPtr(forcedCompactionInfo->TablePathId);
            bool compactionCompleted = (!shardsQueue || shardsQueue->Empty()) && forcedCompactionInfo->ShardsInFlight.empty();
            if (compactionCompleted && forcedCompactionInfo->State != TForcedCompactionInfo::EState::Cancelled) {
                if (!shardsQueue) {
                    Self->ForcedCompactionShardsByTable.erase(forcedCompactionInfo->TablePathId);
                    Self->ForcedCompactionTablesQueue.Remove(forcedCompactionInfo->TablePathId);
                }
                forcedCompactionInfo->State = TForcedCompactionInfo::EState::Done;
                forcedCompactionInfo->EndTime = TAppData::TimeProvider->Now();
                Self->InProgressForcedCompactionsByTable.erase(forcedCompactionInfo->TablePathId);
            }
            Self->PersistForcedCompactionState(db, *forcedCompactionInfo);
            SendNotificationsIfFinished(*forcedCompactionInfo, ctx);
        }
        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_N("TForcedCompaction::TTxProgress DoComplete");
        Self->DoneShardsToPersist.clear();
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

private:
    TSideEffects SideEffects;
};

ITransaction* TSchemeShard::CreateTxProgressForcedCompaction() {
    return new TForcedCompaction::TTxProgress(this);
}

} // namespace NKikimr::NSchemeShard
