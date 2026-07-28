#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

namespace {

bool IsShardPendingDropCleanup(const TSchemeShard& ss, const TShardIdx shardIdx) {
    const auto* shardInfo = ss.ShardInfos.FindPtr(shardIdx);
    if (!shardInfo || shardInfo->TabletType != ETabletType::ColumnShard) {
        return false;
    }
    const auto* path = ss.PathsById.FindPtr(shardInfo->PathId);
    if (!path || !(*path)->Dropped()) {
        // e.g. a legitimately empty shard of an alive store
        return false;
    }
    if (ss.Operations.contains((*path)->DropTxId)) {
        // The drop operation is still in flight and decides the shards' fate itself
        return false;
    }
    return true;
}

}   // namespace

// A polled column shard of a dropped table reported whether external (S3) cleanup is complete
struct TSchemeShard::TTxColumnShardDropCleanupState: public TTransactionBase<TSchemeShard> {
    TEvColumnShard::TEvDropCleanupState::TPtr Ev;
    TSideEffects SideEffects;

    TTxColumnShardDropCleanupState(TSelf* self, TEvColumnShard::TEvDropCleanupState::TPtr& ev)
        : TTransactionBase<TSchemeShard>(self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_COLUMN_SHARD_CLEANUP_COMPLETE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto tabletId = TTabletId(Ev->Get()->Record.GetTabletId());
        Self->ColumnShardDropCleanupSilentPolls.erase(tabletId);

        if (!Ev->Get()->Record.GetReady()) {
            return true;
        }

        const auto* shardIdxPtr = Self->TabletIdToShardIdx.FindPtr(tabletId);
        if (!shardIdxPtr) {
            // Unknown or already deleted tablet; a duplicate of an already processed answer
            return true;
        }
        const auto shardIdx = *shardIdxPtr;
        if (!IsShardPendingDropCleanup(*Self, shardIdx)) {
            return true;
        }

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxColumnShardDropCleanupState: external data cleanup complete"
                       << ", delete shard " << shardIdx
                       << ", tabletId: " << tabletId
                       << ", at schemeshard: " << Self->TabletID());

        SideEffects.DeleteShard(shardIdx);
        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }
};

// Deletes the given shards right away: the feature flag is disabled
struct TSchemeShard::TTxColumnShardDropCleanupDrain: public TTransactionBase<TSchemeShard> {
    std::vector<TShardIdx> Shards;
    TSideEffects SideEffects;

    TTxColumnShardDropCleanupDrain(TSelf* self, std::vector<TShardIdx>&& shards)
        : TTransactionBase<TSchemeShard>(self)
        , Shards(std::move(shards))
    {}

    TTxType GetTxType() const override {
        return TXTYPE_COLUMN_SHARD_CLEANUP_COMPLETE;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        for (const auto& shardIdx : Shards) {
            if (!IsShardPendingDropCleanup(*Self, shardIdx)) {
                continue;
            }
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "TTxColumnShardDropCleanupDrain: deferred deletion disabled by feature flag"
                             << ", delete shard " << shardIdx << " without waiting for external data cleanup"
                             << ", at schemeshard: " << Self->TabletID());
            SideEffects.DeleteShard(shardIdx);
        }
        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SideEffects.ApplyOnComplete(Self, ctx);
    }
};

ITransaction* TSchemeShard::CreateTxColumnShardDropCleanupState(TEvColumnShard::TEvDropCleanupState::TPtr& ev) {
    return new TTxColumnShardDropCleanupState(this, ev);
}

ITransaction* TSchemeShard::CreateTxColumnShardDropCleanupDrain(std::vector<TShardIdx>&& shards) {
    return new TTxColumnShardDropCleanupDrain(this, std::move(shards));
}

void TSchemeShard::Handle(TEvColumnShard::TEvDropCleanupState::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxColumnShardDropCleanupState(ev), ctx);
}

THashSet<TShardIdx> TSchemeShard::CollectColumnShardsPendingDropCleanup() const {
    THashSet<TShardIdx> result;
    for (const auto& [shardIdx, shardInfo] : ShardInfos) {
        if (shardInfo.TabletType != ETabletType::ColumnShard) {
            continue;
        }
        if (IsShardPendingDropCleanup(*this, shardIdx)) {
            result.insert(shardIdx);
        }
    }
    return result;
}

void TSchemeShard::SchedulePollColumnShardDropCleanup(const TActorContext& ctx, const TDuration delay) {
    if (ColumnShardDropCleanupPollScheduled) {
        return;
    }
    ColumnShardDropCleanupPollScheduled = true;
    ctx.Schedule(delay, new TEvPrivate::TEvPollColumnShardDropCleanup());
}

void TSchemeShard::Handle(TEvPrivate::TEvPollColumnShardDropCleanup::TPtr&, const TActorContext& ctx) {
    ColumnShardDropCleanupPollScheduled = false;

    const auto pending = CollectColumnShardsPendingDropCleanup();
    TabletCounters->Simple()[COUNTER_COLUMN_SHARDS_PENDING_DROP_CLEANUP].Set(pending.size());
    if (pending.empty()) {
        ColumnShardDropCleanupSilentPolls.clear();
        TabletCounters->Simple()[COUNTER_COLUMN_SHARDS_CLEANUP_SILENT].Set(0);
        return;
    }

    if (!AppData()->FeatureFlags.GetEnableDeferredColumnShardDeletionOnDrop()) {
        Execute(CreateTxColumnShardDropCleanupDrain(std::vector<TShardIdx>(pending.begin(), pending.end())), ctx);
        SchedulePollColumnShardDropCleanup(ctx);
        return;
    }

    ui32 silent = 0;
    THashSet<TTabletId> pendingTablets;
    for (const auto& shardIdx : pending) {
        const auto tabletId = ShardInfos.at(shardIdx).TabletID;
        pendingTablets.insert(tabletId);
        const auto polls = ++ColumnShardDropCleanupSilentPolls[tabletId];
        if (polls >= 3) {
            // observational only: acting on silence would silently leak data
            ++silent;
        }
        PipeClientCache->Send(ctx, ui64(tabletId), new TEvColumnShard::TEvCheckDropCleanup(ui64(tabletId)));
    }
    for (auto it = ColumnShardDropCleanupSilentPolls.begin(); it != ColumnShardDropCleanupSilentPolls.end();) {
        if (!pendingTablets.contains(it->first)) {
            ColumnShardDropCleanupSilentPolls.erase(it++);
        } else {
            ++it;
        }
    }
    TabletCounters->Simple()[COUNTER_COLUMN_SHARDS_CLEANUP_SILENT].Set(silent);
    if (silent) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Column shards pending drop cleanup did not answer " << silent
                       << " tablets (older binary or unhealthy); waiting indefinitely"
                       << ", at schemeshard: " << TabletID());
    }

    SchedulePollColumnShardDropCleanup(ctx);
}

} // namespace NSchemeShard
} // namespace NKikimr
