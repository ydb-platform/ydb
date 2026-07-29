#include "datashard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

class TDataShard::TTxCompactTable : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvCompactTable::TPtr Ev;

public:
    TTxCompactTable(TDataShard* ds, TEvDataShard::TEvCompactTable::TPtr ev)
        : TBase(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPACT_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = Ev->Get()->Record;

        if (!Self->IsStateActive()) {
            YDB_LOG_WARN_CTX(ctx, "Compaction tx requested at non-ready tablet state",
                {"tabletId", Self->TabletID()},
                {"cookie", Ev->Cookie},
                {"state", Self->State},
                {"senderActorId", Ev->Sender});
            auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
                Self->TabletID(),
                record.GetPathId().GetOwnerId(),
                record.GetPathId().GetLocalId(),
                NKikimrTxDataShard::TEvCompactTableResult::FAILED);
            ctx.Send(Ev->Sender, std::move(response), 0, Ev->Cookie);
            return true;
        }

        const auto pathId = TPathId::FromProto(record.GetPathId());

        if (Self->GetPathOwnerId() != pathId.OwnerId) {
            YDB_LOG_WARN_CTX(ctx, "Compaction requested for table not owned by shard",
                {"tabletId", Self->TabletID()},
                {"cookie", Ev->Cookie},
                {"pathId", pathId},
                {"pathOwnerId", Self->GetPathOwnerId()});
            auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
                Self->TabletID(),
                pathId,
                NKikimrTxDataShard::TEvCompactTableResult::FAILED);
            ctx.Send(Ev->Sender, std::move(response), 0, Ev->Cookie);
            return true;
        }

        const auto& tableId = pathId.LocalPathId;
        auto it = Self->TableInfos.find(tableId);
        if (it == Self->TableInfos.end()) {
            YDB_LOG_WARN_CTX(ctx, "Compaction requested for unknown table",
                {"tabletId", Self->TabletID()},
                {"cookie", Ev->Cookie},
                {"pathId", pathId},
                {"senderActorId", Ev->Sender});
            auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
                Self->TabletID(),
                pathId,
                NKikimrTxDataShard::TEvCompactTableResult::FAILED);
            ctx.Send(Ev->Sender, std::move(response), 0, Ev->Cookie);
            return true;
        }
        const TUserTable& tableInfo = *it->second;
        const auto localTid = tableInfo.LocalTid;

        ++tableInfo.Stats.CompactionRequests;

        bool hasBorrowed = txc.DB.HasBorrowed(tableInfo.LocalTid, Self->TabletID());
        if (hasBorrowed && !record.GetCompactBorrowed()) {
            // normally we should not receive requests to compact in this case
            // but in some rare cases like schemeshard restart we can
            YDB_LOG_DEBUG_CTX(ctx, "Compaction request contains borrowed parts, failed",
                {"tabletId", Self->TabletID()},
                {"cookie", Ev->Cookie},
                {"pathId", pathId},
                {"senderActorId", Ev->Sender});

            Self->IncCounter(COUNTER_TX_COMPACTION_FAILED_BORROWED);

            auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
                Self->TabletID(),
                pathId,
                NKikimrTxDataShard::TEvCompactTableResult::BORROWED);
            ctx.Send(Ev->Sender, std::move(response), 0, Ev->Cookie);
            return true;
        }

        if (Self->Executor()->HasLoanedParts()) {
            // normally we should not receive requests to compact in this case
            // but in some rare cases like schemeshard restart we can
            YDB_LOG_DEBUG_CTX(ctx, "Compaction request contains loaned parts, failed",
                {"tabletId", Self->TabletID()},
                {"cookie", Ev->Cookie},
                {"pathId", pathId},
                {"senderActorId", Ev->Sender});

            Self->IncCounter(COUNTER_TX_COMPACTION_FAILED_LOANED);

            auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
                Self->TabletID(),
                pathId,
                NKikimrTxDataShard::TEvCompactTableResult::LOANED);
            ctx.Send(Ev->Sender, std::move(response), 0, Ev->Cookie);
            return true;
        }

        auto stats = txc.DB.GetCompactionStats(localTid);
        bool isEmpty = stats.PartCount == 0 && stats.MemDataSize == 0;
        bool isSingleParted = stats.PartCount == 1 && stats.MemDataSize == 0;
        bool hasSchemaChanges = Self->Executor()->HasSchemaChanges(tableInfo.LocalTid);
        if (isEmpty || isSingleParted && !hasBorrowed && !hasSchemaChanges && !record.GetCompactSinglePartedShards()) {
            // nothing to compact
            YDB_LOG_DEBUG_CTX(ctx, "Compaction is not needed",
                {"tabletId", Self->TabletID()},
                {"cookie", Ev->Cookie},
                {"pathId", pathId},
                {"senderActorId", Ev->Sender});

            Self->IncCounter(COUNTER_TX_COMPACTION_NOT_NEEDED);

            auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
                Self->TabletID(),
                pathId,
                NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED);
            ctx.Send(Ev->Sender, std::move(response), 0, Ev->Cookie);
            return true;
        }

        auto compactionId = Self->Executor()->CompactTable(tableInfo.LocalTid);
        if (compactionId) {
            YDB_LOG_INFO_CTX(ctx, "Compaction started",
                {"compactionId", compactionId},
                {"cookie", Ev->Cookie},
                {"tabletId", Self->TabletID()},
                {"tableId", tableId},
                {"localTid", localTid},
                {"senderActorId", Ev->Sender},
                {"partsCount", stats.PartCount},
                {"memtableSize", stats.MemDataSize},
                {"memtableWaste", stats.MemDataWaste},
                {"memtableRows", stats.MemRowCount});

            Self->IncCounter(COUNTER_TX_COMPACTION);
            Self->CompactionWaiters[tableInfo.LocalTid].emplace_back(compactionId, Ev->Sender, Ev->Cookie);
            ++tableInfo.Stats.CompactionCount;
        } else {
            // compaction failed, for now we don't care
            Self->IncCounter(COUNTER_TX_COMPACTION_FAILED_START);
            auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
                Self->TabletID(),
                pathId,
                NKikimrTxDataShard::TEvCompactTableResult::FAILED);
            ctx.Send(Ev->Sender, std::move(response), 0, Ev->Cookie);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }
};

class TDataShard::TTxPersistFullCompactionTs : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    ui64 TableId;
    TInstant Ts;

public:
    TTxPersistFullCompactionTs(TDataShard* ds, ui64 tableId, TInstant ts)
        : TBase(ds)
        , TableId(tableId)
        , Ts(ts)
    {}

    // note, that type is the same as in TTxCompactTable
    TTxType GetTxType() const override { return TXTYPE_COMPACT_TABLE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);
        NIceDb::TNiceDb db(txc.DB);
        Self->PersistUserTableFullCompactionTs(db, TableId, Ts.Seconds());
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG_CTX(ctx, "Updated last full compaction timestamp",
            {"tabletId", Self->TabletID()},
            {"tableId", TableId},
            {"compactionTs", Ts});
    }
};

void TDataShard::Handle(TEvDataShard::TEvCompactTable::TPtr& ev, const TActorContext& ctx) {
    Executor()->Execute(new TTxCompactTable(this, ev), ctx);
}

void TDataShard::CompactionComplete(ui32 tableId, const TActorContext &ctx) {
    auto finishedInfo = Executor()->GetFinishedCompactionInfo(tableId);

    YDB_LOG_DEBUG_CTX(ctx, "Compaction completed",
        {"tabletId", TabletID()},
        {"tableId", tableId},
        {"edge", finishedInfo.Edge},
        {"fullCompactionTs", finishedInfo.FullCompactionTs});

    TLocalPathId localPathId = InvalidLocalPathId;
    if (tableId >= Schema::MinLocalTid) {
        for (auto& ti : TableInfos) {
            if (ti.second->LocalTid != tableId && ti.second->ShadowTid != tableId)
                continue;
            if (ti.second->Stats.LastFullCompaction < finishedInfo.FullCompactionTs) {
                IncCounter(COUNTER_FULL_COMPACTION_DONE);
                ti.second->Stats.LastFullCompaction = finishedInfo.FullCompactionTs;
                Executor()->Execute(
                    new TTxPersistFullCompactionTs(
                        this,
                        ti.first,
                        finishedInfo.FullCompactionTs),
                    ctx);
            }

            ti.second->StatsNeedUpdate = true;
            localPathId = ti.first;
            UpdateTableStats(ctx);
            break;
        }
    }

    ReplyCompactionWaiters(tableId, localPathId, finishedInfo, ctx);
}

void TDataShard::ReplyCompactionWaiters(
    ui32 tableId,
    TLocalPathId localPathId,
    const NTabletFlatExecutor::TFinishedCompactionInfo& compactionInfo,
    const TActorContext &ctx)
{
    YDB_LOG_DEBUG_CTX(ctx, "Replying compaction waiters, compaction finished",
        {"tabletId", TabletID()},
        {"tableId", tableId},
        {"edge", compactionInfo.Edge},
        {"frontCompactionId", (CompactionWaiters[tableId].empty() ? 0UL : CompactionWaiters[tableId].front().CompactionId)});

    auto fullCompactionQueue = CompactionWaiters.FindPtr(tableId);
    while (fullCompactionQueue && !fullCompactionQueue->empty()) {
        const auto& waiter = fullCompactionQueue->front();
        if (waiter.CompactionId > compactionInfo.Edge) {
            break;
        }

        auto response = MakeHolder<TEvDataShard::TEvCompactTableResult>(
            TabletID(),
            GetPathOwnerId(),
            localPathId,
            NKikimrTxDataShard::TEvCompactTableResult::OK);
        ctx.Send(waiter.ActorId, std::move(response), 0, waiter.Cookie);

        YDB_LOG_DEBUG_CTX(ctx, "Replying compaction waiters with TEvCompactTableResult",
            {"tabletId", TabletID()},
            {"tableId", tableId},
            {"recipientActorId", waiter.ActorId},
            {"pathId", TPathId(GetPathOwnerId(), localPathId)});

        fullCompactionQueue->pop_front();
    }

    auto compactBorrowedQueue = CompactBorrowedWaiters.FindPtr(tableId);
    if (compactBorrowedQueue && !compactBorrowedQueue->empty()) {
        const bool hasBorrowed = Executor()->HasBorrowed(tableId, TabletID());
        if (!hasBorrowed) {
            while (!compactBorrowedQueue->empty()) {
                const auto& waiter = compactBorrowedQueue->front();
                waiter->CompactingTables.erase(tableId);

                if (waiter->CompactingTables.empty()) { // all requested tables have been compacted
                    auto response = MakeHolder<TEvDataShard::TEvCompactBorrowedResult>(
                        TabletID(),
                        GetPathOwnerId(),
                        waiter->RequestedTable);
                    ctx.Send(waiter->ActorId, std::move(response));

                    YDB_LOG_DEBUG_CTX(ctx, "Replying compaction waiters with TEvCompactBorrowedResult",
                        {"tabletId", TabletID()},
                        {"tableId", tableId},
                        {"recipientActorId", waiter->ActorId},
                        {"pathId", TPathId(GetPathOwnerId(), waiter->RequestedTable)});
                }

                compactBorrowedQueue->pop_front();
            }
        }
    }
}

void TDataShard::Handle(TEvDataShard::TEvGetCompactTableStats::TPtr& ev, const TActorContext& ctx) {
    auto &record = ev->Get()->Record;
    auto response = MakeHolder<TEvDataShard::TEvGetCompactTableStatsResult>();

    const auto pathId = TPathId::FromProto(record.GetPathId());

    const auto& tableId = pathId.LocalPathId;
    auto it = TableInfos.find(tableId);
    if (it != TableInfos.end()) {
        const TUserTable& tableInfo = *it->second;
        response->Record.SetCompactionRequests(tableInfo.Stats.CompactionRequests);
        response->Record.SetCompactionCount(tableInfo.Stats.CompactionCount);
        response->Record.SetCompactBorrowedCount(tableInfo.Stats.CompactBorrowedCount);
    }

    ctx.Send(ev->Sender, std::move(response));
}

} // NDataShard
} // NKikimr
