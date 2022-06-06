#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

class TDataShard::TTxCompactBorrowed : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxCompactBorrowed(TDataShard* self, TEvDataShard::TEvCompactBorrowed::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPACT_BORROWED; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Ev->Get()->Record;

        TPathId pathId(record.GetPathId().GetOwnerId(), record.GetPathId().GetLocalId());
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TEvCompactBorrowed request from " << Ev->Sender
            << " for table " << pathId
            << " at tablet " << Self->TabletID());

        auto response = MakeHolder<TEvDataShard::TEvCompactBorrowedResult>(Self->TabletID(), pathId);

        if (pathId.OwnerId != Self->GetPathOwnerId()) {
            // Ignore unexpected owner
            ctx.Send(Ev->Sender, std::move(response));
            return true;
        }

        auto it = Self->TableInfos.find(pathId.LocalPathId);
        if (it == Self->TableInfos.end()) {
            // Ignore unexpected table (may normally happen with races)
            ctx.Send(Ev->Sender, std::move(response));
            return true;
        }

        const TUserTable& tableInfo = *it->second;

        bool hasBorrowed = txc.DB.HasBorrowed(tableInfo.LocalTid, Self->TabletID());
        if (!hasBorrowed) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TEvCompactBorrowed request from " << Ev->Sender
                << " for table " << pathId
                << " has no borrowed parts"
                << " at tablet " << Self->TabletID());
            ctx.Send(Ev->Sender, std::move(response));
            return true;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "TEvCompactBorrowed request from " << Ev->Sender
            << " for table " << pathId
            << " starting compaction for local table " << tableInfo.LocalTid
            << " at tablet " << Self->TabletID());

        Self->Executor()->CompactBorrowed(tableInfo.LocalTid);
        Self->IncCounter(COUNTER_TX_COMPACT_BORROWED);
        ++tableInfo.Stats.CompactBorrowedCount;

        Self->CompactBorrowedWaiters[tableInfo.LocalTid].emplace_back(Ev->Sender);

        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing needed
    }

private:
    TEvDataShard::TEvCompactBorrowed::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvCompactBorrowed::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCompactBorrowed(this, std::move(ev)), ctx);
}

}}
