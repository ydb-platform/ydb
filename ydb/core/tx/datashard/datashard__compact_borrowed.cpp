#include "datashard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

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

        const auto pathId = TPathId::FromProto(record.GetPathId());
        YDB_LOG_CTX_INFO(ctx, "TEvCompactBorrowed request from for table at tablet",
            {"Sender", Ev->Sender},
            {"pathId", pathId},
            {"TabletID", Self->TabletID()});

        auto nothingToCompactResult = MakeHolder<TEvDataShard::TEvCompactBorrowedResult>(Self->TabletID(), pathId);

        if (pathId.OwnerId != Self->GetPathOwnerId()) { // ignore unexpected owner
            ctx.Send(Ev->Sender, std::move(nothingToCompactResult));
            return true;
        }
        auto it = Self->TableInfos.find(pathId.LocalPathId);
        if (it == Self->TableInfos.end()) { // ignore unexpected table (may normally happen with races)
            ctx.Send(Ev->Sender, std::move(nothingToCompactResult));
            return true;
        }
        const TUserTable& tableInfo = *it->second;
     
        THashSet<ui32> tablesToCompact;
        if (txc.DB.HasBorrowed(tableInfo.LocalTid, Self->TabletID())) {
            tablesToCompact.insert(tableInfo.LocalTid);
        }
        if (tableInfo.ShadowTid && txc.DB.HasBorrowed(tableInfo.ShadowTid, Self->TabletID())) {
            tablesToCompact.insert(tableInfo.ShadowTid);
        }

        auto waiter = MakeIntrusive<TCompactBorrowedWaiter>(Ev->Sender, pathId.LocalPathId);

        for (auto tableToCompact : tablesToCompact) {
            YDB_LOG_CTX_DEBUG(ctx, "TEvCompactBorrowed request from for table starting compaction for local table at tablet",
                {"Sender", Ev->Sender},
                {"pathId", pathId},
                {"tableToCompact", tableToCompact},
                {"TabletID", Self->TabletID()});

            if (Self->Executor()->CompactBorrowed(tableToCompact)) {
                Self->IncCounter(COUNTER_TX_COMPACT_BORROWED);
                ++tableInfo.Stats.CompactBorrowedCount;

                waiter->CompactingTables.insert(tableToCompact);
                Self->CompactBorrowedWaiters[tableToCompact].push_back(waiter);
            } else {
                YDB_LOG_CTX_DEBUG(ctx, "TEvCompactBorrowed request from for table can not be compacted at tablet",
                    {"Sender", Ev->Sender},
                    {"pathId", pathId},
                    {"TabletID", Self->TabletID()});
            }
        }

        if (waiter->CompactingTables.empty()) { // none has been triggered
            YDB_LOG_CTX_DEBUG(ctx, "TEvCompactBorrowed request from for table has no parts for borrowed compaction at tablet",
                {"Sender", Ev->Sender},
                {"pathId", pathId},
                {"TabletID", Self->TabletID()});
            ctx.Send(Ev->Sender, std::move(nothingToCompactResult));
        }

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
