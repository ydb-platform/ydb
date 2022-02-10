#include "datashard_impl.h"

#include <ydb/core/base/tx_processing.h>
#include <ydb/core/tablet/tablet_exception.h>

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

static const TDuration REMOVAL_INTERVAL = TDuration::Seconds(1);
constexpr ui64 MAX_RS_TO_REMOVE_IN_SINGLE_TX = 100000;

class TDataShard::TTxRemoveOldInReadSets : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxRemoveOldInReadSets(TDataShard *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        if (Self->State == TShardState::Offline) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxRemoveOldInReadSets::Execute (skip) at " << Self->TabletID());
            return true;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxRemoveOldInReadSets::Execute at " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        ui64 removed = 0;
        for (auto it = Self->InRSToRemove.begin(); it != Self->InRSToRemove.end(); ) {
            auto cur = it++;
            db.Table<Schema::InReadSets>()
                .Key(cur->TxId, cur->Origin, cur->From, cur->To).Delete();
            Self->InRSToRemove.erase(cur);
            ++removed;

            if (removed == MAX_RS_TO_REMOVE_IN_SINGLE_TX)
                break;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "Removing " << removed << " outdated read sets from " << Self->TabletID());

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        if (Self->State == TShardState::Offline) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxRemoveOldInReadSets::Complete (skip) at " << Self->TabletID());
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxRemoveOldInReadSets::Complete " << Self->InRSToRemove.size()
                    << " outdated read sets remain at " << Self->TabletID());

        if (!Self->InRSToRemove.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Schedule TEvPrivate::TEvRemoveOldInReadSets in " << REMOVAL_INTERVAL);

            auto shardCtx = ctx.MakeFor(Self->SelfId());
            shardCtx.Schedule(REMOVAL_INTERVAL, new TEvPrivate::TEvRemoveOldInReadSets);
        }
    }

    TTxType GetTxType() const override
    {
        return TXTYPE_REMOVE_OLD_IN_READ_SETS;
    }

private:
};

class TDataShard::TTxCheckInReadSets : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxCheckInReadSets(TDataShard *self)
        : TBase(self)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        if (Self->State == TShardState::Offline) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxCheckInReadSets::Execute (skip) at " << Self->TabletID());
            return true;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxCheckInReadSets::Execute at " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        auto rowset = db.Table<Schema::InReadSets>().Range().Select();

        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            ui64 txId = rowset.GetValue<Schema::InReadSets::TxId>();

            if (!Self->TransQueue.Has(txId)) {
                ui64 origin = rowset.GetValue<Schema::InReadSets::Origin>();
                ui64 from = rowset.GetValue<Schema::InReadSets::From>();
                ui64 to = rowset.GetValue<Schema::InReadSets::To>();

                Self->InRSToRemove.insert(TReadSetKey(txId, origin, from, to));

                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                            "Found outdated InReadSet in " << Self->TabletID()
                            << " txid=" << txId
                            << " origin=" << origin
                            << " from=" << from
                            << " to=" << to);
            }

            if (!rowset.Next())
                return false;
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        if (Self->State == TShardState::Offline) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxCheckInReadSets::Complete (skip) at " << Self->TabletID());
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TTxCheckInReadSets::Complete found " << Self->InRSToRemove.size()
                    << " read sets to remove in " << Self->TabletID());

        if (!Self->InRSToRemove.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Schedule TEvPrivate::TEvRemoveOldInReadSets in " << REMOVAL_INTERVAL);

            auto shardCtx = ctx.MakeFor(Self->SelfId());
            shardCtx.Schedule(REMOVAL_INTERVAL, new TEvPrivate::TEvRemoveOldInReadSets);
        }
    }

    TTxType GetTxType() const override
    {
        return TXTYPE_CHECK_IN_READ_SETS;
    }

private:
};

ITransaction *TDataShard::CreateTxCheckInReadSets()
{
    return new TTxCheckInReadSets(this);
}

void TDataShard::Handle(TEvPrivate::TEvRemoveOldInReadSets::TPtr &ev,
                               const TActorContext &ctx)
{
    Y_UNUSED(ev);
    Execute(new TTxRemoveOldInReadSets(this), ctx);
}

} // namespace NDataShard
} // namespace NKikimr
