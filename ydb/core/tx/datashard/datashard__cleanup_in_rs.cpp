#include "datashard_impl.h"

#include <ydb/core/base/tx_processing.h>
#include <ydb/core/tablet/tablet_exception.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

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
            YDB_LOG_DEBUG_CTX(ctx, "TTxRemoveOldInReadSets::Execute (skip)",
                {"tabletId", Self->TabletID()});
            return true;
        }

        YDB_LOG_DEBUG_CTX(ctx, "TTxRemoveOldInReadSets::Execute",
            {"tabletId", Self->TabletID()});

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

        YDB_LOG_DEBUG_CTX(ctx, "Removing outdated read sets",
            {"removed", removed},
            {"tabletId", Self->TabletID()});

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        if (Self->State == TShardState::Offline) {
            YDB_LOG_DEBUG_CTX(ctx, "TTxRemoveOldInReadSets::Complete (skip)",
                {"tabletId", Self->TabletID()});
            return;
        }

        YDB_LOG_DEBUG_CTX(ctx, "TTxRemoveOldInReadSets::Complete, outdated read sets remain",
            {"inRsToRemoveCount", Self->InRSToRemove.size()},
            {"tabletId", Self->TabletID()});

        if (!Self->InRSToRemove.empty()) {
            YDB_LOG_DEBUG_CTX(ctx, "Schedule TEvPrivate::TEvRemoveOldInReadSets",
                {"removalInterval", REMOVAL_INTERVAL});

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
            YDB_LOG_DEBUG_CTX(ctx, "TTxCheckInReadSets::Execute (skip)",
                {"tabletId", Self->TabletID()});
            return true;
        }

        YDB_LOG_DEBUG_CTX(ctx, "TTxCheckInReadSets::Execute",
            {"tabletId", Self->TabletID()});

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

                YDB_LOG_TRACE_CTX(ctx, "Found outdated InReadSet",
                    {"tabletId", Self->TabletID()},
                    {"txId", txId},
                    {"origin", origin},
                    {"sourceTabletId", from},
                    {"targetTabletId", to});
            }

            if (!rowset.Next())
                return false;
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        if (Self->State == TShardState::Offline) {
            YDB_LOG_DEBUG_CTX(ctx, "TTxCheckInReadSets::Complete (skip)",
                {"tabletId", Self->TabletID()});
            return;
        }

        YDB_LOG_DEBUG_CTX(ctx, "TTxCheckInReadSets::Complete, found read sets to remove",
            {"inRsToRemoveCount", Self->InRSToRemove.size()},
            {"tabletId", Self->TabletID()});

        if (!Self->InRSToRemove.empty()) {
            YDB_LOG_DEBUG_CTX(ctx, "Schedule TEvPrivate::TEvRemoveOldInReadSets",
                {"removalInterval", REMOVAL_INTERVAL});

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
