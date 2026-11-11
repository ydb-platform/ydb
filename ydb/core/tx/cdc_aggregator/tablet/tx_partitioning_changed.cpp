#include "tablet_impl.h"

#include <ydb/core/tx/cdc_aggregator/stream/usecases.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxPartitioningChanged : public TTxBase {
    TEvCdcAggregator::TEvPartitioningChanged::TPtr Ev;
    THolder<TEvCdcAggregator::TEvPartitioningChangedAck> Result;

public:
    explicit TTxPartitioningChanged(TCdcAggregator* self, TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev)
        : TTxBase(self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PARTITIONING_CHANGED;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TStreamChanges changes;
        Result = MakeHolder<TEvCdcAggregator::TEvPartitioningChangedAck>();
        Result->Record = PartitioningChanged(Self->Stream, Ev->Get()->Record, changes);
        Result->Record.SetOrigin(Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistStreamChanges(db, changes);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("Reply",
            {"tabletId", Self->TabletID()},
            {"result", Result->ToString()});

        ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
    }
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"tabletId", TabletID()},
        {"ev", ev->Get()->ToString()});

    Execute(new TTxPartitioningChanged(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
