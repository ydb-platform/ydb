#include "tablet_impl.h"

#include <ydb/core/tx/cdc_aggregator/stream/usecases.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxWriteHeartbeats : public TTxBase {
    TEvCdcAggregator::TEvWriteHeartbeats::TPtr Ev;
    THolder<TEvCdcAggregator::TEvWriteHeartbeatsResult> Result;

public:
    explicit TTxWriteHeartbeats(TCdcAggregator* self, TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev)
        : TTxBase(self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_WRITE_HEARTBEATS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TStreamChanges changes;
        Result = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeatsResult>();
        Result->Record = WriteHeartbeats(Self->Stream, Ev->Get()->Record, changes);
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

void TCdcAggregator::Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"tabletId", TabletID()},
        {"ev", ev->Get()->ToString()});

    Execute(new TTxWriteHeartbeats(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
