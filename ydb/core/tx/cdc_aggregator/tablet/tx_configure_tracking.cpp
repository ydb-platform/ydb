#include "tablet_impl.h"

#include <ydb/core/tx/cdc_aggregator/stream/usecases.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxConfigureTracking : public TTxBase {
    TEvCdcAggregator::TEvConfigureTracking::TPtr Ev;
    THolder<TEvCdcAggregator::TEvConfigureTrackingResult> Result;

public:
    explicit TTxConfigureTracking(TCdcAggregator* self, TEvCdcAggregator::TEvConfigureTracking::TPtr& ev)
        : TTxBase(self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CONFIGURE_TRACKING;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Ev->Get()->Record;

        TStreamChanges changes;
        Result = MakeHolder<TEvCdcAggregator::TEvConfigureTrackingResult>();
        Result->Record = ConfigureTracking(Self->Stream, record, changes);
        Result->Record.SetOrigin(Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        if (changes.ConfigChanged) {
            db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
                NIceDb::TUpdate<Schema::State::TrackingConfig>(record.GetConfig().SerializeAsString()));
        }
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

void TCdcAggregator::Handle(TEvCdcAggregator::TEvConfigureTracking::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"tabletId", TabletID()},
        {"ev", ev->Get()->ToString()});

    Execute(new TTxConfigureTracking(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
