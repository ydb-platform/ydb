#include "tablet_impl.h"

#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxConfigureTracking : public TTxBase {
    using TResult = NKikimrCdcAggregator::TEvConfigureTrackingResult;

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
        Result = MakeHolder<TEvCdcAggregator::TEvConfigureTrackingResult>();
        Result->Record.SetOrigin(Self->TabletID());

        TTrackingConfig config;
        TString error;
        if (!ParseTrackingConfig(record.GetConfig(), config, error)) {
            Result->Record.SetStatus(TResult::BAD_REQUEST);
            Result->Record.SetError(error);
            return true;
        }

        TStreamChanges changes;
        switch (Self->Stream.Configure(config, changes)) {
            case EConfigureOutcome::Success:
                Result->Record.SetStatus(TResult::SUCCESS);
                break;
            case EConfigureOutcome::Conflict:
                Result->Record.SetStatus(TResult::CONFLICT);
                Result->Record.SetError(TStringBuilder() << "Tracking of stream " << Self->Stream.GetConfig().StreamPathId
                    << " is already configured differently");
                break;
        }

        NIceDb::TNiceDb db(txc.DB);
        if (changes.ConfigChanged) {
            db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
                NIceDb::TUpdate<Schema::State::TrackingConfig>(record.GetConfig().SerializeAsString()));
        }
        Self->PersistStreamChanges(db, changes);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT({"tabletId", Self->TabletID()});
        YDB_LOG_DEBUG("Reply",
            {"result", Result->ToString()});

        ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
    }
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvConfigureTracking::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"ev", ev->Get()->ToString()});

    Execute(new TTxConfigureTracking(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
