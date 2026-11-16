#include "tablet_impl.h"

#include <util/string/builder.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

namespace {

bool IsSameConfig(const TCdcAggregatorStream& stream, const TCdcAggregatorStreamConfig& config) {
    const auto& sources = stream.GetSourceDataShardIds();
    return stream.GetTopic() == config.Topic
        && sources.size() == config.SourceDataShardIds.size()
        && std::all_of(config.SourceDataShardIds.begin(), config.SourceDataShardIds.end(), [&sources](ui64 sourceDataShardId) {
            return sources.contains(sourceDataShardId);
        });
}

} // namespace

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

        TCdcAggregatorStreamConfig config;
        TString error;
        if (!ParseCdcAggregatorStreamConfig(record.GetConfig(), config, error)) {
            Result->Record.SetStatus(TResult::BAD_REQUEST);
            Result->Record.SetError(error);
            return true;
        }

        if (!Self->CdcAggregatorStream) {
            Self->CdcAggregatorStream.emplace(config);
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistCdcAggregatorStream(db);
            Result->Record.SetStatus(TResult::SUCCESS);
        } else if (IsSameConfig(*Self->CdcAggregatorStream, config)) {
            Result->Record.SetStatus(TResult::SUCCESS);
        } else {
            Result->Record.SetStatus(TResult::CONFLICT);
            Result->Record.SetError(TStringBuilder() << "Tracking of stream " << Self->CdcAggregatorStream->GetTopic().StreamPathId
                << " is already configured differently");
        }

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
