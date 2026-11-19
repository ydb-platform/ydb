#include "tablet_impl.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

namespace {

bool IsSameConfig(const TCdcAggregatorStream& stream, const TCdcAggregatorStreamConfig& config) {
    const auto& sources = stream.GetLastHeartbeatsBySourceDataShardId();
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
        : TTxBase("TxConfigureTracking", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CONFIGURE_TRACKING;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG("Execute",
            {"ev", Ev->Get()->ToString()});

        const auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvCdcAggregator::TEvConfigureTrackingResult>();
        Result->Record.SetOrigin(Self->TabletID());

        const auto [config, error] = Self->CdcAggregatorStreamProtoConverter.ParseConfig(record.GetConfig());
        if (!config) {
            Result->Record.SetStatus(TResult::BAD_REQUEST);
            Result->Record.SetError(error);
            return true;
        }

        if (!Self->CdcAggregatorStream) {
            Self->CdcAggregatorStream.emplace(*config);
            NIceDb::TNiceDb db(txc.DB);
            Self->CdcAggregatorStreamRepository.InsertCdcAggregatorStream(db, *Self->CdcAggregatorStream);

            YDB_LOG_INFO("Stream configured",
                {"streamPathId", Self->CdcAggregatorStream->GetTopic().StreamPathId},
                {"sources", Self->CdcAggregatorStream->GetLastHeartbeatsBySourceDataShardId().size()});

            Result->Record.SetStatus(TResult::SUCCESS);
        } else if (IsSameConfig(*Self->CdcAggregatorStream, *config)) {
            Result->Record.SetStatus(TResult::SUCCESS);
        } else {
            Result->Record.SetStatus(TResult::CONFLICT);
            Result->Record.SetError(TStringBuilder() << "Tracking of stream " << Self->CdcAggregatorStream->GetTopic().StreamPathId
                << " is already configured differently");
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG("Complete",
            {"result", Result->ToString()});

        ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
    }
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvConfigureTracking::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxConfigureTracking(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
