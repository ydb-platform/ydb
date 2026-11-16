#include "tablet_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxWriteHeartbeats : public TTxBase {
    using TResult = NKikimrCdcAggregator::TEvWriteHeartbeatsResult;

    TEvCdcAggregator::TEvWriteHeartbeats::TPtr Ev;
    THolder<TEvCdcAggregator::TEvWriteHeartbeatsResult> Result;

public:
    explicit TTxWriteHeartbeats(TCdcAggregator* self, TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev)
        : TTxBase("TxWriteHeartbeats", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_WRITE_HEARTBEATS;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG("Execute",
            {"ev", Ev->Get()->ToString()});

        Result = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeatsResult>();
        if (!Self->CdcAggregatorStream) {
            Result->Record.SetOrigin(Self->TabletID());
            Result->Record.SetStatus(TResult::NOT_READY);
            Result->Record.SetError("Tracking is not configured");
            return true;
        }

        TCdcAggregatorStreamProtoConverter converter;
        const auto batch = converter.ParseHeartbeatsBatch(Ev->Get()->Record);
        const auto diff = Self->CdcAggregatorStream->WriteHeartbeats(batch.HeartbeatsBySourceDataShardId);
        NIceDb::TNiceDb db(txc.DB);
        Self->CdcAggregatorStreamRepository.ApplyCdcAggregatorStreamHeartbeatsDiff(db, diff);
        Result->Record = converter.SerializeWriteHeartbeatsResult(batch, diff);
        Result->Record.SetOrigin(Self->TabletID());

        if (const auto& currentHeartbeat = diff.CurrentHeartbeat) {
            YDB_LOG_DEBUG("Current heartbeat advanced",
                {"version", currentHeartbeat->Version});
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

void TCdcAggregator::Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWriteHeartbeats(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
