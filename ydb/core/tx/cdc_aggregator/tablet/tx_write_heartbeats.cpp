#include "tablet_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

namespace {

THolder<TEvCdcAggregator::TEvWriteHeartbeatsResult> MakeNotReadyResult(ui64 tabletId) {
    auto result = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeatsResult>();
    result->Record.SetOrigin(tabletId);
    result->Record.SetStatus(NKikimrCdcAggregator::TEvWriteHeartbeatsResult::NOT_READY);
    result->Record.SetError("Tracking is not configured");
    return result;
}

THolder<TEvCdcAggregator::TEvWriteHeartbeatsResult> MakeSuccessResult(ui64 tabletId, NKikimrCdcAggregator::TEvWriteHeartbeatsResult record) {
    auto result = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeatsResult>();
    result->Record = std::move(record);
    result->Record.SetOrigin(tabletId);
    return result;
}

} // namespace

class TCdcAggregator::TTxWriteHeartbeats : public TTxBase {
    TEvCdcAggregator::TEvWriteHeartbeats::TPtr Ev;
    THolder<TEvCdcAggregator::TEvWriteHeartbeatsResult> Result;
    std::optional<NPQ::THeartbeat> HeartbeatToBroadcast;

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

        if (!Self->CdcAggregatorStream) {
            Result = MakeNotReadyResult(Self->TabletID());
            return true;
        }

        const auto batch = Self->CdcAggregatorStreamProtoConverter.ParseHeartbeatsBatch(Ev->Get()->Record);
        const auto diff = Self->CdcAggregatorStream->WriteHeartbeats(batch.HeartbeatsBySourceDataShardId);
        NIceDb::TNiceDb db(txc.DB);
        Self->CdcAggregatorStreamRepository.ApplyCdcAggregatorStreamHeartbeatsDiff(db, diff);
        Result = MakeSuccessResult(Self->TabletID(), Self->CdcAggregatorStreamProtoConverter.SerializeWriteHeartbeatsResult(batch, diff));
        HeartbeatToBroadcast = diff.UpdatedCurrentHeartbeat;

        if (const auto& currentHeartbeat = diff.UpdatedCurrentHeartbeat) {
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
        if (HeartbeatToBroadcast) {
            Self->BroadcastHeartbeat(*HeartbeatToBroadcast, ctx);
        }
    }
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWriteHeartbeats(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
