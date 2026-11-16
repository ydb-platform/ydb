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
        Result->Record.SetOrigin(Self->TabletID());

        if (!Self->CdcAggregatorStream) {
            Result->Record.SetStatus(TResult::NOT_READY);
            Result->Record.SetError("Tracking is not configured");
            return true;
        }

        THashMap<ui64, NPQ::THeartbeat> heartbeatBySourceDataShardId;
        for (const auto& proto : Ev->Get()->Record.GetHeartbeats()) {
            auto [heartbeat, error] = TCdcAggregatorStreamProtoConverter().ParseHeartbeat(proto);
            if (!heartbeat) {
                YDB_LOG_WARN("Invalid heartbeat",
                    {"error", error});
                auto& result = *Result->Record.AddResults();
                result.SetSourceDataShardId(proto.GetSourceDataShardId());
                result.SetStatus(TResult::THeartbeatResult::BAD_REQUEST);
                continue;
            }
            heartbeatBySourceDataShardId[proto.GetSourceDataShardId()] = std::move(*heartbeat);
        }

        const auto diff = Self->CdcAggregatorStream->WriteHeartbeats(heartbeatBySourceDataShardId);
        NIceDb::TNiceDb db(txc.DB);
        Self->CdcAggregatorStreamRepository.ApplyCdcAggregatorStreamHeartbeatsDiff(db, diff);

        for (const auto& [sourceDataShardId, _] : heartbeatBySourceDataShardId) {
            auto& result = *Result->Record.AddResults();
            result.SetSourceDataShardId(sourceDataShardId);
            if (const auto* lastHeartbeat = diff.LastHeartbeatsBySourceDataShardId.FindPtr(sourceDataShardId)) {
                result.SetStatus(TResult::THeartbeatResult::SUCCESS);
                lastHeartbeat->Version.ToProto(result.MutableWrittenVersion());
            } else {
                result.SetStatus(TResult::THeartbeatResult::UNKNOWN_SOURCE);
            }
        }

        if (const auto& currentHeartbeat = diff.CurrentHeartbeat) {
            YDB_LOG_DEBUG("Current heartbeat advanced",
                {"version", currentHeartbeat->Version});
        }

        Result->Record.SetStatus(TResult::SUCCESS);
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
