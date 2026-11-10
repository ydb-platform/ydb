#include "stream_repository_over_local_db.h"
#include "tablet_impl.h"

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInit : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
public:
    explicit TTxInit(TCdcAggregator* self)
        : TTransactionBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TStream stream;
        if (!TStreamRepositoryOverLocalDb(txc.DB).Load(stream)) {
            return false;
        }
        Self->Stream = std::move(stream);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        // No other transaction runs before the tablet becomes active, so the
        // restored stream is entirely committed.
        const auto& stream = Self->Stream;
        Self->CommittedReadyBarrier = stream.GetReadyBarrier();
        Self->CommittedActive = stream.IsConfigured() && stream.GetPhase() == EStreamPhase::Active;
        Self->UpdateStreamCounters();

        YDB_LOG_INFO("Stream restored",
            {"tabletId", Self->TabletID()},
            {"configured", stream.IsConfigured()},
            {"active", Self->CommittedActive},
            {"sources", stream.GetSources().size()},
            {"sourcesWithoutProgress", stream.CountSourcesWithoutProgress()});

        Self->SwitchToWork(ctx);
        // After a restart the last barrier is published to every partition
        // again; partitions acknowledge repeats without a new record.
        Self->PublishCommittedBarrier();
    }
};

void TCdcAggregator::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

} // namespace NKikimr::NCdcAggregator
