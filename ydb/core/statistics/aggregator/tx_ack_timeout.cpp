#include "aggregator_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAckTimeout : public TTxBase {
    explicit TTxAckTimeout(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ACK_TIMEOUT; }

    bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxAckTimeout::Execute",
            {"tabletId", Self->TabletID()});
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxAckTimeout::Complete",
            {"tabletId", Self->TabletID()});

        ctx.Send(Self->SelfId(), new TEvPrivate::TEvRequestDistribution);
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvAckTimeout::TPtr& ev) {
    if (ev->Get()->SeqNo < KeepAliveSeqNo) {
        return;
    }
    // timeout
    Execute(new TTxAckTimeout(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
