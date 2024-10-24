#include "aggregator_impl.h"

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAckTimeout : public TTxBase {
    explicit TTxAckTimeout(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ACK_TIMEOUT; }

    bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAckTimeout::Execute");
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAckTimeout::Complete");

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
