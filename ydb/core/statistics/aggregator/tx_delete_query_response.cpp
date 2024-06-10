#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxDeleteQueryResponse : public TTxBase {

    TTxDeleteQueryResponse(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_DELETE_QUERY_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxDeleteQueryResponse::Execute");

        if (Self->ReplyToActorId) {
            ctx.Send(Self->ReplyToActorId, new TEvStatistics::TEvScanTableResponse);
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishScan(db);

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxDeleteQueryResponse::Complete");
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr&) {
    Execute(new TTxDeleteQueryResponse(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
