#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSaveQueryResponse : public TTxBase {

    TTxSaveQueryResponse(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SAVE_QUERY_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Execute");

        if (Self->ReplyToActorId) {
            ctx.Send(Self->ReplyToActorId, new TEvStatistics::TEvScanTableResponse);
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishScan(db);

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Complete");
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
    Execute(new TTxSaveQueryResponse(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
