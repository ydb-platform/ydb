#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSaveQueryResponse : public TTxBase {

    TTxSaveQueryResponse(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SAVE_QUERY_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Execute");

        NIceDb::TNiceDb db(txc.DB);

        Self->RescheduleScanTable(db);
        Self->ResetScanState(db);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Complete");

        if (Self->ReplyToActorId) {
            ctx.Send(Self->ReplyToActorId, new TEvStatistics::TEvScanTableResponse);
        }

        Self->ScheduleNextScan();
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
    Execute(new TTxSaveQueryResponse(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
