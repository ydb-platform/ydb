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

        Self->ScanTableId.PathId = TPathId();
        Self->PersistScanTableId(db);

        for (auto& [tag, _] : Self->CountMinSketches) {
            db.Table<Schema::Statistics>().Key(tag).Delete();
        }
        Self->CountMinSketches.clear();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Complete");

        ctx.Send(Self->ReplyToActorId, new TEvStatistics::TEvScanTableResponse);
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
    Execute(new TTxSaveQueryResponse(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
