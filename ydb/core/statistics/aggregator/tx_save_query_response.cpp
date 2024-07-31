#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSaveQueryResponse : public TTxBase {
    std::unordered_set<TActorId> ReplyToActorIds;

    TTxSaveQueryResponse(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SAVE_QUERY_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Execute");

        ReplyToActorIds.swap(Self->ReplyToActorIds);

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishTraversal(db);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Complete");

        for (auto& id : ReplyToActorIds) {
            ctx.Send(id, new TEvStatistics::TEvAnalyzeResponse);
        }
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
    Execute(new TTxSaveQueryResponse(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
