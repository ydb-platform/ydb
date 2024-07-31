#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxDeleteQueryResponse : public TTxBase {
    std::unordered_set<TActorId> ReplyToActorIds;

    TTxDeleteQueryResponse(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_DELETE_QUERY_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxDeleteQueryResponse::Execute");

        ReplyToActorIds.swap(Self->ReplyToActorIds);

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishTraversal(db);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxDeleteQueryResponse::Complete");

        for (auto& id : ReplyToActorIds) {
            ctx.Send(id, new TEvStatistics::TEvAnalyzeResponse);
        }
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr&) {
    Execute(new TTxDeleteQueryResponse(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
