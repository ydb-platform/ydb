#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxFinishTraversal : public TTxBase {
    std::vector<TAnalyzeResponseToActor> AnalyzeResponseToActors;

    TTxFinishTraversal(TSelf* self)
        : TTxBase(self)
    {
        AnalyzeResponseToActors.swap(self->AnalyzeResponseToActors);
    }

    TTxType GetTxType() const override { return TXTYPE_FINISH_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Execute");

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishTraversal(db);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete");
        
        for (const TAnalyzeResponseToActor& response : AnalyzeResponseToActors) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete " <<
                "Send TEvAnalyzeResponse, Cookie=" << response.Cookie << ", ActorId=" << response.ActorId);
            auto ev = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            ev->Record.SetCookie(response.Cookie);
            ctx.Send(response.ActorId, ev.release());
        }
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvFinishTraversal::TPtr&) {
    Execute(new TTxFinishTraversal(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
