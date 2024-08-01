#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxFinishTraversal : public TTxBase {
    ui64 Cookie;
    std::unordered_set<TActorId> ReplyToActorIds;

    TTxFinishTraversal(TSelf* self)
        : TTxBase(self)
        , Cookie(self->TraversalCookie)
    {
        ReplyToActorIds.swap(self->ReplyToActorIds);
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
        
        for (auto& actorId : ReplyToActorIds) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete " <<
                "Send TEvAnalyzeResponse, Cookie=" << Cookie << ", ActorId=" << actorId);
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetCookie(Cookie);
            ctx.Send(actorId, response.release());
        }
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvFinishTraversal::TPtr&) {
    Execute(new TTxFinishTraversal(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
