#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxFinishTraversal : public TTxBase {
    ui64 Cookie;
    TActorId ReplyToActorId;

    TTxFinishTraversal(TSelf* self)
        : TTxBase(self)
        , Cookie(self->TraversalCookie)
        , ReplyToActorId(self->TraversalReplyToActorId)
    {}

    TTxType GetTxType() const override { return TXTYPE_FINISH_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Execute");

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishTraversal(db);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete");
        
        if (ReplyToActorId) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete " <<
                "Send TEvAnalyzeResponse, Cookie=" << Cookie << ", ActorId=" << ReplyToActorId);
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetCookie(Cookie);
            ctx.Send(ReplyToActorId, response.release());
        }
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
    Execute(new TTxFinishTraversal(this), TActivationContext::AsActorContext());
}
void TStatisticsAggregator::Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr&) {
    Execute(new TTxFinishTraversal(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
