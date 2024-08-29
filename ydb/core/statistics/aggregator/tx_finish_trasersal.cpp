#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxFinishTraversal : public TTxBase {
    TString OperationId;
    TPathId PathId;
    TActorId ReplyToActorId;

    TTxFinishTraversal(TSelf* self)
        : TTxBase(self)
        , OperationId(self->ForceTraversalOperationId)
        , PathId(self->TraversalPathId)
    {
        auto forceTraversal = Self->CurrentForceTraversalOperation();
        if (forceTraversal) {
            ReplyToActorId = forceTraversal->ReplyToActorId;
        }
    }

    TTxType GetTxType() const override { return TXTYPE_FINISH_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Execute");

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishTraversal(db);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete " <<
            Self->LastTraversalWasForceString() << " traversal for path " << PathId);

        if (!ReplyToActorId) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. No ActorId to send reply.");            
            return;
        }

        auto forceTraversalRemained = Self->ForceTraversalOperation(OperationId);       
        
        if (forceTraversalRemained) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. Don't send TEvAnalyzeResponse. " <<
                "There are pending operations, OperationId " << OperationId << " , ActorId=" << ReplyToActorId);
        } else {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. " <<
                "Send TEvAnalyzeResponse, OperationId=" << OperationId << ", ActorId=" << ReplyToActorId);
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetOperationId(OperationId);
            response->Record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
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
