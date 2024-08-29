#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeDeadline : public TTxBase {
    TString OperationId;
    TActorId ReplyToActorId;
    
    TTxAnalyzeDeadline(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_DEADLINE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeDeadline::Execute");

        NIceDb::TNiceDb db(txc.DB);
        auto now = ctx.Now();

        for (TForceTraversalOperation& operation : Self->ForceTraversals) {
            if (operation.CreatedAt + Self->AnalyzeDeadline < now) {
                SA_LOG_E("[" << Self->TabletID() << "] Delete long analyze operation, OperationId=" << operation.OperationId);

                OperationId = operation.OperationId;
                ReplyToActorId = operation.ReplyToActorId;
                Self->DeleteForceTraversalOperation(operation.OperationId, db);
                break;
            }
        }        

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeDeadline::Complete");

        if (OperationId) {
            if (ReplyToActorId) {
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeDeadline::Complete. " <<
                    "Send TEvAnalyzeResponse for deleted operation, OperationId=" << OperationId << ", ActorId=" << ReplyToActorId);
                auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
                response->Record.SetOperationId(OperationId);
                response->Record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
                ctx.Send(ReplyToActorId, response.release());
            } else {
                SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeDeadline::Complete. No ActorId to send reply. OperationId=" << OperationId);
            }
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvAnalyzeDeadline());
        } else {
            ctx.Schedule(AnalyzeDeadlinePeriod, new TEvPrivate::TEvAnalyzeDeadline());
        }
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvAnalyzeDeadline::TPtr&) {
    Execute(new TTxAnalyzeDeadline(this),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
