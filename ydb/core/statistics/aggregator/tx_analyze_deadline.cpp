#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeDeadline : public TTxBase {
    TString OperationId;
    TActorId ReplyToActorId;

    TTxAnalyzeDeadline(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_DEADLINE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_TRACE("TTxAnalyzeDeadline::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);
        auto now = ctx.Now();

        for (TForceTraversalOperation& operation : Self->ForceTraversals) {
            if (operation.CreatedAt + Self->AnalyzeDeadline < now) {
                YDB_LOG_ERROR("Delete long analyze operation,",
                    {"tabletId", Self->TabletID()},
                    {"operationId", operation.OperationId});

                OperationId = operation.OperationId;
                ReplyToActorId = operation.ReplyToActorId;
                Self->DeleteForceTraversalOperation(operation.OperationId, db);
                break;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_TRACE("TTxAnalyzeDeadline::Complete",
            {"tabletId", Self->TabletID()});

        if (OperationId) {
            if (ReplyToActorId) {
                YDB_LOG_DEBUG("TTxAnalyzeDeadline::Complete. Send TEvAnalyzeResponse for deleted operation,",
                    {"tabletId", Self->TabletID()},
                    {"operationId", OperationId},
                    {"actorId", ReplyToActorId});
                auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
                response->Record.SetOperationId(OperationId);
                response->Record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
                NYql::IssueToMessage(
                    NYql::TIssue("ANALYZE deadline exceeded"), response->Record.AddIssues());
                ctx.Send(ReplyToActorId, response.release());
            } else {
                YDB_LOG_DEBUG("TTxAnalyzeDeadline::Complete. No ActorId to send reply",
                    {"tabletId", Self->TabletID()},
                    {"operationId", OperationId});
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
