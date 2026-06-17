#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxFinishTraversal : public TTxBase {
    TString OperationId;
    TPathId PathId;
    TActorId ReplyToActorId;
    NKikimrStat::TEvAnalyzeResponse::EStatus Status;
    NYql::TIssues Issues;

    TTxFinishTraversal(
            TSelf* self,
            NKikimrStat::TEvAnalyzeResponse::EStatus status,
            NYql::TIssues issues = NYql::TIssues())
        : TTxBase(self)
        , OperationId(self->ForceTraversalOperationId)
        , PathId(self->TraversalPathId)
        , Status(status)
        , Issues(std::move(issues))
    {
        auto forceTraversal = Self->CurrentForceTraversalOperation();
        if (forceTraversal) {
            ReplyToActorId = forceTraversal->ReplyToActorId;
        }
    }

    TTxType GetTxType() const override { return TXTYPE_FINISH_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxFinishTraversal::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);
        Self->FinishTraversal(
            db,
            /*finishAllForceTraversalTables=*/Status != NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxFinishTraversal::Complete traversal for path",
            {"tabletId", Self->TabletID()},
            {"lastTraversalWasForce", Self->LastTraversalWasForceString()},
            {"pathId", PathId});

        if (!ReplyToActorId) {
            YDB_LOG_DEBUG("TTxFinishTraversal::Complete. No ActorId to send reply",
                {"tabletId", Self->TabletID()});
            return;
        }

        auto forceTraversalRemained = Self->ForceTraversalOperation(OperationId);

        if (forceTraversalRemained) {
            YDB_LOG_DEBUG("TTxFinishTraversal::Complete. Don't send TEvAnalyzeResponse. There are pending operations, OperationId",
                {"tabletId", Self->TabletID()},
                {"operationId", OperationId},
                {"actorId", ReplyToActorId});
        } else {
            YDB_LOG_DEBUG("TTxFinishTraversal::Complete. Send TEvAnalyzeResponse,",
                {"tabletId", Self->TabletID()},
                {"operationId", OperationId},
                {"actorId", ReplyToActorId});
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetOperationId(OperationId);
            response->Record.SetStatus(Status);
            for (const auto& issue : Issues) {
                NYql::IssueToMessage(issue, response->Record.AddIssues());
            }
            ctx.Send(ReplyToActorId, response.release());
        }
    }
};

void TStatisticsAggregator::DispatchFinishTraversalTx(
        NKikimrStat::TEvAnalyzeResponse::EStatus status,
        NYql::TIssues issues) {
    Execute(
        new TTxFinishTraversal(this, status, std::move(issues)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
