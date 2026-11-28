#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxFinishTraversal : public TTxBase {
    const TPathId PathId;
    const ui64 Attempt;
    const NKikimrStat::TEvAnalyzeResponse::EStatus Status;
    NYql::TIssues Issues;
    TTraversalReply Reply;

    TTxFinishTraversal(TSelf* self, const TTraversal& traversal,
            NKikimrStat::TEvAnalyzeResponse::EStatus status, NYql::TIssues issues)
        : TTxBase(self)
        , PathId(traversal.PathId)
        , Attempt(traversal.Attempt)
        , Status(status)
        , Issues(std::move(issues))
    {}

    TTxType GetTxType() const override { return TXTYPE_FINISH_TRAVERSAL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxFinishTraversal::Execute",
            {"tabletId", Self->TabletID()},
            {"pathId", PathId},
            {"attempt", Attempt});

        auto it = Self->ActiveTraversals.find(PathId);
        if (it != Self->ActiveTraversals.end() && it->second.Attempt == Attempt) {
            NIceDb::TNiceDb db(txc.DB);
            Reply = Self->FinishTraversal(db, it->second, Status, Issues);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxFinishTraversal::Complete",
            {"tabletId", Self->TabletID()},
            {"pathId", PathId},
            {"attempt", Attempt});

        if (Reply.Recipient) {
            YDB_LOG_DEBUG("TTxFinishTraversal::Complete. Send TEvAnalyzeResponse.",
                {"tabletId", Self->TabletID()},
                {"operationId", Reply.OperationId.Quote()},
                {"actorId", Reply.Recipient});
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetOperationId(Reply.OperationId);
            response->Record.SetStatus(Status);
            for (const auto& issue : Issues) {
                NYql::IssueToMessage(issue, response->Record.AddIssues());
            }
            ctx.Send(Reply.Recipient, response.release());
        }
        if (Self->EnableColumnStatistics) {
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvScheduleForceTraversal());
        }
    }
};

void TStatisticsAggregator::DispatchFinishTraversalTx(TTraversal& traversal,
        NKikimrStat::TEvAnalyzeResponse::EStatus status, NYql::TIssues issues) {
    if (traversal.Finishing) {
        return;
    }
    traversal.Finishing = true;
    Execute(new TTxFinishTraversal(this, traversal, status, std::move(issues)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
