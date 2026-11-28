#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeDeadline : public TTxBase {
    std::vector<TTraversalReply> DeadlineExceeded;

    TTxAnalyzeDeadline(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_DEADLINE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_TRACE("TTxAnalyzeDeadline::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);
        Self->ExpireAnalyzeOperations(db, ctx.Now(), DeadlineExceeded);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_TRACE("TTxAnalyzeDeadline::Complete",
            {"tabletId", Self->TabletID()});

        Self->ReplyToExpiredAnalyzeOperations(ctx, DeadlineExceeded);
        if (!DeadlineExceeded.empty()) {
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvScheduleForceTraversal());
        }

        Self->AnalyzeDeadlineCheckScheduled = false;
        Self->ScheduleAnalyzeDeadlineCheck();
    }
};

void TStatisticsAggregator::ExpireAnalyzeOperations(NIceDb::TNiceDb& db, TInstant now,
        std::vector<TTraversalReply>& expired) {
    // Collect ids to avoid mutating ForceTraversals during iteration
    std::vector<TString> toFailDeadline;  // non-terminal, deadline exceeded
    std::vector<TString> toDelete;        // terminal, retention exceeded

    for (const auto& operation : ForceTraversals) {
        if (IsTerminalAnalyzeState(operation.State)) {
            if (operation.EndTime && now - operation.EndTime >= AnalyzeOpHistoryRetention) {
                toDelete.push_back(operation.OperationId);
            }
        } else if (operation.CreatedAt + AnalyzeDeadline < now) {
            toFailDeadline.push_back(operation.OperationId);
        }
    }

    NYql::TIssues deadlineIssues;
    deadlineIssues.AddIssue(NYql::TIssue("ANALYZE deadline exceeded"));

    for (const auto& operationId : toFailDeadline) {
        auto* op = ForceTraversalOperation(operationId);
        if (!op) continue;
        expired.push_back({op->ReplyToActorId, operationId});
        DiscardOperationTraversals(operationId, db);
        MarkForceTraversalOperationFinished(operationId,
            Ydb::Table::AnalyzeState::STATE_FAILED, now, db, deadlineIssues);
    }

    for (const auto& operationId : toDelete) {
        YDB_LOG_DEBUG("TTxAnalyzeDeadline: deleting expired history",
            {"tabletId", TabletID()},
            {"operationId", operationId.Quote()});
        DeleteForceTraversalOperation(operationId, db);
    }
}

void TStatisticsAggregator::ReplyToExpiredAnalyzeOperations(const TActorContext& ctx,
        const std::vector<TTraversalReply>& expired) {
    for (const auto& entry : expired) {
        YDB_LOG_ERROR("TTxAnalyzeDeadline: deadline exceeded",
            {"tabletId", TabletID()},
            {"operationId", entry.OperationId.Quote()});
        if (entry.Recipient) {
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetOperationId(entry.OperationId);
            response->Record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
            NYql::IssueToMessage(
                NYql::TIssue("ANALYZE deadline exceeded"), response->Record.AddIssues());
            ctx.Send(entry.Recipient, response.release());
        }
    }
}

void TStatisticsAggregator::ScheduleAnalyzeDeadlineCheck() {
    // Pending operations can expire while collection is disabled, and terminal history has a retention deadline.
    if (!AnalyzeDeadlineCheckScheduled && !ForceTraversals.empty()) {
        AnalyzeDeadlineCheckScheduled = true;
        Schedule(AnalyzeDeadlinePeriod, new TEvPrivate::TEvAnalyzeDeadline());
    }
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvAnalyzeDeadline::TPtr&) {
    Execute(new TTxAnalyzeDeadline(this),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
