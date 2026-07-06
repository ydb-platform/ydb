#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeDeadline : public TTxBase {
    struct TDeadlineEntry {
        TString OperationId;
        TActorId ReplyToActorId;
    };
    std::vector<TDeadlineEntry> DeadlineExceeded;
    bool ActiveDeadlineExceeded = false;

    TTxAnalyzeDeadline(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_DEADLINE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeDeadline::Execute");

        NIceDb::TNiceDb db(txc.DB);
        const auto now = ctx.Now();

        // Collect ids to avoid mutating ForceTraversals during iteration
        std::vector<TString> toFailDeadline;  // non-terminal queued, deadline exceeded
        std::vector<TString> toDelete;        // terminal, retention exceeded

        for (const auto& operation : Self->ForceTraversals) {
            if (IsTerminalAnalyzeState(operation.State)) {
                if (operation.EndTime && now - operation.EndTime >= Self->AnalyzeOpHistoryRetention) {
                    toDelete.push_back(operation.OperationId);
                }
            } else {
                if (operation.CreatedAt + Self->AnalyzeDeadline < now) {
                    if (Self->ForceTraversalOperationId == operation.OperationId) {
                        ActiveDeadlineExceeded = true;
                    } else {
                        toFailDeadline.push_back(operation.OperationId);
                    }
                }
            }
        }

        NYql::TIssues deadlineIssues;
        deadlineIssues.AddIssue(NYql::TIssue("ANALYZE deadline exceeded"));

        for (const auto& operationId : toFailDeadline) {
            auto* op = Self->ForceTraversalOperation(operationId);
            if (!op) continue;
            DeadlineExceeded.push_back({operationId, op->ReplyToActorId});
            Self->MarkForceTraversalOperationFinished(operationId,
                Ydb::Table::AnalyzeState::STATE_FAILED, now, db, deadlineIssues);
        }

        for (const auto& operationId : toDelete) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeDeadline: deleting expired history for operationId=" << operationId.Quote());
            Self->DeleteForceTraversalOperation(operationId, db);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_T("[" << Self->TabletID() << "] TTxAnalyzeDeadline::Complete");

        for (const auto& entry : DeadlineExceeded) {
            SA_LOG_E("[" << Self->TabletID() << "] TTxAnalyzeDeadline: deadline exceeded, operationId=" << entry.OperationId.Quote());
            if (entry.ReplyToActorId) {
                auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
                response->Record.SetOperationId(entry.OperationId);
                response->Record.SetStatus(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
                NYql::IssueToMessage(
                    NYql::TIssue("ANALYZE deadline exceeded"), response->Record.AddIssues());
                ctx.Send(entry.ReplyToActorId, response.release());
            }
        }

        if (ActiveDeadlineExceeded) {
            SA_LOG_E("[" << Self->TabletID() << "] TTxAnalyzeDeadline: active deadline exceeded, operationId="
                << Self->ForceTraversalOperationId.Quote());
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue("ANALYZE deadline exceeded"));
            Self->DispatchFinishTraversalTx(
                NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, std::move(issues));
        }

        ctx.Schedule(AnalyzeDeadlinePeriod, new TEvPrivate::TEvAnalyzeDeadline());
    }
};

void TStatisticsAggregator::Handle(TEvPrivate::TEvAnalyzeDeadline::TPtr&) {
    Execute(new TTxAnalyzeDeadline(this),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
