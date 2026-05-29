#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

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
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Execute");

        NIceDb::TNiceDb db(txc.DB);
        // Map TEvAnalyzeResponse status to the persisted terminal state:
        //   SUCCESS   -> nullopt  (natural completion path: mark only the current table done;
        //                           if all tables are done the op flips to STATE_DONE)
        //   CANCELLED -> STATE_CANCELLED (user cancel or deadline)
        //   ERROR     -> STATE_CANCELLED (terminal failure; the proto has no STATE_FAILED, and
        //                                  errors should be distinguishable from successful completion)
        std::optional<Ydb::Table::AnalyzeState::State> forceTerminalState;
        if (Status != NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS) {
            forceTerminalState = Ydb::Table::AnalyzeState::STATE_CANCELLED;
        }
        Self->FinishTraversal(db, forceTerminalState, Issues);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete " <<
            Self->LastTraversalWasForceString() << " traversal for path " << PathId);

        if (!ReplyToActorId) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. No ActorId to send reply.");            
            return;
        }

        // Check whether the operation still has pending (non-terminal) tables.
        // If the operation is now terminal (DONE/CANCELLED) or was deleted, send the response.
        auto forceTraversal = Self->ForceTraversalOperation(OperationId);

        // A terminal state (DONE or CANCELLED) means all work is finished, regardless
        // of individual table statuses.
        const bool isTerminal = !forceTraversal ||
            forceTraversal->State == Ydb::Table::AnalyzeState::STATE_DONE ||
            forceTraversal->State == Ydb::Table::AnalyzeState::STATE_CANCELLED;

        const bool hasPendingTables = !isTerminal &&
            std::any_of(forceTraversal->Tables.begin(), forceTraversal->Tables.end(),
                [](const TForceTraversalTable& t) {
                    return t.Status != TForceTraversalTable::EStatus::TraversalFinished;
                });

        if (hasPendingTables) {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. Don't send TEvAnalyzeResponse. " <<
                "There are pending operations, OperationId " << OperationId.Quote() << " , ActorId=" << ReplyToActorId);
        } else {
            SA_LOG_D("[" << Self->TabletID() << "] TTxFinishTraversal::Complete. " <<
                "Send TEvAnalyzeResponse, OperationId=" << OperationId.Quote() << ", ActorId=" << ReplyToActorId);
            auto response = std::make_unique<TEvStatistics::TEvAnalyzeResponse>();
            response->Record.SetOperationId(OperationId);
            response->Record.SetStatus(Status);
            for (const auto& issue : Issues) {
                NYql::IssueToMessage(issue, response->Record.AddIssues());
            }
            ctx.Send(ReplyToActorId, response.release());
            // Clear ReplyToActorId to prevent double-reply on subsequent traversal ticks
            if (forceTraversal) {
                forceTraversal->ReplyToActorId = TActorId{};
            }
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
