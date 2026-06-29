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
        //   CANCELLED -> STATE_CANCELLED (user cancel)
        //   ERROR     -> STATE_FAILED    (terminal failure: scan error, deadline, etc.)
        std::optional<Ydb::Table::AnalyzeState::State> forceTerminalState;
        switch (Status) {
            case NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS:
                break;
            case NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED:
                forceTerminalState = Ydb::Table::AnalyzeState::STATE_CANCELLED;
                break;
            default:
                forceTerminalState = Ydb::Table::AnalyzeState::STATE_FAILED;
                break;
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
        // If the operation is now terminal (or was deleted), send the response.
        auto forceTraversal = Self->ForceTraversalOperation(OperationId);
        const bool isTerminal = !forceTraversal || IsTerminalAnalyzeState(forceTraversal->State);

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
