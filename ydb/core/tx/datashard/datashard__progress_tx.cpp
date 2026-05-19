#include "datashard_txs.h"
#include "datashard_failpoints.h"

#include <ydb/core/tablet_flat/flat_exec_seat.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

TDataShard::TTxProgressTransaction::TTxProgressTransaction(TDataShard *self, TOperation::TPtr op, NWilson::TTraceId &&traceId)
    : TBase(self, std::move(traceId))
    , ActiveOp(std::move(op))
{}

bool TDataShard::TTxProgressTransaction::Execute(TTransactionContext &txc, const TActorContext &ctx) {
    YDB_LOG_CTX_DEBUG(ctx, "TTxProgressTransaction::Execute at",
        {"TabletID", Self->TabletID()});

    if (!Self->IsStateActive()) {
        Self->IncCounter(COUNTER_TX_PROGRESS_SHARD_INACTIVE);
        YDB_LOG_CTX_INFO(ctx, "Progress tx at non-ready tablet state",
            {"TabletID", Self->TabletID()},
            {"State", Self->State});
        Y_ENSURE(!ActiveOp, "Unexpected ActiveOp at inactive shard " << Self->TabletID());
        Self->PlanQueue.Reset(ctx);
        return true;
    }

    if (!ActiveOp) {
        const bool expireSnapshotsAllowed = (
                Self->State == TShardState::Ready ||
                Self->State == TShardState::SplitSrcWaitForNoTxInFlight ||
                Self->State == TShardState::SplitSrcMakeSnapshot);

        const bool needFutureCleanup = Self->TxInFly() > 0 || expireSnapshotsAllowed;

        if (needFutureCleanup) {
            Self->PlanCleanup(ctx);
        }

        // Allow another concurrent progress tx
        Self->PlanQueue.Reset(ctx);
        Self->Pipeline.ActivateWaitingTxOps(ctx);

        ActiveOp = Self->Pipeline.GetNextActiveOp(false);

        if (!ActiveOp) {
            Self->IncCounter(COUNTER_TX_PROGRESS_IDLE);
            YDB_LOG_CTX_INFO(ctx, "No tx to execute at TxInFly",
                {"TabletID", Self->TabletID()},
                {"TxInFly", Self->TxInFly()});
            return true;
        }

        Y_ENSURE(!ActiveOp->IsInProgress(),
                    "GetNextActiveOp returned in-progress operation "
                    << ActiveOp->GetKind() << " " << *ActiveOp << " (unit "
                    << ActiveOp->GetCurrentUnit() << ") at " << Self->TabletID());
        ActiveOp->IncrementInProgress();

        if (ActiveOp->OperationSpan) {
            if (!TxSpan) {
                // If Progress Tx for this operation is being executed the first time,
                // it won't have a span, because we choose what operation to run in the transaction itself.
                // We create transaction span and transaction execution spans here instead.
                SetupTxSpan(ActiveOp->GetTraceId());
                txc.StartExecutionSpan();
            }
        }
    }

    Y_ENSURE(ActiveOp && ActiveOp->IsInProgress());
    auto status = Self->Pipeline.RunExecutionPlan(ActiveOp, CompleteList, txc, ctx);

    if (Self->Pipeline.CanRunAnotherOp())
        Self->PlanQueue.Progress(ctx);

    switch (status) {
        case EExecutionStatus::Restart:
            // Restart even if current CompleteList is not empty
            // It will be extended in subsequent iterations
            return false;

        case EExecutionStatus::Reschedule:
            // Reschedule transaction as soon as possible
            if (!ActiveOp->IsExecutionPlanFinished()) {
                ActiveOp->IncrementInProgress();
                Self->ExecuteProgressTx(ActiveOp, ctx);
                Rescheduled = true;
            }
            ActiveOp->DecrementInProgress();
            break;

        case EExecutionStatus::Executed:
        case EExecutionStatus::Continue:
            ActiveOp->DecrementInProgress();
            break;

        case EExecutionStatus::WaitComplete:
            WaitComplete = true;
            break;

        default:
            Y_ENSURE(false, "unexpected execution status " << status << " for operation "
                    << *ActiveOp << " " << ActiveOp->GetKind() << " at " << Self->TabletID());
    }

    if (WaitComplete || !CompleteList.empty()) {
        // Keep operation active until we run the complete list
        CommitStart = AppData()->TimeProvider->Now();
    } else {
        // Release operation as it's no longer needed
        ActiveOp = nullptr;
    }

    // Commit all side effects
    return true;
}

void TDataShard::TTxProgressTransaction::Complete(const TActorContext &ctx) {
    YDB_LOG_CTX_DEBUG(ctx, "TTxProgressTransaction::Complete at",
        {"TabletID", Self->TabletID()});

    if (ActiveOp) {
        Y_ENSURE(!ActiveOp->GetExecutionPlan().empty());
        if (!CompleteList.empty()) {
            auto commitTime = AppData()->TimeProvider->Now() - CommitStart;
            ActiveOp->SetCommitTime(CompleteList.front(), commitTime);

            if (!ActiveOp->IsExecutionPlanFinished()
                && (ActiveOp->GetCurrentUnit() != CompleteList.front()))
                ActiveOp->SetDelayedCommitTime(commitTime);

            Self->Pipeline.RunCompleteList(ActiveOp, CompleteList, ctx);
        }

        if (WaitComplete) {
            ActiveOp->DecrementInProgress();

            if (!ActiveOp->IsInProgress() && !ActiveOp->IsExecutionPlanFinished()) {
                Self->Pipeline.AddCandidateOp(ActiveOp);

                if (Self->Pipeline.CanRunAnotherOp()) {
                    Self->PlanQueue.Progress(ctx);
                }
            }
        }
    }

    Self->CheckSplitCanStart(ctx);
}

}}
