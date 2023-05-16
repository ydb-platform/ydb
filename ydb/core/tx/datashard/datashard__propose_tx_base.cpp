#include "datashard_txs.h"
#include "datashard_failpoints.h"
#include "operation.h"
#include "probes.h"

#include <ydb/core/util/pb.h>
#include <ydb/core/base/wilson.h>

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr {
namespace NDataShard {

TDataShard::TTxProposeTransactionBase::TTxProposeTransactionBase(TDataShard *self,
                                                                        TEvDataShard::TEvProposeTransaction::TPtr &&ev,
                                                                        TInstant receivedAt, ui64 tieBreakerIndex,
                                                                        bool delayed)
    : TBase(self)
    , Ev(std::move(ev))
    , ReceivedAt(receivedAt)
    , TieBreakerIndex(tieBreakerIndex)
    , Kind(static_cast<EOperationKind>(Ev->Get()->GetTxKind()))
    , TxId(Ev->Get()->GetTxId())
    , Acked(!delayed)
    , ProposeTransactionSpan(TWilsonKqp::ProposeTransaction, std::move(Ev->TraceId), "ProposeTransaction", NWilson::EFlags::AUTO_END)
{
}

bool TDataShard::TTxProposeTransactionBase::Execute(NTabletFlatExecutor::TTransactionContext &txc,
                                                           const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxProposeTransactionBase::Execute at " << Self->TabletID());

    if (!Acked) {
        // Ack event on the first execute (this will schedule the next event if any)
        Self->ProposeQueue.Ack(ctx);
        Acked = true;
    }

    try {
        TOutputOpData::TResultPtr result = nullptr;
        // If tablet is in follower mode then we should sync scheme
        // before we build and check operation.
        if (Self->IsFollower()) {
            NKikimrTxDataShard::TError::EKind status = NKikimrTxDataShard::TError::OK;
            TString errMessage;

            if (!Self->SyncSchemeOnFollower(txc, ctx, status, errMessage))
                return false;

            if (status != NKikimrTxDataShard::TError::OK) {
                auto kind = static_cast<NKikimrTxDataShard::ETransactionKind>(Kind);
                result.Reset(new TEvDataShard::TEvProposeTransactionResult(kind, Self->TabletID(), TxId,
                                                                       NKikimrTxDataShard::TEvProposeTransactionResult::ERROR));
                result->AddError(status, errMessage);
            }
        }

        if (result) {
            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Propose transaction complete txid " << TxId << " at tablet "
                        << Self->TabletID() << " status: " << result->GetStatus());
            TString errors = result->GetError();
            if (errors.Size()) {
                LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                            "Errors while proposing transaction txid " << TxId
                            << " at tablet " << Self->TabletID() << " status: "
                            << result->GetStatus() << " errors: " << errors);
            }

            TActorId target = Op ? Op->GetTarget() : Ev->Get()->GetSource();
            ui64 cookie = Op ? Op->GetCookie() : Ev->Cookie;

            if (ProposeTransactionSpan) {
                ProposeTransactionSpan.EndOk();
            }
            ctx.Send(target, result.Release(), 0, cookie);

            return true;
        }

        if (Ev) {
            Y_VERIFY(!Op);

            if (Self->CheckDataTxRejectAndReply(Ev->Get(), ctx)) {
                Ev = nullptr;
                return true;
            }

            TOperation::TPtr op = Self->Pipeline.BuildOperation(Ev, ReceivedAt, TieBreakerIndex, txc, ctx);

            // Unsuccessful operation parse.
            if (op->IsAborted()) {
                LWTRACK(ProposeTransactionParsed, op->Orbit, false);
                Y_VERIFY(op->Result());

                if (ProposeTransactionSpan) {
                    ProposeTransactionSpan.EndError("Unsuccessful operation parse");
                }
                ctx.Send(op->GetTarget(), op->Result().Release());
                return true;
            }

            LWTRACK(ProposeTransactionParsed, op->Orbit, true);

            op->BuildExecutionPlan(false);
            if (!op->IsExecutionPlanFinished())
                Self->Pipeline.GetExecutionUnit(op->GetCurrentUnit()).AddOperation(op);

            Op = op;
            Ev = nullptr;
            Op->IncrementInProgress();
        }

        Y_VERIFY(Op && Op->IsInProgress() && !Op->GetExecutionPlan().empty());

        auto status = Self->Pipeline.RunExecutionPlan(Op, CompleteList, txc, ctx);

        switch (status) {
            case EExecutionStatus::Restart:
                // Restart even if current CompleteList is not empty
                // It will be extended in subsequent iterations
                return false;

            case EExecutionStatus::Reschedule:
                // Reschedule transaction as soon as possible
                if (!Op->IsExecutionPlanFinished()) {
                    Op->IncrementInProgress();
                    Self->ExecuteProgressTx(Op, ctx);
                    Rescheduled = true;
                }
                Op->DecrementInProgress();
                break;

            case EExecutionStatus::Executed:
            case EExecutionStatus::Continue:
                Op->DecrementInProgress();
                break;

            case EExecutionStatus::WaitComplete:
                WaitComplete = true;
                break;

            default:
                Y_FAIL_S("unexpected execution status " << status << " for operation "
                        << *Op << " " << Op->GetKind() << " at " << Self->TabletID());
        }

        if (WaitComplete || !CompleteList.empty()) {
            // Keep operation active until we run the complete list
            CommitStart = AppData()->TimeProvider->Now();
        } else {
            // Release operation as it's no longer needed
            Op = nullptr;
        }

        // Commit all side effects
        return true;
    } catch (const TNotReadyTabletException &) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "TX [" << 0 << " : " << TxId << "] can't prepare (tablet's not ready) at tablet " << Self->TabletID());
        return false;
    } catch (const TSchemeErrorTabletException &ex) {
        Y_UNUSED(ex);
        Y_FAIL();
    } catch (const TMemoryLimitExceededException &ex) {
        Y_FAIL("there must be no leaked exceptions: TMemoryLimitExceededException");
    } catch (const std::exception &e) {
        Y_FAIL("there must be no leaked exceptions: %s", e.what());
    } catch (...) {
        Y_FAIL("there must be no leaked exceptions");
    }
}

void TDataShard::TTxProposeTransactionBase::Complete(const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                "TTxProposeTransactionBase::Complete at " << Self->TabletID());

    if (ProposeTransactionSpan) {
        ProposeTransactionSpan.End();
    }

    if (Op) {
        Y_VERIFY(!Op->GetExecutionPlan().empty());
        if (!CompleteList.empty()) {
            auto commitTime = AppData()->TimeProvider->Now() - CommitStart;
            Op->SetCommitTime(CompleteList.front(), commitTime);

            if (!Op->IsExecutionPlanFinished()
                && (Op->GetCurrentUnit() != CompleteList.front()))
                Op->SetDelayedCommitTime(commitTime);

            Self->Pipeline.RunCompleteList(Op, CompleteList, ctx);
        }

        if (WaitComplete) {
            Op->DecrementInProgress();

            if (!Op->IsInProgress() && !Op->IsExecutionPlanFinished()) {
                Self->Pipeline.AddCandidateOp(Op);

                if (Self->Pipeline.CanRunAnotherOp()) {
                    Self->PlanQueue.Progress(ctx);
                }
            }
        }
    }

    Self->CheckSplitCanStart(ctx);
    Self->CheckMvccStateChangeCanStart(ctx);
}

}}
