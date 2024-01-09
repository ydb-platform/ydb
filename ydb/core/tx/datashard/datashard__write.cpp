#include "datashard_txs.h"
#include "probes.h"
#include "operation.h"
#include "datashard_write_operation.h"

#include <ydb/library/wilson_ids/wilson.h>

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr::NDataShard {

TDataShard::TTxWrite::TTxWrite(TDataShard* self, NEvents::TDataEvents::TEvWrite::TPtr ev, TInstant receivedAt, ui64 tieBreakerIndex, bool delayed)
    : TBase(self, std::move(ev->TraceId))
    , Ev(std::move(ev))
    , ReceivedAt(receivedAt)
    , TieBreakerIndex(tieBreakerIndex)
    , TxId(Ev->Get()->GetTxId())
    , Acked(!delayed)
    , ProposeTransactionSpan(TWilsonKqp::ProposeTransaction, TxSpan.GetTraceId(), "ProposeTransaction", NWilson::EFlags::AUTO_END)
{
    ProposeTransactionSpan.Attribute("Shard", std::to_string(self->TabletID()));
}

bool TDataShard::TTxWrite::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxWrite:: execute at tablet# " << Self->TabletID());
    auto* request = Ev->Get();
    const auto& record = request->Record;
    Y_UNUSED(record);

    LWTRACK(WriteExecute, request->GetOrbit());

    if (!Acked) {
        // Ack event on the first execute (this will schedule the next event if any)
        Self->ProposeQueue.Ack(ctx);
        Acked = true;
    }

    try {
        // If tablet is in follower mode then we should sync scheme
        // before we build and check operation.
        if (Self->IsFollower()) {
            NKikimrTxDataShard::TError::EKind status;
            TString errMessage;

            if (!Self->SyncSchemeOnFollower(txc, ctx, status, errMessage))
                return false;

            if (status != NKikimrTxDataShard::TError::OK) {
                LOG_LOG_S_THROTTLE(Self->GetLogThrottler(TDataShard::ELogThrottlerType::TxProposeTransactionBase_Execute), ctx, NActors::NLog::PRI_ERROR, NKikimrServices::TX_DATASHARD, 
                    "TTxWrite:: errors while proposing transaction txid " << TxId << " at tablet " << Self->TabletID() << " status: " << status << " error: " << errMessage);

                auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(Self->TabletID(), TxId, NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, errMessage);

                TActorId target = Op ? Op->GetTarget() : Ev->Sender;
                ui64 cookie = Op ? Op->GetCookie() : Ev->Cookie;

                if (ProposeTransactionSpan) {
                    ProposeTransactionSpan.EndOk();
                }
                ctx.Send(target, result.release(), 0, cookie);

                return true;
            }
        }

        if (Ev) {
            Y_ABORT_UNLESS(!Op);

            if (Self->CheckDataTxRejectAndReply(Ev, ctx)) {
                Ev = nullptr;
                return true;
            }

            TOperation::TPtr op = Self->Pipeline.BuildOperation(Ev, ReceivedAt, TieBreakerIndex, txc, ctx, ProposeTransactionSpan.GetTraceId());

            // Unsuccessful operation parse.
            if (op->IsAborted()) {
                LWTRACK(ProposeTransactionParsed, op->Orbit, false);
                Y_ABORT_UNLESS(op->Result());

                if (ProposeTransactionSpan) {
                    ProposeTransactionSpan.EndError("TTxWrite:: unsuccessful operation parse");
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

        Y_ABORT_UNLESS(Op && Op->IsInProgress() && !Op->GetExecutionPlan().empty());

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
                Y_FAIL_S("unexpected execution status " << status << " for operation " << *Op << " " << Op->GetKind() << " at " << Self->TabletID());
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
    } catch (const TNotReadyTabletException&) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "TX [" << 0 << " : " << TxId << "] can't prepare (tablet's not ready) at tablet " << Self->TabletID());
        return false;
    } catch (const TSchemeErrorTabletException& ex) {
        Y_UNUSED(ex);
        Y_ABORT();
    } catch (const TMemoryLimitExceededException& ex) {
        Y_ABORT("there must be no leaked exceptions: TMemoryLimitExceededException");
    } catch (const std::exception& e) {
        Y_ABORT("there must be no leaked exceptions: %s", e.what());
    } catch (...) {
        Y_ABORT("there must be no leaked exceptions");
    }

    return true;
}

void TDataShard::TTxWrite::Complete(const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxWrite complete: at tablet# " << Self->TabletID());

    if (ProposeTransactionSpan) {
        ProposeTransactionSpan.End();
    }

    if (Op) {
        Y_ABORT_UNLESS(!Op->GetExecutionPlan().empty());
        if (!CompleteList.empty()) {
            auto commitTime = AppData()->TimeProvider->Now() - CommitStart;
            Op->SetCommitTime(CompleteList.front(), commitTime);

            if (!Op->IsExecutionPlanFinished() && (Op->GetCurrentUnit() != CompleteList.front()))
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


void TDataShard::Handle(NEvents::TDataEvents::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Handle TTxWrite: at tablet# " << TabletID());

    auto* msg = ev->Get();
    const auto& record = msg->Record;
    Y_UNUSED(record);

    LWTRACK(WriteRequest, msg->GetOrbit());

    // Check if we need to delay an immediate transaction
    if (MediatorStateWaiting && record.txmode() == NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE)
    {
        // We cannot calculate correct version until we restore mediator state
        LWTRACK(ProposeTransactionWaitMediatorState, msg->GetOrbit());
        MediatorStateWaitingMsgs.emplace_back(ev.Release());
        UpdateProposeQueueSize();
        return;
    }

    if (Pipeline.HasProposeDelayers()) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Handle TEvProposeTransaction delayed at " << TabletID() << " until dependency graph is restored");
        LWTRACK(ProposeTransactionWaitDelayers, msg->GetOrbit());
        DelayedProposeQueue.emplace_back().Reset(ev.Release());
        UpdateProposeQueueSize();
        return;
    }

    if (CheckTxNeedWait()) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Handle TEvProposeTransaction delayed at " << TabletID() << " until interesting plan step will come");
        if (Pipeline.AddWaitingTxOp(ev)) {
            UpdateProposeQueueSize();
            return;
        }
    }

    IncCounter(COUNTER_PREPARE_REQUEST);

    if (CheckDataTxRejectAndReply(ev, ctx)) {
        return;
    }

    ProposeTransaction(std::move(ev), ctx);
}

ui64 EvWrite::Convertor::GetTxId(const TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        case TEvDataShard::TEvProposeTransaction::EventType:
            return ev->Get<TEvDataShard::TEvProposeTransaction>()->GetTxId();
        case NEvents::TDataEvents::TEvWrite::EventType:
            return ev->Get<NEvents::TDataEvents::TEvWrite>()->GetTxId();
        default:
            Y_FAIL_S("Unexpected event type " << ev->GetTypeRewrite());
    }
}

ui64 EvWrite::Convertor::GetProposeFlags(NKikimrDataEvents::TEvWrite::ETxMode txMode) {
    switch (txMode) {
        case NKikimrDataEvents::TEvWrite::MODE_PREPARE:
            return TTxFlags::Default;
        case NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE:
            return TTxFlags::VolatilePrepare;
        case NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE:
            return TTxFlags::Immediate;
        default:
            Y_FAIL_S("Unexpected tx mode " << txMode);
    }
}
}