#include "datashard_txs.h"
#include "probes.h"
#include "operation.h"
#include "datashard_write_operation.h"

#include <ydb/library/wilson_ids/wilson.h>

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr::NDataShard {

TDataShard::TTxWrite::TTxWrite(TDataShard* self,
                                    NEvents::TDataEvents::TEvWrite::TPtr ev,
                                    TInstant receivedAt,
                                    ui64 tieBreakerIndex,
                                    bool delayed,
                                    NWilson::TSpan &&datashardTransactionSpan)
    : TBase(self, datashardTransactionSpan.GetTraceId())
    , Ev(std::move(ev))
    , ReceivedAt(receivedAt)
    , TieBreakerIndex(tieBreakerIndex)
    , TxId(Ev->Get()->GetTxId())
    , Acked(!delayed)
    , DatashardTransactionSpan(std::move(datashardTransactionSpan))
{ }

bool TDataShard::TTxWrite::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "TTxWrite:: execute at tablet# " << Self->TabletID());

    if (Ev) {
        auto* request = Ev->Get();
        LWTRACK(WriteExecute, request->GetOrbit());
    }

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

                auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(Self->TabletID(), TxId, NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED, errMessage);

                TActorId target = Op ? Op->GetTarget() : Ev->Sender;
                ui64 cookie = Op ? Op->GetCookie() : Ev->Cookie;

                DatashardTransactionSpan.EndOk();
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

            TOperation::TPtr op = Self->Pipeline.BuildOperation(std::move(Ev), ReceivedAt, TieBreakerIndex, txc, std::move(DatashardTransactionSpan));
            Y_ABORT_UNLESS(!Ev);

            TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

            // Unsuccessful operation parse.
            if (op->IsAborted()) {
                LWTRACK(ProposeTransactionParsed, op->Orbit, false);
                Y_ABORT_UNLESS(writeOp->GetWriteResult());
                op->OperationSpan.EndError("Unsuccessful operation parse");
                ctx.Send(op->GetTarget(), writeOp->ReleaseWriteResult().release());
                return true;
            }
            LWTRACK(ProposeTransactionParsed, op->Orbit, true);

            op->BuildExecutionPlan(false);
            if (!op->IsExecutionPlanFinished())
                Self->Pipeline.GetExecutionUnit(op->GetCurrentUnit()).AddOperation(op);

            Op = op;
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

    if (CheckDataTxRejectAndReply(ev, ctx)) {
        IncCounter(COUNTER_WRITE_REQUEST);
        return;
    }

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

    if (CheckTxNeedWait(ev)) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Handle TEvProposeTransaction delayed at " << TabletID() << " until interesting plan step will come");
        if (Pipeline.AddWaitingTxOp(ev, ctx)) {
            UpdateProposeQueueSize();
            return;
        } else {
            Y_ABORT("Unexpected failure to add a waiting unrejected tx");
        }
    }

    IncCounter(COUNTER_WRITE_REQUEST);

    ProposeTransaction(std::move(ev), ctx);
}

ui64 NEvWrite::TConvertor::GetTxId(const TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        case TEvDataShard::TEvProposeTransaction::EventType:
            return ev->Get<TEvDataShard::TEvProposeTransaction>()->GetTxId();
        case NEvents::TDataEvents::TEvWrite::EventType:
            return ev->Get<NEvents::TDataEvents::TEvWrite>()->GetTxId();
        default:
            Y_FAIL_S("Unexpected event type " << ev->GetTypeRewrite());
    }
}

ui64 NEvWrite::TConvertor::GetProposeFlags(NKikimrDataEvents::TEvWrite::ETxMode txMode) {
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

NKikimrDataEvents::TEvWrite::ETxMode NEvWrite::TConvertor::GetTxMode(ui64 flags) {
    if ((flags & TTxFlags::Immediate) && !(flags & TTxFlags::ForceOnline)) {
        return NKikimrDataEvents::TEvWrite::ETxMode::TEvWrite_ETxMode_MODE_IMMEDIATE;
    }
    else if (flags & TTxFlags::VolatilePrepare) {
        return NKikimrDataEvents::TEvWrite::ETxMode::TEvWrite_ETxMode_MODE_VOLATILE_PREPARE;
    }
    else {
        return NKikimrDataEvents::TEvWrite::ETxMode::TEvWrite_ETxMode_MODE_PREPARE;
    }
}

NKikimrTxDataShard::TEvProposeTransactionResult::EStatus NEvWrite::TConvertor::GetStatus(NKikimrDataEvents::TEvWriteResult::EStatus status) {
    switch (status) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED:
            return NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE;
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED:
            return NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED;
        default:
            return NKikimrTxDataShard::TEvProposeTransactionResult::ERROR;
    }
}

NKikimrDataEvents::TEvWriteResult::EStatus NEvWrite::TConvertor::ConvertErrCode(NKikimrTxDataShard::TError::EKind code) {
    switch (code) {
        case NKikimrTxDataShard::TError_EKind_OK:
            return NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED;
        case NKikimrTxDataShard::TError_EKind_BAD_ARGUMENT:
        case NKikimrTxDataShard::TError_EKind_SCHEME_ERROR:
        case NKikimrTxDataShard::TError_EKind_WRONG_PAYLOAD_TYPE:
            return NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST;
        case NKikimrTxDataShard::TError_EKind_SCHEME_CHANGED:
            return NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED;
        case NKikimrTxDataShard::TError_EKind_OUT_OF_SPACE:
            return NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED;
        case NKikimrTxDataShard::TError_EKind_DISK_SPACE_EXHAUSTED:
            return NKikimrDataEvents::TEvWriteResult::STATUS_DISK_SPACE_EXHAUSTED;
        default:
            return NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR;
    }
}

TOperation::TPtr NEvWrite::TConvertor::MakeOperation(EOperationKind kind, const TBasicOpInfo& info, ui64 tabletId) {
    switch (kind) {
        case EOperationKind::DataTx:
        case EOperationKind::SchemeTx:
        case EOperationKind::Snapshot:
        case EOperationKind::DistributedErase:
        case EOperationKind::CommitWrites:
        case EOperationKind::ReadTable:
            return MakeIntrusive<TActiveTransaction>(info);
        case EOperationKind::WriteTx:
            return MakeIntrusive<TWriteOperation>(info, tabletId);
        case EOperationKind::DirectTx:
        case EOperationKind::ReadTx:
        case EOperationKind::Unknown:
            Y_ABORT("Unsupported");
    }
}
}
