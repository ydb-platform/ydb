#include "datashard_impl.h"
#include "datashard_direct_transaction.h"

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

template <typename TEvRequest>
class TTxDirectBase : public TTransactionBase<TDataShard> {
    TEvRequest Ev;

    TOperation::TPtr Op;
    TVector<EExecutionUnitKind> CompleteList;
    bool WaitComplete = false;

public:
    TTxDirectBase(TDataShard* ds, TEvRequest ev)
        : TBase(ds)
        , Ev(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxDirectBase(" << GetTxType() << ") Execute"
            << ": at tablet# " << Self->TabletID());

        if (Self->IsFollower()) {
            return true; // TODO: report error
        }

        if (Ev) {
            const ui64 tieBreaker = Self->NextTieBreakerIndex++;
            Op = new TDirectTransaction(ctx.Now(), tieBreaker, Ev);
            Op->BuildExecutionPlan(false);
            Self->Pipeline.GetExecutionUnit(Op->GetCurrentUnit()).AddOperation(Op);

            Ev = nullptr;
            Op->IncrementInProgress();
        }

        Y_VERIFY(Op && Op->IsInProgress() && !Op->GetExecutionPlan().empty());

        auto status = Self->Pipeline.RunExecutionPlan(Op, CompleteList, txc, ctx);

        switch (status) {
            case EExecutionStatus::Restart:
                return false;

            case EExecutionStatus::Reschedule:
                Y_FAIL("Unexpected Reschedule status while handling a direct operation");

            case EExecutionStatus::Executed:
            case EExecutionStatus::Continue:
                Op->DecrementInProgress();
                break;

            case EExecutionStatus::WaitComplete:
                WaitComplete = true;
                break;

            case EExecutionStatus::ExecutedNoMoreRestarts:
            case EExecutionStatus::DelayComplete:
            case EExecutionStatus::DelayCompleteNoMoreRestarts:
                Y_FAIL_S("unexpected execution status " << status << " for operation "
                        << *Op << " " << Op->GetKind() << " at " << Self->TabletID());
        }

        if (WaitComplete || !CompleteList.empty()) {
            // Keep current operation
        } else {
            Op = nullptr;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "TTxDirectBase(" << GetTxType() << ") Complete"
            << ": at tablet# " << Self->TabletID());

        if (Op) {
            if (!CompleteList.empty()) {
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
    }

}; // TTxDirectBase

class TDataShard::TTxUploadRows : public TTxDirectBase<TEvDataShard::TEvUploadRowsRequest::TPtr> {
public:
    using TTxDirectBase::TTxDirectBase;
    TTxType GetTxType() const override { return TXTYPE_UPLOAD_ROWS; }
};

class TDataShard::TTxEraseRows : public TTxDirectBase<TEvDataShard::TEvEraseRowsRequest::TPtr> {
public:
    using TTxDirectBase::TTxDirectBase;
    TTxType GetTxType() const override { return TXTYPE_ERASE_ROWS; }
};

static void OutOfSpace(NKikimrTxDataShard::TEvUploadRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TError::OUT_OF_SPACE);
}

static void DiskSpaceExhausted(NKikimrTxDataShard::TEvUploadRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TError::DISK_SPACE_EXHAUSTED);
}

static void WrongShardState(NKikimrTxDataShard::TEvUploadRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TError::WRONG_SHARD_STATE);
}

static void ReadOnly(NKikimrTxDataShard::TEvUploadRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TError::READONLY);
}

static void Overloaded(NKikimrTxDataShard::TEvUploadRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED);
}

static void OutOfSpace(NKikimrTxDataShard::TEvEraseRowsResponse& response) {
    // NOTE: this function is never called, because erase is allowed when out of space
    response.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE);
}

static void DiskSpaceExhausted(NKikimrTxDataShard::TEvEraseRowsResponse& response) {
    // NOTE: this function is never called, because erase is allowed when out of space
    response.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE);
}

static void WrongShardState(NKikimrTxDataShard::TEvEraseRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE);
}

static void ExecError(NKikimrTxDataShard::TEvEraseRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::EXEC_ERROR);
}

static void Overloaded(NKikimrTxDataShard::TEvEraseRowsResponse& response) {
    response.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_OVERLOADED);
}

template <typename TEvResponse>
using TSetStatusFunc = void(*)(typename TEvResponse::ProtoRecordType&);

template <typename TEvResponse, typename TEvRequest>
static void Reject(TDataShard* self, TEvRequest& ev, const TString& txDesc, const TString& reason,
        TSetStatusFunc<TEvResponse> setStatusFunc, const TActorContext& ctx)
{
    LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, "Rejecting " << txDesc << " request on datashard"
        << ": tablet# " << self->TabletID()
        << ", error# " << reason);

    auto response = MakeHolder<TEvResponse>();
    setStatusFunc(response->Record);
    response->Record.SetTabletID(self->TabletID());
    response->Record.SetErrorDescription(reason);
    ctx.Send(ev->Sender, std::move(response));
}

template <typename TEvResponse, typename TEvRequest>
static bool MaybeReject(TDataShard* self, TEvRequest& ev, const TActorContext& ctx, const TString& txDesc, bool isWrite) {
    NKikimrTxDataShard::TEvProposeTransactionResult::EStatus rejectStatus;
    TString rejectReason;
    if (self->CheckDataTxReject(txDesc, ctx, rejectStatus, rejectReason)) {
        Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReason, &WrongShardState, ctx);
        return true;
    }

    if (self->CheckChangesQueueOverflow()) {
        rejectReason = TStringBuilder() << "Change queue overflow at tablet " << self->TabletID();
        Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReason, &Overloaded, ctx);
        return true;
    }

    if (isWrite) {
        if (self->IsAnyChannelYellowStop()) {
            self->IncCounter(COUNTER_PREPARE_OUT_OF_SPACE);
            rejectReason = TStringBuilder() << "Cannot perform writes: out of disk space at tablet " << self->TabletID();
            Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReason, &OutOfSpace, ctx);
            return true;
        } else if (self->IsSubDomainOutOfSpace()) {
            self->IncCounter(COUNTER_PREPARE_OUT_OF_SPACE);
            rejectReason = "Cannot perform writes: database is out of disk space";
            Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReason, &DiskSpaceExhausted, ctx);
            return true;
        }
    }

    return false;
}

void TDataShard::Handle(TEvDataShard::TEvUploadRowsRequest::TPtr& ev, const TActorContext& ctx) {
    if (MediatorStateWaiting) {
        MediatorStateWaitingMsgs.emplace_back(ev.Release());
        UpdateProposeQueueSize();
        return;
    }
    if (IsReplicated()) {
        return Reject<TEvDataShard::TEvUploadRowsResponse>(this, ev, "bulk upsert",
            "Can't execute bulk upsert at replicated table", &ReadOnly, ctx);
    }
    if (!MaybeReject<TEvDataShard::TEvUploadRowsResponse>(this, ev, ctx, "bulk upsert", true)) {
        Executor()->Execute(new TTxUploadRows(this, ev), ctx);
    } else {
        IncCounter(COUNTER_BULK_UPSERT_OVERLOADED);
    }
}

void TDataShard::Handle(TEvDataShard::TEvEraseRowsRequest::TPtr& ev, const TActorContext& ctx) {
    if (MediatorStateWaiting) {
        MediatorStateWaitingMsgs.emplace_back(ev.Release());
        UpdateProposeQueueSize();
        return;
    }
    if (IsReplicated()) {
        return Reject<TEvDataShard::TEvEraseRowsResponse>(this, ev, "erase",
            "Can't execute erase at replicated table", &ExecError, ctx);
    }
    if (!MaybeReject<TEvDataShard::TEvEraseRowsResponse>(this, ev, ctx, "erase", false)) {
        Executor()->Execute(new TTxEraseRows(this, ev), ctx);
    } else {
        IncCounter(COUNTER_ERASE_ROWS_OVERLOADED);
    }
}

} // NDataShard
} // NKikimr
