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
        : TBase(ds, std::move(ev->TraceId))
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

        Y_ABORT_UNLESS(Op && Op->IsInProgress() && !Op->GetExecutionPlan().empty());

        auto status = Self->Pipeline.RunExecutionPlan(Op, CompleteList, txc, ctx);

        switch (status) {
            case EExecutionStatus::Restart:
                return false;

            case EExecutionStatus::Reschedule:
                Y_ABORT("Unexpected Reschedule status while handling a direct operation");

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
static void Reject(TDataShard* self, TEvRequest& ev, const TString& txDesc,
        ERejectReasons rejectReasons, const TString& rejectDescription,
        TSetStatusFunc<TEvResponse> setStatusFunc, const TActorContext& ctx,
        TDataShard::ELogThrottlerType logThrottlerType)
{
    LOG_LOG_S_THROTTLE(self->GetLogThrottler(logThrottlerType), ctx, NActors::NLog::PRI_NOTICE, NKikimrServices::TX_DATASHARD, "Rejecting " << txDesc << " request on datashard"
        << ": tablet# " << self->TabletID()
        << ", error# " << rejectDescription);

    auto response = MakeHolder<TEvResponse>();
    setStatusFunc(response->Record);
    response->Record.SetTabletID(self->TabletID());
    response->Record.SetErrorDescription(rejectDescription);

    std::optional<ui64> overloadSubscribe = ev->Get()->Record.HasOverloadSubscribe() ? ev->Get()->Record.GetOverloadSubscribe() : std::optional<ui64>{};
    self->SetOverloadSubscribed(overloadSubscribe, ev->Recipient, ev->Sender, rejectReasons, response->Record);

    ctx.Send(ev->Sender, std::move(response));
}

template <typename TEvResponse, typename TEvRequest>
static bool MaybeReject(TDataShard* self, TEvRequest& ev, const TActorContext& ctx, const TString& txDesc, bool isWrite, TDataShard::ELogThrottlerType logThrottlerType) {
    NKikimrTxDataShard::TEvProposeTransactionResult::EStatus rejectStatus;
    ERejectReasons rejectReasons = ERejectReasons::None;
    TString rejectDescription;
    if (self->CheckDataTxReject(txDesc, ctx, rejectStatus, rejectReasons, rejectDescription)) {
        Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReasons, rejectDescription, &WrongShardState, ctx, logThrottlerType);
        return true;
    }

    if (self->CheckChangesQueueOverflow()) {
        rejectReasons = ERejectReasons::ChangesQueueOverflow;
        rejectDescription = TStringBuilder() << "Change queue overflow at tablet " << self->TabletID();
        Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReasons, rejectDescription, &Overloaded, ctx, logThrottlerType);
        return true;
    }

    if (isWrite) {
        if (self->IsAnyChannelYellowStop()) {
            self->IncCounter(COUNTER_PREPARE_OUT_OF_SPACE);
            rejectReasons = ERejectReasons::YellowChannels;
            rejectDescription = TStringBuilder() << "Cannot perform writes: out of disk space at tablet " << self->TabletID();
            Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReasons, rejectDescription, &OutOfSpace, ctx, logThrottlerType);
            return true;
        } else if (self->IsSubDomainOutOfSpace()) {
            self->IncCounter(COUNTER_PREPARE_DISK_SPACE_EXHAUSTED);
            rejectReasons = ERejectReasons::DiskSpace;
            rejectDescription = "Cannot perform writes: database is out of disk space";
            Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReasons, rejectDescription, &DiskSpaceExhausted, ctx, logThrottlerType);
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
            ERejectReasons::WrongState, "Can't execute bulk upsert at replicated table", &ReadOnly, ctx, TDataShard::ELogThrottlerType::UploadRows_Reject);
    }
    if (!MaybeReject<TEvDataShard::TEvUploadRowsResponse>(this, ev, ctx, "bulk upsert", true, TDataShard::ELogThrottlerType::UploadRows_Reject)) {
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
            ERejectReasons::WrongState, "Can't execute erase at replicated table", &ExecError, ctx, TDataShard::ELogThrottlerType::EraseRows_Reject);
    }
    if (!MaybeReject<TEvDataShard::TEvEraseRowsResponse>(this, ev, ctx, "erase", false, TDataShard::ELogThrottlerType::EraseRows_Reject)) {
        Executor()->Execute(new TTxEraseRows(this, ev), ctx);
    } else {
        IncCounter(COUNTER_ERASE_ROWS_OVERLOADED);
    }
}

} // NDataShard
} // NKikimr
