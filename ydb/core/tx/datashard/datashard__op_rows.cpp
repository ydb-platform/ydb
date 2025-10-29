#include "datashard_impl.h"
#include "datashard_direct_transaction.h"
#include "datashard_txs.h"

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

        Y_ENSURE(Op && Op->IsInProgress() && !Op->GetExecutionPlan().empty());

        auto status = Self->Pipeline.RunExecutionPlan(Op, CompleteList, txc, ctx);

        switch (status) {
            case EExecutionStatus::Restart:
                return false;

            case EExecutionStatus::Reschedule:
                Y_ENSURE(false, "Unexpected Reschedule status while handling a direct operation");

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
                Y_ENSURE(false, "unexpected execution status " << status << " for operation "
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

static void OutOfSpace(TEvDataShard::TEvUploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::OUT_OF_SPACE);
}

static void DiskSpaceExhausted(TEvDataShard::TEvUploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::DISK_SPACE_EXHAUSTED);
}

static void WrongShardState(TEvDataShard::TEvUploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::WRONG_SHARD_STATE);
}

static void Replicated(TEvDataShard::TEvUploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::READONLY);
}

static void Overloaded(TEvDataShard::TEvUploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED);
}

ECumulativeCounters OverloadedCounter(TEvDataShard::TEvUploadRowsRequest::TPtr&) {
    return COUNTER_BULK_UPSERT_OVERLOADED;
}

static bool TryDelayS3UploadRows(TDataShard*, TEvDataShard::TEvUploadRowsRequest::TPtr&, ERejectReasons) {
    // not a S3 upload rows
    return false;
}

static void OutOfSpace(TEvDataShard::TEvS3UploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::OUT_OF_SPACE);
}

static void DiskSpaceExhausted(TEvDataShard::TEvS3UploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::DISK_SPACE_EXHAUSTED);
}

static void WrongShardState(TEvDataShard::TEvS3UploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::WRONG_SHARD_STATE);
}

static void Replicated(TEvDataShard::TEvS3UploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::READONLY);
}

static void Overloaded(TEvDataShard::TEvS3UploadRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED);
}

ECumulativeCounters OverloadedCounter(TEvDataShard::TEvS3UploadRowsRequest::TPtr&) {
    return COUNTER_BULK_UPSERT_OVERLOADED;
}

static bool TryDelayS3UploadRows(TDataShard* self, TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev, ERejectReasons rejectReasons) {
    if (rejectReasons == ERejectReasons::OverloadByProbability) {
        self->DelayS3UploadRows(ev);
        return true;
    }
    return false;
}

static void WrongShardState(TEvDataShard::TEvEraseRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE);
}

static void Replicated(TEvDataShard::TEvEraseRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::EXEC_ERROR);
}

static void Overloaded(TEvDataShard::TEvEraseRowsResponse& response) {
    response.Record.SetStatus(NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_OVERLOADED);
}

ECumulativeCounters OverloadedCounter(TEvDataShard::TEvEraseRowsRequest::TPtr&) {
    return COUNTER_ERASE_ROWS_OVERLOADED;
}

static bool TryDelayS3UploadRows(TDataShard*, TEvDataShard::TEvEraseRowsRequest::TPtr&, ERejectReasons) {
    // not a S3 upload rows
    return false;
}

template <typename TEvResponse>
using TSetStatusFunc = void(*)(TEvResponse&);

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
    setStatusFunc(*response);
    response->Record.SetTabletID(self->TabletID());
    response->Record.SetErrorDescription(rejectDescription);

    std::optional<ui64> overloadSubscribe = ev->Get()->Record.HasOverloadSubscribe() ? ev->Get()->Record.GetOverloadSubscribe() : std::optional<ui64>{};
    self->SetOverloadSubscribed(overloadSubscribe, ev->Recipient, ev->Sender, rejectReasons, response->Record);

    ctx.Send(ev->Sender, std::move(response));
}

template <typename TEvResponse, bool IsWrite, typename TEvRequest>
static bool MaybeReject(TDataShard* self, TEvRequest& ev, const TActorContext& ctx, const TString& txDesc, TDataShard::ELogThrottlerType logThrottlerType) {
    ERejectReasons rejectReasons = ERejectReasons::None;
    TString rejectDescription;
    
    if (self->IsReplicated()) {
        rejectReasons = ERejectReasons::WrongState;
        rejectDescription = TStringBuilder() << "Can't execute " << txDesc << " at replicated table";
        Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReasons, rejectDescription, &Replicated, ctx, logThrottlerType);
        return true;
    }
    
    NKikimrTxDataShard::TEvProposeTransactionResult::EStatus rejectStatus;
    if (self->CheckDataTxReject(txDesc, ctx, rejectStatus, rejectReasons, rejectDescription)) {
        self->IncCounter(OverloadedCounter(ev));

        // FIXME: it seems that s3 upload should work via TUploadRowsInternal
        // this will deduplicate code and support overload subscribed
        if (TryDelayS3UploadRows(self, ev, rejectReasons)) {
            return true;
        }

        Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReasons, rejectDescription, &WrongShardState, ctx, logThrottlerType);
        return true;
    }

    if (self->CheckChangesQueueOverflow()) {
        self->IncCounter(OverloadedCounter(ev));
        rejectReasons = ERejectReasons::ChangesQueueOverflow;
        rejectDescription = TStringBuilder() << "Change queue overflow at tablet " << self->TabletID();
        Reject<TEvResponse, TEvRequest>(self, ev, txDesc, rejectReasons, rejectDescription, &Overloaded, ctx, logThrottlerType);
        return true;
    }

    if constexpr (IsWrite) {
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
    if (ShouldDelayOperation(ev)) {
        return;
    }
    
    if (!MaybeReject<TEvDataShard::TEvUploadRowsResponse, true>(this, ev, ctx, "bulk upsert", TDataShard::ELogThrottlerType::UploadRows_Reject)) {
        Executor()->Execute(new TTxUploadRows(this, ev), ctx);
    }
}

void TDataShard::Handle(TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev, const TActorContext& ctx) {
    if (ShouldDelayOperation(ev)) {
        return;
    }

    if (!MaybeReject<TEvDataShard::TEvS3UploadRowsResponse, true>(this, ev, ctx, "s3 bulk upsert", TDataShard::ELogThrottlerType::S3UploadRows_Reject)) {
        Executor()->Execute(new TTxS3UploadRows(this, ev), ctx);
    }
}

void TDataShard::Handle(TEvDataShard::TEvEraseRowsRequest::TPtr& ev, const TActorContext& ctx) {
    if (ShouldDelayOperation(ev)) {
        return;
    }
    
    if (!MaybeReject<TEvDataShard::TEvEraseRowsResponse, false>(this, ev, ctx, "erase", TDataShard::ELogThrottlerType::EraseRows_Reject)) {
        Executor()->Execute(new TTxEraseRows(this, ev), ctx);
    }
}

} // NDataShard
} // NKikimr
