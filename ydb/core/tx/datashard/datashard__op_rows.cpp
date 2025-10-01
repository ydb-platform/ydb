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

void TDataShard::Handle(TEvDataShard::TEvUploadRowsRequest::TPtr& ev, const TActorContext& ctx) {
    if (ShouldDelayUpload(ev)) {
        return;
    }

    // Note: all validation checks are placed inside TCommonUploadOps::Execute

    Executor()->Execute(new TTxUploadRows(this, ev), ctx);
}

void TDataShard::Handle(TEvDataShard::TEvEraseRowsRequest::TPtr& ev, const TActorContext& ctx) {
    if (ShouldDelayUpload(ev)) {
        return;
    }

    auto reject = [&](NKikimrTxDataShard::TEvEraseRowsResponse::EStatus status, ERejectReasons rejectReasons, const TString& rejectDescription) {
        LOG_LOG_S_THROTTLE(GetLogThrottler(TDataShard::ELogThrottlerType::EraseRows_Reject), ctx, NActors::NLog::PRI_NOTICE, NKikimrServices::TX_DATASHARD,
            "Rejecting erase request on datashard"
                << ": tablet# " << TabletID()
                << ", status# " << status
                << ", reasons# " << rejectReasons
                << ", error# " << rejectDescription);

        auto response = MakeHolder<TEvDataShard::TEvEraseRowsResponse>();
        response->Record.SetTabletID(TabletID());
        response->Record.SetStatus(status);
        response->Record.SetErrorDescription(rejectDescription);

        std::optional<ui64> overloadSubscribe = ev->Get()->Record.HasOverloadSubscribe() ? ev->Get()->Record.GetOverloadSubscribe() : std::optional<ui64>{};
        SetOverloadSubscribed(overloadSubscribe, ev->Recipient, ev->Sender, rejectReasons, response->Record);

        ctx.Send(ev->Sender, std::move(response));
    };

    if (IsReplicated()) {
        return reject(NKikimrTxDataShard::TEvEraseRowsResponse::EXEC_ERROR, ERejectReasons::WrongState,
            "Can't execute erase at replicated table");
    }

    {
        NKikimrTxDataShard::TEvProposeTransactionResult::EStatus rejectStatus;
        ERejectReasons rejectReasons = ERejectReasons::None;
        TString rejectDescription;
        if (CheckDataTxReject("erase", rejectStatus, rejectReasons, rejectDescription)) {
            IncCounter(COUNTER_ERASE_ROWS_OVERLOADED);
            return reject(NKikimrTxDataShard::TEvEraseRowsResponse::WRONG_SHARD_STATE, rejectReasons, rejectDescription);
        }
    }

    if (CheckChangesQueueOverflow()) {
        IncCounter(COUNTER_ERASE_ROWS_OVERLOADED);
        return reject(NKikimrTxDataShard::TEvEraseRowsResponse::SHARD_OVERLOADED, ERejectReasons::ChangesQueueOverflow,
            TStringBuilder() << "Change queue overflow at tablet " << TabletID());
    }
        
    Executor()->Execute(new TTxEraseRows(this, ev), ctx);
}

} // NDataShard
} // NKikimr
