#include "datashard_failpoints.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "probes.h"

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr {
namespace NDataShard {

class TFinishProposeUnit : public TExecutionUnit {
public:
    TFinishProposeUnit(TDataShard &dataShard,
                       TPipeline &pipeline);
    ~TFinishProposeUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    TDataShard::TPromotePostExecuteEdges PromoteImmediatePostExecuteEdges(TOperation* op, TTransactionContext& txc);
    void CompleteRequest(TOperation::TPtr op,
                         const TActorContext &ctx);
    void AddDiagnosticsResult(TOutputOpData::TResultPtr &res);
    void UpdateCounters(TOperation::TPtr op,
                        const TActorContext &ctx);
};

TFinishProposeUnit::TFinishProposeUnit(TDataShard &dataShard,
                                       TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::FinishPropose, false, dataShard, pipeline)
{
}

TFinishProposeUnit::~TFinishProposeUnit()
{
}

bool TFinishProposeUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

TDataShard::TPromotePostExecuteEdges TFinishProposeUnit::PromoteImmediatePostExecuteEdges(
        TOperation* op,
        TTransactionContext& txc)
{
    if (op->IsMvccSnapshotRead()) {
        if (op->IsMvccSnapshotRepeatable() && op->GetPerformedUserReads()) {
            return DataShard.PromoteImmediatePostExecuteEdges(op->GetMvccSnapshot(), TDataShard::EPromotePostExecuteEdges::RepeatableRead, txc);
        } else {
            return DataShard.PromoteImmediatePostExecuteEdges(op->GetMvccSnapshot(), TDataShard::EPromotePostExecuteEdges::ReadOnly, txc);
        }
    } else if (op->MvccReadWriteVersion) {
        if (op->IsReadOnly() || op->LockTxId()) {
            return DataShard.PromoteImmediatePostExecuteEdges(*op->MvccReadWriteVersion, TDataShard::EPromotePostExecuteEdges::ReadOnly, txc);
        } else {
            return DataShard.PromoteImmediatePostExecuteEdges(*op->MvccReadWriteVersion, TDataShard::EPromotePostExecuteEdges::ReadWrite, txc);
        }
    } else {
        return { };
    }
}

EExecutionStatus TFinishProposeUnit::Execute(TOperation::TPtr op,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    if (op->Result())
        UpdateCounters(op, ctx);

    bool hadWrites = false;

    // When mvcc is enabled we perform marking after transaction is executed
    if (op->IsAborted()) {
        // Make sure we confirm aborts with a commit
        op->SetWaitCompletionFlag(true);
    } else if (DataShard.IsFollower()) {
        // It doesn't matter whether we wait or not
    } else if (DataShard.IsMvccEnabled() && op->IsImmediate()) {
        auto res = PromoteImmediatePostExecuteEdges(op.Get(), txc);

        if (res.HadWrites) {
            hadWrites = true;
            res.WaitCompletion = true;
        }

        if (res.WaitCompletion) {
            op->SetWaitCompletionFlag(true);
        }
    }

    if (op->HasVolatilePrepareFlag() && !op->HasResultSentFlag() && !op->IsDirty()) {
        op->SetFinishProposeTs(DataShard.ConfirmReadOnlyLease());
    }

    if (!op->HasResultSentFlag() && (op->IsDirty() || op->HasVolatilePrepareFlag() || !Pipeline.WaitCompletion(op))) {
        DataShard.IncCounter(COUNTER_PREPARE_COMPLETE);
        op->SetProposeResultSentEarly();
        CompleteRequest(op, ctx);
    }

    if (!DataShard.IsFollower())
        DataShard.PlanCleanup(ctx);

    // Release acquired snapshot for immediate and aborted operations
    // N.B. currently only immediate operations may acquire snapshots, but in
    // the future it may be possible for read/write operations to read and write
    // at different points in time. Those snapshots would need to stay acquired
    // until the operation is complete.
    auto status = EExecutionStatus::DelayComplete;
    if (hadWrites) {
        status = EExecutionStatus::DelayCompleteNoMoreRestarts;
    }
    if (op->HasAcquiredSnapshotKey() && (op->IsImmediate() || op->IsAborted())) {
        if (DataShard.GetSnapshotManager().ReleaseReference(op->GetAcquiredSnapshotKey(), txc.DB, ctx.Now())) {
            status = EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        op->ResetAcquiredSnapshotKey();
    }

    return status;
}

void TFinishProposeUnit::Complete(TOperation::TPtr op,
                                  const TActorContext &ctx)
{
    if (!op->HasResultSentFlag() && !op->IsProposeResultSentEarly()) {
        DataShard.IncCounter(COUNTER_PREPARE_COMPLETE);

        if (op->Result())
            CompleteRequest(op, ctx);
    }

    Pipeline.ForgetUnproposedTx(op->GetTxId());
    if (op->IsImmediate()) {
        Pipeline.RemoveCommittingOp(op);
        Pipeline.RemoveActiveOp(op);

        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
        DataShard.EmitHeartbeats();
    }

    DataShard.SendRegistrationRequestTimeCast(ctx);
}

void TFinishProposeUnit::CompleteRequest(TOperation::TPtr op,
                                         const TActorContext &ctx)
{
    auto res = std::move(op->Result());
    Y_ABORT_UNLESS(res);

    TDuration duration = TAppData::TimeProvider->Now() - op->GetReceivedAt();
    res->Record.SetProposeLatency(duration.MilliSeconds());

    LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                "Propose transaction complete txid " << op->GetTxId() << " at tablet "
                << DataShard.TabletID() << " send to client, exec latency: "
                << res->Record.GetExecLatency() << " ms, propose latency: "
                << duration.MilliSeconds() << " ms, status: " << res->GetStatus());

    TString errors = res->GetError();
    if (errors.size()) {
        LOG_LOG_S_THROTTLE(DataShard.GetLogThrottler(TDataShard::ELogThrottlerType::FinishProposeUnit_CompleteRequest), ctx, NActors::NLog::PRI_ERROR, NKikimrServices::TX_DATASHARD, 
                    "Errors while proposing transaction txid " << op->GetTxId()
                    << " at tablet " << DataShard.TabletID() << " status: "
                    << res->GetStatus() << " errors: " << errors);
    }

    if (res->IsPrepared()) {
        DataShard.IncCounter(COUNTER_PREPARE_SUCCESS_COMPLETE_LATENCY, duration);
    } else {
        DataShard.CheckSplitCanStart(ctx);
        DataShard.CheckMvccStateChangeCanStart(ctx);
    }

    if (op->HasNeedDiagnosticsFlag())
        AddDiagnosticsResult(res);

    DataShard.FillExecutionStats(op->GetExecutionProfile(), *res->Record.MutableTxStats());

    DataShard.IncCounter(COUNTER_TX_RESULT_SIZE, res->Record.GetTxResult().size());

    if (!gSkipRepliesFailPoint.Check(DataShard.TabletID(), op->GetTxId())) {
        if (res->IsPrepared()) {
            LWTRACK(ProposeTransactionSendPrepared, op->Orbit);
        } else {
            LWTRACK(ProposeTransactionSendResult, op->Orbit);
            res->Orbit = std::move(op->Orbit);
        }
        if (op->IsImmediate() && !op->IsReadOnly() && !op->IsAborted() && op->MvccReadWriteVersion) {
            DataShard.SendImmediateWriteResult(*op->MvccReadWriteVersion, op->GetTarget(), res.Release(), op->GetCookie(), {}, op->GetTraceId());
        } else if (op->IsImmediate() && op->IsReadOnly() && !op->IsAborted()) {
            // TODO: we should actually measure a read timestamp and use it here
            DataShard.SendImmediateReadResult(op->GetTarget(), res.Release(), op->GetCookie(), {}, op->GetTraceId());
        } else if (op->HasVolatilePrepareFlag() && !op->IsDirty()) {
            DataShard.SendWithConfirmedReadOnlyLease(op->GetFinishProposeTs(), op->GetTarget(), res.Release(), op->GetCookie(), {}, op->GetTraceId());
        } else {
            ctx.Send(op->GetTarget(), res.Release(), 0, op->GetCookie(), op->GetTraceId());
        }
    }
}

void TFinishProposeUnit::AddDiagnosticsResult(TOutputOpData::TResultPtr &res)
{
    auto &tabletInfo = *res->Record.MutableTabletInfo();
    ActorIdToProto(DataShard.SelfId(), tabletInfo.MutableActorId());

    tabletInfo.SetTabletId(DataShard.TabletID());
    tabletInfo.SetGeneration(DataShard.Generation());
    tabletInfo.SetStep(DataShard.GetExecutorStep());
    tabletInfo.SetIsFollower(DataShard.IsFollower());
}

void TFinishProposeUnit::UpdateCounters(TOperation::TPtr op,
                                        const TActorContext &ctx)
{
    auto &res = op->Result();
    Y_ABORT_UNLESS(res);
    auto execLatency = TAppData::TimeProvider->Now() - op->GetReceivedAt();

    res->Record.SetExecLatency(execLatency.MilliSeconds());

    DataShard.IncCounter(COUNTER_PREPARE_EXEC_LATENCY, execLatency);
    if (res->IsPrepared()) {
        DataShard.IncCounter(COUNTER_PREPARE_SUCCESS);
    } else {
        if (op->IsDirty())
            DataShard.IncCounter(COUNTER_PREPARE_DIRTY);

        if (res->IsError()) {
            DataShard.IncCounter(COUNTER_PREPARE_ERROR);
            LOG_LOG_S_THROTTLE(DataShard.GetLogThrottler(TDataShard::ELogThrottlerType::FinishProposeUnit_UpdateCounters), ctx,  NActors::NLog::PRI_ERROR, NKikimrServices::TX_DATASHARD,
                        "Prepare transaction failed. txid " << op->GetTxId()
                        << " at tablet " << DataShard.TabletID()  << " errors: " << res->GetError());
        } else {
            DataShard.IncCounter(COUNTER_PREPARE_IMMEDIATE);
        }
    }
}



THolder<TExecutionUnit> CreateFinishProposeUnit(TDataShard &dataShard,
                                                TPipeline &pipeline)
{
    return THolder(new TFinishProposeUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
