#include "datashard_failpoints.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "probes.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>

LWTRACE_USING(DATASHARD_PROVIDER)

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TCompleteOperationUnit : public TExecutionUnit {
public:
    TCompleteOperationUnit(TDataShard &dataShard,
                           TPipeline &pipeline);
    ~TCompleteOperationUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    void CompleteOperation(TOperation::TPtr op,
                           const TActorContext &ctx);
};

TCompleteOperationUnit::TCompleteOperationUnit(TDataShard &dataShard,
                                               TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::CompleteOperation, false, dataShard, pipeline)
{
}

TCompleteOperationUnit::~TCompleteOperationUnit()
{
}

bool TCompleteOperationUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TCompleteOperationUnit::Execute(TOperation::TPtr op,
                                                 TTransactionContext &txc,
                                                 const TActorContext &ctx)
{
    Pipeline.DeactivateOp(op, txc, ctx);

    TOutputOpData::TResultPtr &result = op->Result();
    if (result) {
        auto execLatency = op->GetCompletedAt() - op->GetStartExecutionAt();
        result->Record.SetExecLatency(execLatency.MilliSeconds());
    }

    if (op->IsDirty()) {
        DataShard.IncCounter(COUNTER_TX_PROGRESS_DIRTY);
        CompleteOperation(op, ctx);
    } else if (result) {
        Pipeline.AddCompletingOp(op);
    }

    // TODO: release snapshot used by a planned tx (not currently used)
    // TODO: prepared txs may be cancelled until planned, in which case we may
    // end up with a dangling snapshot reference. Such references would have
    // to be handled in a restart-safe manner too.
    Y_DEBUG_ABORT_UNLESS(!op->HasAcquiredSnapshotKey());

    return EExecutionStatus::DelayComplete;
}

void TCompleteOperationUnit::CompleteOperation(TOperation::TPtr op,
                                               const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    auto duration = TAppData::TimeProvider->Now() - op->GetStartExecutionAt();

    if (DataShard.GetDataTxProfileLogThresholdMs()
        && duration.MilliSeconds() >= DataShard.GetDataTxProfileLogThresholdMs()) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD,
                   op->ExecutionProfileLogString(DataShard.TabletID()));
    }

    if (DataShard.GetDataTxProfileBufferThresholdMs()
        && duration.MilliSeconds() >= DataShard.GetDataTxProfileBufferThresholdMs()) {
        Pipeline.HoldExecutionProfile(op);
    }

    TOutputOpData::TResultPtr result = std::move(op->Result());
    if (result) {
        result->Record.SetProposeLatency(duration.MilliSeconds());

        DataShard.FillExecutionStats(op->GetExecutionProfile(), *result->Record.MutableTxStats());

        if (!gSkipRepliesFailPoint.Check(DataShard.TabletID(), op->GetTxId())) {
            result->Orbit = std::move(op->Orbit);
            DataShard.SendResult(ctx, result, op->GetTarget(), op->GetStep(), op->GetTxId(), op->GetTraceId());
        }
    }

    Pipeline.RemoveCompletingOp(op);
}

void TCompleteOperationUnit::Complete(TOperation::TPtr op,
                                      const TActorContext &ctx)
{
    Pipeline.RemoveCommittingOp(op);
    Pipeline.RemoveTx(op->GetStepOrder());
    DataShard.IncCounter(COUNTER_PLANNED_TX_COMPLETE);
    if (!op->IsDirty())
        CompleteOperation(op, ctx);

    DataShard.SendDelayedAcks(ctx, op->DelayedAcks());
    if (op->IsSchemeTx())
        DataShard.NotifySchemeshard(ctx, op->GetTxId());

    DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
    DataShard.EmitHeartbeats();

    if (op->HasOutputData()) {
        const auto& outReadSets = op->OutReadSets();
        const auto& expectedReadSets = op->ExpectedReadSets();
        auto itOut = outReadSets.begin();
        auto itExpected = expectedReadSets.begin();
        while (itExpected != expectedReadSets.end()) {
            while (itOut != outReadSets.end() && itOut->first < itExpected->first) {
                ++itOut;
            }
            if (itOut != outReadSets.end() && itOut->first == itExpected->first) {
                ++itOut;
                ++itExpected;
                continue;
            }
            // We have an expected readset without a corresponding out readset
            for (const auto& recipient : itExpected->second) {
                DataShard.SendReadSetNoData(ctx, recipient, op->GetStep(), op->GetTxId(), itExpected->first.first, itExpected->first.second);
            }
            ++itExpected;
        }
    }
}

THolder<TExecutionUnit> CreateCompleteOperationUnit(TDataShard &dataShard,
                                                    TPipeline &pipeline)
{
    return THolder(new TCompleteOperationUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
