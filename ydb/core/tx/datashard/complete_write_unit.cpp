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

class TCompleteWriteUnit : public TExecutionUnit {
public:
    TCompleteWriteUnit(TDataShard &dataShard, TPipeline &pipeline);
    ~TCompleteWriteUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext &txc,const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op, const TActorContext &ctx) override;

private:
    void CompleteWrite(TOperation::TPtr op, const TActorContext &ctx);
};

TCompleteWriteUnit::TCompleteWriteUnit(TDataShard &dataShard, TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::CompleteWrite, false, dataShard, pipeline)
{
}

TCompleteWriteUnit::~TCompleteWriteUnit()
{
}

bool TCompleteWriteUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TCompleteWriteUnit::Execute(TOperation::TPtr op,
                                                 TTransactionContext &txc,
                                                 const TActorContext &ctx)
{
    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

    Pipeline.DeactivateOp(op, txc, ctx);

    if (writeOp->GetWriteResult()) {
        Pipeline.AddCompletingOp(op);
    }

    // TODO: release snapshot used by a planned tx (not currently used)
    // TODO: prepared txs may be cancelled until planned, in which case we may
    // end up with a dangling snapshot reference. Such references would have
    // to be handled in a restart-safe manner too.
    Y_DEBUG_ABORT_UNLESS(!op->HasAcquiredSnapshotKey());

    return EExecutionStatus::DelayComplete;
}

void TCompleteWriteUnit::CompleteWrite(TOperation::TPtr op, const TActorContext& ctx)
{
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

    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

    auto result = writeOp->ReleaseWriteResult();
    if (result) {
        DataShard.FillExecutionStats(op->GetExecutionProfile(), *result->Record.MutableTxStats());

        if (!gSkipRepliesFailPoint.Check(DataShard.TabletID(), op->GetTxId())) {
            result->SetOrbit(std::move(op->Orbit));
            DataShard.SendWriteResult(ctx, result, op->GetTarget(), op->GetStep(), op->GetTxId(), op->GetTraceId());
        }
    }

    Pipeline.RemoveCompletingOp(op);
}

void TCompleteWriteUnit::Complete(TOperation::TPtr op, const TActorContext &ctx)
{
    Pipeline.RemoveCommittingOp(op);
    Pipeline.RemoveTx(op->GetStepOrder());
    DataShard.IncCounter(COUNTER_WRITE_SUCCESS);
    
    CompleteWrite(op, ctx);

    DataShard.SendDelayedAcks(ctx, op->DelayedAcks());

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

THolder<TExecutionUnit> CreateCompleteWriteUnit(TDataShard &dataShard, TPipeline &pipeline)
{
    return THolder(new TCompleteWriteUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
