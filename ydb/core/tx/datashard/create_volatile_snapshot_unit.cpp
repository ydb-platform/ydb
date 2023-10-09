#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCreateVolatileSnapshotUnit : public TExecutionUnit {
public:
    TCreateVolatileSnapshotUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreateVolatileSnapshot, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsSnapshotTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        const auto& record = tx->GetSnapshotTx();
        if (!record.HasCreateVolatileSnapshot()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = record.GetCreateVolatileSnapshot();

        ui64 ownerId = params.GetOwnerId();
        ui64 pathId = params.GetPathId();
        ui64 step = tx->GetStep();
        ui64 txId = tx->GetTxId();
        Y_ABORT_UNLESS(step != 0);

        const TSnapshotKey key(ownerId, pathId, step, txId);

        ui64 flags = 0;
        TDuration timeout;
        if (params.HasTimeoutMs()) {
            flags |= TSnapshot::FlagTimeout;
            timeout = TDuration::MilliSeconds(params.GetTimeoutMs());
        }

        bool added = DataShard.GetSnapshotManager().AddSnapshot(
                txc.DB, key, params.GetName(), flags, timeout);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        if (added) {
            DataShard.GetSnapshotManager().InitSnapshotExpireTime(key, ctx.Now());
            if (DataShard.GetSnapshotManager().HasExpiringSnapshots())
                DataShard.PlanCleanup(ctx);

            return EExecutionStatus::ExecutedNoMoreRestarts;
        } else {
            return EExecutionStatus::Executed;
        }
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        // nothing
    }
};

THolder<TExecutionUnit> CreateCreateVolatileSnapshotUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TCreateVolatileSnapshotUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
