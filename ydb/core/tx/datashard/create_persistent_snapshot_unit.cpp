#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCreatePersistentSnapshotUnit : public TExecutionUnit {
public:
    TCreatePersistentSnapshotUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreatePersistentSnapshot, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext&) override {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasCreatePersistentSnapshot()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetCreatePersistentSnapshot();

        ui64 ownerId = params.GetOwnerId();
        ui64 pathId = params.GetPathId();
        ui64 step = tx->GetStep();
        ui64 txId = tx->GetTxId();
        Y_ENSURE(step != 0);

        const TSnapshotKey key(ownerId, pathId, step, txId);

        ui64 flags = TSnapshot::FlagScheme;

        bool added = DataShard.GetSnapshotManager().AddSnapshot(
                txc.DB, key, params.GetName(), flags, TDuration::Zero());

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        if (added) {
            return EExecutionStatus::ExecutedNoMoreRestarts;
        } else {
            return EExecutionStatus::Executed;
        }
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        // nothing
    }
};

THolder<TExecutionUnit> CreateCreatePersistentSnapshotUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TCreatePersistentSnapshotUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
