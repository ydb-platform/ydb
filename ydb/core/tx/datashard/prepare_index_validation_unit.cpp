#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

// Moves shadow data and creates a snapshot in one transaction
// TAlterMoveShadowUnit is used to move shadow

class TPrepareIndexValidationUnit : public TExecutionUnit {
public:
    TPrepareIndexValidationUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::PrepareIndexValidation, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasPrepareIndexValidation()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetPrepareIndexValidation();

        const TPathId tableId = TPathId::FromProto(params.GetIndexId());
        ui64 step = tx->GetStep();
        ui64 txId = tx->GetTxId();
        Y_ENSURE(step != 0);

        auto oldInfo = DataShard.FindUserTable(tableId);
        Y_ENSURE(oldInfo);
        NKikimrSchemeOp::TTableDescription description;
        oldInfo->GetSchema(description);
        Y_ENSURE(description.GetTableSchemaVersion() == params.GetTableSchemaVersion()-1);
        description.SetTableSchemaVersion(params.GetTableSchemaVersion());
        Y_ENSURE(description.MutablePartitionConfig()->GetShadowData());
        description.MutablePartitionConfig()->SetShadowData(false);
        description.MutablePartitionConfig()->MutableCompactionPolicy()->SetKeepEraseMarkers(false);
        auto newInfo = DataShard.AlterUserTable(ctx, txc, description);
        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.AddUserTable(tableId, newInfo, &locksDb);

        const TSnapshotKey key(tableId.OwnerId, tableId.LocalPathId, step, txId);

        ui64 flags = TSnapshot::FlagScheme;

        bool added = DataShard.GetSnapshotManager().AddSnapshot(
                txc.DB, key, params.GetSnapshotName(), flags, TDuration::Zero());

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

THolder<TExecutionUnit> CreatePrepareIndexValidationUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TPrepareIndexValidationUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
