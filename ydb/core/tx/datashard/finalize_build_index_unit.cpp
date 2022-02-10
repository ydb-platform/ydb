#include "datashard_impl.h" 
#include "datashard_pipeline.h" 
#include "execution_unit_ctors.h" 
 
namespace NKikimr { 
namespace NDataShard {
 
class TFinalizeBuildIndexUnit : public TExecutionUnit { 
public: 
    TFinalizeBuildIndexUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::FinalizeBuildIndex, false, dataShard, pipeline) 
    { } 
 
    bool IsReadyToExecute(TOperation::TPtr) const override { 
        return true; 
    } 
 
    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override { 
        Y_VERIFY(op->IsSchemeTx()); 
 
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get()); 
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind()); 
 
        auto& schemeTx = tx->GetSchemeTx(); 
        if (!schemeTx.HasFinalizeBuildIndex()) { 
            return EExecutionStatus::Executed; 
        } 
 
        const auto& params = schemeTx.GetFinalizeBuildIndex(); 
 
        auto pathId = TPathId(params.GetPathId().GetOwnerId(), params.GetPathId().GetLocalId()); 
        Y_VERIFY(pathId.OwnerId == DataShard.GetPathOwnerId()); 
 
        auto tableInfo = DataShard.AlterTableSchemaVersion(ctx, txc, pathId, params.GetTableSchemaVersion()); 
        DataShard.AddUserTable(pathId, tableInfo);
 
        ui64 step = params.GetSnapshotStep(); 
        ui64 txId = params.GetSnapshotTxId(); 
        Y_VERIFY(step != 0); 
 
        const TSnapshotKey key(pathId.OwnerId, pathId.LocalPathId, step, txId); 
 
        if (DataShard.GetBuildIndexManager().Contains(params.GetBuildIndexId())) { 
            auto  record = DataShard.GetBuildIndexManager().Get(params.GetBuildIndexId()); 
            DataShard.CancelScan(tableInfo->LocalTid, record.ScanId); 
            DataShard.GetBuildIndexManager().Drop(params.GetBuildIndexId()); 
        } 
 
        bool removed = DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key); 
 
        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE); 
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair()); 
 
        if (removed) { 
            return EExecutionStatus::ExecutedNoMoreRestarts; 
        } else { 
            return EExecutionStatus::Executed; 
        } 
    } 
 
    void Complete(TOperation::TPtr, const TActorContext&) override { 
        // nothing 
    } 
}; 
 
THolder<TExecutionUnit> CreateFinalizeBuildIndexUnit( 
    TDataShard& dataShard,
    TPipeline& pipeline) 
{ 
    return THolder(new TFinalizeBuildIndexUnit(dataShard, pipeline));
} 
 
} // namespace NDataShard
} // namespace NKikimr 
