#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

////////////////////////////////////////////////////////////////////////////////

class TAlterMoveShadowUnit : public TExecutionUnit {
public:
    TAlterMoveShadowUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::AlterMoveShadow, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        if (!op->IsWaitingForSnapshot())
            return true;

        return !op->InputSnapshots().empty();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        // Only applicable when ALTER TABLE is in the transaction
        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasAlterTable())
            return EExecutionStatus::Executed;

        // Only applicable when ALTER TABLE has disabled ShadowData
        const auto& alter = schemeTx.GetAlterTable();
        const bool shadowDisabled = (
            alter.HasPartitionConfig() &&
            alter.GetPartitionConfig().HasShadowData() &&
            !alter.GetPartitionConfig().GetShadowData());
        if (!shadowDisabled)
            return EExecutionStatus::Executed;

        ui64 tableId = alter.GetId_Deprecated();
        if (alter.HasPathId()) {
            auto& pathId = alter.GetPathId();
            Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == pathId.GetOwnerId());
            tableId = pathId.GetLocalId();
        }

        // Only applicable when table has ShadowData enabled
        Y_ABORT_UNLESS(DataShard.GetUserTables().contains(tableId));
        const TUserTable& table = *DataShard.GetUserTables().at(tableId);
        const ui32 localTid = table.LocalTid;
        const ui32 shadowTid = table.ShadowTid;
        if (!shadowTid)
            return EExecutionStatus::Executed;

        // We must create shadow table snapshot
        if (!op->IsWaitingForSnapshot()) {
            TIntrusivePtr<TTableSnapshotContext> snapContext
                = new TTxTableSnapshotContext(op->GetStep(), op->GetTxId(), { shadowTid });
            txc.Env.MakeSnapshot(snapContext);

            op->SetWaitingForSnapshotFlag();
            return EExecutionStatus::Continue;
        }

        Y_ABORT_UNLESS(op->InputSnapshots().size() == 1, "Expected a single shadow snapshot");
        {
            auto& snapshot = op->InputSnapshots()[0];
            txc.Env.MoveSnapshot(*snapshot, /* src */ shadowTid, /* dst */ localTid);
            txc.Env.DropSnapshot(snapshot);
        }

        // Snapshot move cannot be mixed with other operations on shadowTid
        // We have to wait for completion before dropping shadow table
        op->InputSnapshots().clear();
        op->ResetWaitingForSnapshotFlag();
        return EExecutionStatus::WaitComplete;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Y_UNUSED(op);
        Y_UNUSED(ctx);
    }
};

THolder<TExecutionUnit> CreateAlterMoveShadowUnit(TDataShard& dataShard, TPipeline& pipeline)
{
    return THolder(new TAlterMoveShadowUnit(dataShard, pipeline));
}

////////////////////////////////////////////////////////////////////////////////

class TAlterTableUnit : public TExecutionUnit {
public:
    TAlterTableUnit(TDataShard &dataShard,
                    TPipeline &pipeline);
    ~TAlterTableUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TAlterTableUnit::TAlterTableUnit(TDataShard &dataShard,
                                 TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::AlterTable, false, dataShard, pipeline)
{
}

TAlterTableUnit::~TAlterTableUnit()
{
}

bool TAlterTableUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TAlterTableUnit::Execute(TOperation::TPtr op,
                                          TTransactionContext &txc,
                                          const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasAlterTable())
        return EExecutionStatus::Executed;

    const auto& alterTableTx = schemeTx.GetAlterTable();

    const auto version = alterTableTx.GetTableSchemaVersion();
    Y_ABORT_UNLESS(version);

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
               "Trying to ALTER TABLE at " << DataShard.TabletID()
               << " version " << version);

    TPathId tableId(DataShard.GetPathOwnerId(), alterTableTx.GetId_Deprecated());
    if (alterTableTx.HasPathId()) {
        auto& pathId = alterTableTx.GetPathId();
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == pathId.GetOwnerId());
        tableId.LocalPathId = pathId.GetLocalId();
    }

    TUserTable::TPtr info = DataShard.AlterUserTable(ctx, txc, alterTableTx);
    TDataShardLocksDb locksDb(DataShard, txc);
    DataShard.AddUserTable(tableId, info, &locksDb);

    if (info->NeedSchemaSnapshots()) {
        DataShard.AddSchemaSnapshot(tableId, version, op->GetStep(), op->GetTxId(), txc, ctx);
    }

    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    return EExecutionStatus::ExecutedNoMoreRestarts;
}

void TAlterTableUnit::Complete(TOperation::TPtr,
                               const TActorContext &)
{
}

THolder<TExecutionUnit> CreateAlterTableUnit(TDataShard &dataShard,
                                             TPipeline &pipeline)
{
    return THolder(new TAlterTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
