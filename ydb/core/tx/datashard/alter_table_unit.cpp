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
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

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
            Y_ENSURE(DataShard.GetPathOwnerId() == pathId.GetOwnerId());
            tableId = pathId.GetLocalId();
        }

        // Only applicable when table has ShadowData enabled
        Y_ENSURE(DataShard.GetUserTables().contains(tableId));
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

        Y_ENSURE(op->InputSnapshots().size() == 1, "Expected a single shadow snapshot");
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
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasAlterTable())
        return EExecutionStatus::Executed;

    const auto& alterTableTx = schemeTx.GetAlterTable();

    const auto version = alterTableTx.GetTableSchemaVersion();
    Y_ENSURE(version);

    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
               "Trying to ALTER TABLE at " << DataShard.TabletID()
               << " version " << version);

    TPathId tableId(DataShard.GetPathOwnerId(), alterTableTx.GetId_Deprecated());
    if (alterTableTx.HasPathId()) {
        auto& pathId = alterTableTx.GetPathId();
        Y_ENSURE(DataShard.GetPathOwnerId() == pathId.GetOwnerId());
        tableId.LocalPathId = pathId.GetLocalId();
    }

    auto oldInfo = DataShard.FindUserTable(tableId);
    auto newInfo = DataShard.AlterUserTable(ctx, txc, alterTableTx);
    TDataShardLocksDb locksDb(DataShard, txc);
    DataShard.AddUserTable(tableId, newInfo, &locksDb);

    if (newInfo->NeedSchemaSnapshots()) {
        DataShard.AddSchemaSnapshot(tableId, version, op->GetStep(), op->GetTxId(), txc, ctx);
    }

    bool schemaChanged = false;
    if (alterTableTx.DropColumnsSize()) {
        schemaChanged = true;
    } else {
        for (const auto& [tag, column] : newInfo->Columns) {
            if (!oldInfo->Columns.contains(tag)) {
                schemaChanged = true;
                break;
            }
        }
    }

    if (schemaChanged) {
        NIceDb::TNiceDb db(txc.DB);

        for (const auto& streamPathId : newInfo->GetSchemaChangesCdcStreams()) {
            auto recordPtr = TChangeRecordBuilder(TChangeRecord::EKind::CdcSchemaChange)
                .WithOrder(DataShard.AllocateChangeRecordOrder(db))
                .WithGroup(0)
                .WithStep(op->GetStep())
                .WithTxId(op->GetTxId())
                .WithPathId(streamPathId)
                .WithTableId(tableId)
                .WithSchemaVersion(newInfo->GetTableSchemaVersion())
                .Build();

            const auto& record = *recordPtr;
            DataShard.PersistChangeRecord(db, record);

            op->ChangeRecords().push_back(IDataShardChangeCollector::TChange{
                .Order = record.GetOrder(),
                .Group = record.GetGroup(),
                .Step = record.GetStep(),
                .TxId = record.GetTxId(),
                .PathId = record.GetPathId(),
                .BodySize = 0,
                .TableId = record.GetTableId(),
                .SchemaVersion = record.GetSchemaVersion(),
            });
        }
    }

    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TAlterTableUnit::Complete(TOperation::TPtr op,
                               const TActorContext &)
{
    DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
}

THolder<TExecutionUnit> CreateAlterTableUnit(TDataShard &dataShard,
                                             TPipeline &pipeline)
{
    return THolder(new TAlterTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
