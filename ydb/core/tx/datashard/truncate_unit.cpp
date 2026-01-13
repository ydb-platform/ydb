#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TTruncateUnit : public TExecutionUnit {
public:
    TTruncateUnit(TDataShard&, TPipeline&);
    ~TTruncateUnit() override;

    bool IsReadyToExecute(TOperation::TPtr) const override;
    EExecutionStatus Execute(TOperation::TPtr, TTransactionContext&, const TActorContext&) override;
    void Complete(TOperation::TPtr, const TActorContext&) override;
};

TTruncateUnit::TTruncateUnit(TDataShard& dataShard, TPipeline& pipeline)
    : TExecutionUnit(EExecutionUnitKind::Truncate, false, dataShard, pipeline)
{
}

TTruncateUnit::~TTruncateUnit() {
}

bool TTruncateUnit::IsReadyToExecute(TOperation::TPtr) const {
    return true;
}

EExecutionStatus TTruncateUnit::Execute(
    TOperation::TPtr op, TTransactionContext& txc, const TActorContext& actorCtx
) {
    TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    auto& schemeTx = tx->GetSchemeTx();

    if (!schemeTx.HasTruncateTable()) {
        return EExecutionStatus::Executed;
    }

    const auto& truncate = schemeTx.GetTruncateTable();
    const auto& pathId = TPathId::FromProto(truncate.GetPathId());
    Y_ENSURE(DataShard.GetPathOwnerId() == pathId.OwnerId);

    const auto version = truncate.GetTableSchemaVersion();
    Y_ENSURE(version);

    LOG_TRACE_S(actorCtx, NKikimrServices::TX_DATASHARD,
            "TTruncateUnit::Execute. Changing SchemaVersion. TableId = " << pathId.LocalPathId << "; "
           << " New SchemaVersion = " << version << "; "
           << " TxId = " << op->GetTxId() << ".");

    auto tableId = pathId.LocalPathId;
    Y_ENSURE(DataShard.GetUserTables().contains(tableId));
    auto localTid = DataShard.GetUserTables().at(tableId)->LocalTid;

    LOG_DEBUG_S(actorCtx, NKikimrServices::TX_DATASHARD,
               "TTruncateUnit::Execute - About to TRUNCATE TABLE at " << DataShard.TabletID()
               << " tableId# " << tableId << " localTid# " << localTid << " TxId = " << op->GetTxId());

    // break locks
    TDataShardLocksDb locksDb(DataShard, txc);

    TSetupSysLocks guardLocks(op, DataShard, &locksDb);
    const TTableId fullTableId(pathId.OwnerId, tableId);
    DataShard.SysLocksTable().BreakAllLocks(fullTableId);
    DataShard.GetConflictsCache().GetTableCache(localTid).RemoveAllUncommittedWrites(txc.DB);

    txc.DB.Truncate(localTid);

    auto userTable = DataShard.AlterTableSchemaVersion(actorCtx, txc, pathId, version);
    DataShard.AddUserTable(pathId, userTable, &locksDb);
    if (userTable->NeedSchemaSnapshots()) {
        DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, actorCtx);
    }

    txc.DB.NoMoreReadsForTx();
    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    DataShard.SysLocksTable().ApplyLocks();
    DataShard.SubscribeNewLocks(actorCtx);

    LOG_DEBUG_S(actorCtx, NKikimrServices::TX_DATASHARD,
               "TTruncateUnit::Execute - Finished successfully. TableId = " << tableId
               << " TxId = " << op->GetTxId() << " - Operation COMPLETED");

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TTruncateUnit::Complete(TOperation::TPtr,
                                    const TActorContext &)
{
}

THolder<TExecutionUnit> CreateTruncateUnit(TDataShard &dataShard, TPipeline &pipeline) {
    return THolder(new TTruncateUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
