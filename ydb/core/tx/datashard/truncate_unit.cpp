#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

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

    YDB_LOG_CTX_TRACE(actorCtx, "TTruncateUnit::Execute. Changing SchemaVersion. TableId New SchemaVersion TxId .",
        {"LocalPathId", pathId.LocalPathId},
        {"version", version},
        {"GetTxId", op->GetTxId()});

    auto tableId = pathId.LocalPathId;
    Y_ENSURE(DataShard.GetUserTables().contains(tableId));
    auto localTid = DataShard.GetUserTables().at(tableId)->LocalTid;

    YDB_LOG_CTX_DEBUG(actorCtx, "TTruncateUnit::Execute - About to TRUNCATE TABLE at TxId",
        {"TabletID", DataShard.TabletID()},
        {"tableId", tableId},
        {"localTid", localTid},
        {"GetTxId", op->GetTxId()});

    // break locks
    TDataShardLocksDb locksDb(DataShard, txc);

    TSetupSysLocks guardLocks(op, DataShard, &locksDb);
    const TTableId fullTableId(pathId.OwnerId, tableId);
    DataShard.SysLocksTable().BreakAllLocks(fullTableId);
    DataShard.GetConflictsCache().GetTableCache(localTid).RemoveAllUncommittedWrites(txc.DB);

    txc.DB.Truncate(localTid);

    auto userTable = DataShard.AlterTableSchemaVersion(actorCtx, txc, pathId, version);

    // We must set these flags here for the following reasons:
    //
    // 1. Space usage statistics in the local database are aggregated from two sources:
    //    SSTs and the MemTable.
    //
    // 2. If TRUNCATE is executed without these flags, the SST stats will not be
    //    recalculated. This is because the `userTable` object is copied within
    //    `AlterTableSchemaVersion`, and that copy can carry over stale statistic
    //    values from the old `userTable` instance.
    //
    // 3. By setting the `StatsUpdateInProgress` and `StatsNeedUpdate` flags, we
    //    force a full recalculation of LocalDB statistics after the TRUNCATE completes.
    //
    // This is primarily crucial for ensuring an accurate calculation of the byte size
    // occupied by the user table.
    userTable->StatsUpdateInProgress = false;
    userTable->StatsNeedUpdate = true;

    DataShard.AddUserTable(pathId, userTable, &locksDb);
    if (userTable->NeedSchemaSnapshots()) {
        DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, actorCtx);
    }

    txc.DB.NoMoreReadsForTx();
    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    DataShard.SysLocksTable().ApplyLocks();
    DataShard.SubscribeNewLocks(actorCtx);

    YDB_LOG_CTX_DEBUG(actorCtx, "TTruncateUnit::Execute - Finished successfully. TableId TxId - Operation COMPLETED",
        {"tableId", tableId},
        {"GetTxId", op->GetTxId()});

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
