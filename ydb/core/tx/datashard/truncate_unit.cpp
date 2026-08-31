#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "datashard_locks_db.h"

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

    YDB_LOG_TRACE_CTX(actorCtx, "TTruncateUnit::Execute: changing schema version",
        {"localPathId", pathId.LocalPathId},
        {"version", version},
        {"txId", op->GetTxId()});

    auto tableId = pathId.LocalPathId;
    Y_ENSURE(DataShard.GetUserTables().contains(tableId));
    auto localTid = DataShard.GetUserTables().at(tableId)->LocalTid;

    YDB_LOG_DEBUG_CTX(actorCtx, "TTruncateUnit::Execute: about to truncate table",
        {"tabletId", DataShard.TabletID()},
        {"tableId", tableId},
        {"localTid", localTid},
        {"txId", op->GetTxId()});

    TDataShardLocksDb locksDb(DataShard, txc);

    DataShard.GetConflictsCache().GetTableCache(localTid).RemoveAllUncommittedWrites(txc.DB);

    txc.DB.Truncate(localTid);

    // Truncate drops every row version, so nothing below this operation can be read any more.
    // Advancing the low watermark keeps a stale snapshot read failing loudly instead of silently
    // returning no rows.
    DataShard.GetSnapshotManager().AdvanceWatermark(txc.DB, DataShard.GetMvccVersion(op.Get()));

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

    // Passing locksDb invalidates every lock of this shard, like any other schema change does.
    DataShard.ReplaceUserTable(pathId, userTable, locksDb);
    if (userTable->NeedSchemaSnapshots()) {
        DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, actorCtx);
    }

    txc.DB.NoMoreReadsForTx();
    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    YDB_LOG_DEBUG_CTX(actorCtx, "TTruncateUnit::Execute: finished successfully",
        {"tableId", tableId},
        {"txId", op->GetTxId()});

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
