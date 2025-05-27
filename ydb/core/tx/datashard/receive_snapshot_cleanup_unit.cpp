#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TReceiveSnapshotCleanupUnit : public TExecutionUnit {
public:
    TReceiveSnapshotCleanupUnit(TDataShard &dataShard,
                         TPipeline &pipeline);
    ~TReceiveSnapshotCleanupUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TReceiveSnapshotCleanupUnit::TReceiveSnapshotCleanupUnit(TDataShard &dataShard,
                                                         TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::ReceiveSnapshotCleanup, false, dataShard, pipeline)
{
}

TReceiveSnapshotCleanupUnit::~TReceiveSnapshotCleanupUnit()
{
}

bool TReceiveSnapshotCleanupUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TReceiveSnapshotCleanupUnit::Execute(TOperation::TPtr op,
                                                      TTransactionContext &txc,
                                                      const TActorContext &)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasReceiveSnapshot())
        return EExecutionStatus::Executed;

    size_t removedTxs = 0;
    for (const auto& pr : DataShard.GetUserTables()) {
        auto localTid = pr.second->LocalTid;
        auto openTxs = txc.DB.GetOpenTxs(localTid);
        for (ui64 txId : openTxs) {
            if (removedTxs >= 1000) {
                // We don't want to remove more than 1000 txs at a time
                // Commit current changes and reschedule
                return EExecutionStatus::Reschedule;
            }
            txc.DB.RemoveTx(localTid, txId);
            DataShard.GetConflictsCache().GetTableCache(localTid).RemoveUncommittedWrites(txId, txc.DB);
            ++removedTxs;
        }
    }

    if (removedTxs > 0) {
        return EExecutionStatus::ExecutedNoMoreRestarts;
    }

    return EExecutionStatus::Executed;
}

void TReceiveSnapshotCleanupUnit::Complete(TOperation::TPtr,
                                           const TActorContext &)
{
}

THolder<TExecutionUnit> CreateReceiveSnapshotCleanupUnit(TDataShard &dataShard,
                                                         TPipeline &pipeline)
{
    return MakeHolder<TReceiveSnapshotCleanupUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
