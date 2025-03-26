#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "datashard_locks_db.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TStoreAndSendOutRSUnit : public TExecutionUnit {
public:
    TStoreAndSendOutRSUnit(TDataShard &dataShard,
                           TPipeline &pipeline);
    ~TStoreAndSendOutRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TStoreAndSendOutRSUnit::TStoreAndSendOutRSUnit(TDataShard &dataShard,
                                               TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::StoreAndSendOutRS, false, dataShard, pipeline)
{
}

TStoreAndSendOutRSUnit::~TStoreAndSendOutRSUnit()
{
}

bool TStoreAndSendOutRSUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TStoreAndSendOutRSUnit::Execute(TOperation::TPtr op,
                                                 TTransactionContext &txc,
                                                 const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    bool newArtifact = false;
    // TODO: move artifact flags into operation flags.
    if (!tx->IsOutRSStored() && !op->OutReadSets().empty()) {
        DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(),
                                            op->PreparedOutReadSets(), txc, ctx);
        tx->MarkOutRSStored();
        newArtifact = true;
    }
    if (!tx->IsLocksStored() && !tx->LocksAccessLog().Locks.empty()) {
        // N.B. we copy access log to locks cache, so that future lock access is repeatable
        tx->LocksCache().Locks = tx->LocksAccessLog().Locks;
        tx->DbStoreLocksAccessLog(DataShard.TabletID(), txc, ctx);
        // Freeze persistent locks that we have cached
        for (auto& pr : tx->LocksCache().Locks) {
            ui64 lockId = pr.first;
            auto lock = DataShard.SysLocksTable().GetRawLock(lockId, TRowVersion::Min());
            if (lock && lock->IsPersistent()) {
                TDataShardLocksDb locksDb(DataShard, txc);
                lock->SetFrozen(&locksDb);
            }
        }
        tx->MarkLocksStored();
        newArtifact = true;
    }
    if (newArtifact)
        tx->DbStoreArtifactFlags(DataShard.TabletID(), txc, ctx);

    bool hadWrites = false;
    if (tx->IsOutRSStored() || tx->IsLocksStored()) {
        // Don't allow immediate writes to corrupt data we have read
        hadWrites |= Pipeline.MarkPlannedLogicallyIncompleteUpTo(TRowVersion(op->GetStep(), op->GetTxId()), txc);
    }

    if (!op->PreparedOutReadSets().empty())
        return EExecutionStatus::DelayCompleteNoMoreRestarts;

    if (newArtifact || hadWrites)
        return EExecutionStatus::ExecutedNoMoreRestarts;

    return EExecutionStatus::Executed;
}

void TStoreAndSendOutRSUnit::Complete(TOperation::TPtr op,
                                      const TActorContext &ctx)
{
    if (!op->PreparedOutReadSets().empty())
        DataShard.SendReadSets(ctx, std::move(op->PreparedOutReadSets()));
}

THolder<TExecutionUnit> CreateStoreAndSendOutRSUnit(TDataShard &dataShard,
                                                    TPipeline &pipeline)
{
    return THolder(new TStoreAndSendOutRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
