#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "datashard_locks_db.h"

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;

class TStoreAndSendWriteOutRSUnit : public TExecutionUnit {
public:
    TStoreAndSendWriteOutRSUnit(TDataShard &dataShard,
                           TPipeline &pipeline);
    ~TStoreAndSendWriteOutRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TStoreAndSendWriteOutRSUnit::TStoreAndSendWriteOutRSUnit(TDataShard &dataShard,
                                               TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::StoreAndSendWriteOutRS, false, dataShard, pipeline)
{
}

TStoreAndSendWriteOutRSUnit::~TStoreAndSendWriteOutRSUnit()
{
}

bool TStoreAndSendWriteOutRSUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TStoreAndSendWriteOutRSUnit::Execute(TOperation::TPtr op,
                                                 TTransactionContext &txc,
                                                 const TActorContext &ctx)
{
    TWriteOperation* writeOp = TWriteOperation::CastWriteOperation(op);

    bool newArtifact = false;
    // TODO: move artifact flags into operation flags.
    if (!writeOp->IsOutRSStored() && !op->OutReadSets().empty()) {
        DataShard.PrepareAndSaveOutReadSets(op->GetStep(), op->GetTxId(), op->OutReadSets(),
                                            op->PreparedOutReadSets(), txc, ctx);
        writeOp->MarkOutRSStored();
        newArtifact = true;
    }
    if (!writeOp->IsLocksStored() && !writeOp->LocksAccessLog().Locks.empty()) {
        // N.B. we copy access log to locks cache, so that future lock access is repeatable
        writeOp->LocksCache().Locks = writeOp->LocksAccessLog().Locks;
        writeOp->DbStoreLocksAccessLog(txc.DB);
        // Freeze persistent locks that we have cached
        for (auto& pr : writeOp->LocksCache().Locks) {
            ui64 lockId = pr.first;
            auto lock = DataShard.SysLocksTable().GetRawLock(lockId, TRowVersion::Min());
            if (lock && lock->IsPersistent()) {
                TDataShardLocksDb locksDb(DataShard, txc);
                lock->SetFrozen(&locksDb);
            }
        }
        writeOp->MarkLocksStored();
        newArtifact = true;
    }
    if (newArtifact)
        writeOp->DbStoreArtifactFlags(txc.DB);

    bool hadWrites = false;
    if (writeOp->IsOutRSStored() || writeOp->IsLocksStored()) {
        // Don't allow immediate writes to corrupt data we have read
        hadWrites |= Pipeline.MarkPlannedLogicallyIncompleteUpTo(TRowVersion(op->GetStep(), op->GetTxId()), txc);
    }

    if (!op->PreparedOutReadSets().empty())
        return EExecutionStatus::DelayCompleteNoMoreRestarts;

    if (newArtifact || hadWrites)
        return EExecutionStatus::ExecutedNoMoreRestarts;

    return EExecutionStatus::Executed;
}

void TStoreAndSendWriteOutRSUnit::Complete(TOperation::TPtr op,
                                      const TActorContext &ctx)
{
    if (!op->PreparedOutReadSets().empty())
        DataShard.SendReadSets(ctx, std::move(op->PreparedOutReadSets()));
}

THolder<TExecutionUnit> CreateStoreAndSendWriteOutRSUnit(TDataShard &dataShard,
                                                    TPipeline &pipeline)
{
    return THolder(new TStoreAndSendWriteOutRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
