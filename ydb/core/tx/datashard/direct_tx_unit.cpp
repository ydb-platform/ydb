#include "datashard_direct_transaction.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"

namespace NKikimr {
namespace NDataShard {

class TDirectOpUnit : public TExecutionUnit {
public:
    TDirectOpUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::DirectOp, true, self, pipeline)
    {
    }

    ~TDirectOpUnit()
    {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        return !op->HasRuntimeConflicts();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        if (op->IsImmediate()) {
            // Every time we execute immediate transaction we may choose a new mvcc version
            op->MvccReadWriteVersion.reset();
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        TDirectTransaction* tx = dynamic_cast<TDirectTransaction*>(op.Get());
        Y_VERIFY(tx != nullptr);

        if (!tx->Execute(&DataShard, txc)) {
            return EExecutionStatus::Restart;
        }

        if (Pipeline.AddLockDependencies(op, guardLocks)) {
            txc.Reschedule();
            return EExecutionStatus::Restart;
        }

        op->ChangeRecords() = std::move(tx->GetCollectedChanges());

        DataShard.SysLocksTable().ApplyLocks();
        DataShard.SubscribeNewLocks(ctx);
        Pipeline.AddCommittingOp(op);

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Pipeline.RemoveCommittingOp(op);
        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));

        TDirectTransaction* tx = dynamic_cast<TDirectTransaction*>(op.Get());
        Y_VERIFY(tx != nullptr);

        tx->SendResult(&DataShard, ctx);
    }

}; // TDirectOpUnit

THolder<TExecutionUnit> CreateDirectOpUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TDirectOpUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
