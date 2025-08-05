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
        return !op->HasRuntimeConflicts() && !op->HasWaitingForGlobalTxIdFlag();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        if (op->HasWaitingForGlobalTxIdFlag()) {
            return EExecutionStatus::Continue;
        }

        if (op->IsImmediate()) {
            // Every time we execute immediate transaction we may choose a new mvcc version
            op->CachedMvccVersion.reset();
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        TDirectTransaction* tx = dynamic_cast<TDirectTransaction*>(op.Get());
        Y_ENSURE(tx != nullptr);

        try {
            if (!tx->Execute(&DataShard, txc)) {
                return EExecutionStatus::Restart;
            }
        } catch (const TNeedGlobalTxId&) {
            Y_ENSURE(op->GetGlobalTxId() == 0,
                "Unexpected TNeedGlobalTxId exception for direct operation with TxId# " << op->GetGlobalTxId());
            Y_ENSURE(op->IsImmediate(),
                "Unexpected TNeedGlobalTxId exception for a non-immediate operation with TxId# " << op->GetTxId());

            ctx.Send(MakeTxProxyID(),
                new TEvTxUserProxy::TEvAllocateTxId(),
                0, op->GetTxId());
            op->SetWaitingForGlobalTxIdFlag();

            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        }

        if (Pipeline.AddLockDependencies(op, guardLocks)) {
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
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
        DataShard.EmitHeartbeats();

        TDirectTransaction* tx = dynamic_cast<TDirectTransaction*>(op.Get());
        Y_ENSURE(tx != nullptr);

        tx->SendResult(&DataShard, ctx);
    }

}; // TDirectOpUnit

THolder<TExecutionUnit> CreateDirectOpUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TDirectOpUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
