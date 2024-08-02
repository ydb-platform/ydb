#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TMoveTableUnit : public TExecutionUnit {
    TVector<IDataShardChangeCollector::TChange> ChangeRecords;

public:
    TMoveTableUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::MoveTable, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    void MoveChangeRecords(NIceDb::TNiceDb& db, const NKikimrTxDataShard::TMoveTable& move, TVector<IDataShardChangeCollector::TChange>& changeRecords) {
        const THashMap<TPathId, TPathId> remap = DataShard.GetRemapIndexes(move);

        for (auto& record: changeRecords) {
            // We skip records for deleted indexes
            if (remap.contains(record.PathId)) {
                record.PathId = remap.at(record.PathId);
                if (record.LockId) {
                    DataShard.MoveChangeRecord(db, record.LockId, record.LockOffset, record.PathId);
                } else {
                    DataShard.MoveChangeRecord(db, record.Order, record.PathId);
                }
            }
        }

        for (auto& pr : DataShard.GetLockChangeRecords()) {
            for (auto& record : pr.second.Changes) {
                if (remap.contains(record.PathId)) {
                    record.PathId = remap.at(record.PathId);
                    DataShard.MoveChangeRecord(db, record.LockId, record.LockOffset, record.PathId);
                }
            }
        }
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        if (tx->GetSchemeTxType() != TSchemaOperation::ETypeMoveTable) {
            return EExecutionStatus::Executed;
        }

        const auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasMoveTable()) {
            return EExecutionStatus::Executed;
        }

        NIceDb::TNiceDb db(txc.DB);

        ChangeRecords.clear();

        auto changesQueue = DataShard.TakeChangesQueue();
        auto lockChangeRecords = DataShard.TakeLockChangeRecords();
        auto committedLockChangeRecords = DataShard.TakeCommittedLockChangeRecords();

        if (!DataShard.LoadChangeRecords(db, ChangeRecords)) {
            DataShard.SetChangesQueue(std::move(changesQueue));
            DataShard.SetLockChangeRecords(std::move(lockChangeRecords));
            DataShard.SetCommittedLockChangeRecords(std::move(committedLockChangeRecords));
            return EExecutionStatus::Restart;
        }

        if (!DataShard.LoadLockChangeRecords(db)) {
            DataShard.SetChangesQueue(std::move(changesQueue));
            DataShard.SetLockChangeRecords(std::move(lockChangeRecords));
            DataShard.SetCommittedLockChangeRecords(std::move(committedLockChangeRecords));
            return EExecutionStatus::Restart;
        }

        if (!DataShard.LoadChangeRecordCommits(db, ChangeRecords)) {
            DataShard.SetChangesQueue(std::move(changesQueue));
            DataShard.SetLockChangeRecords(std::move(lockChangeRecords));
            DataShard.SetCommittedLockChangeRecords(std::move(committedLockChangeRecords));
            return EExecutionStatus::Restart;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "TMoveTableUnit Execute"
            << ": schemeTx# " << schemeTx.DebugString()
            << ": changeRecords size# " << ChangeRecords.size()
            << ", at tablet# " << DataShard.TabletID());

        DataShard.SuspendChangeSender(ctx);

        const auto& params = schemeTx.GetMoveTable();
        DataShard.MoveUserTable(op, params, ctx, txc);
        MoveChangeRecords(db, params, ChangeRecords);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr, const TActorContext& ctx) override {
        DataShard.CreateChangeSender(ctx);
        DataShard.MaybeActivateChangeSender(ctx);
        DataShard.EnqueueChangeRecords(std::move(ChangeRecords), 0, true);
    }
};

THolder<TExecutionUnit> CreateMoveTableUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TMoveTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
