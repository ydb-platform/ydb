#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TMoveIndexUnit : public TExecutionUnit {
    TVector<IDataShardChangeCollector::TChange> ChangeRecords;

public:
    TMoveIndexUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::MoveIndex, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    void MoveChangeRecords(NIceDb::TNiceDb& db, const NKikimrTxDataShard::TMoveIndex& move, TVector<IDataShardChangeCollector::TChange>& changeRecords) {
        const auto remapPrevId = PathIdFromPathId(move.GetReMapIndex().GetSrcPathId());
        const auto remapNewId = PathIdFromPathId(move.GetReMapIndex().GetDstPathId());

        for (auto& record: changeRecords) {
            if (record.PathId == remapPrevId) {
                record.PathId = remapNewId;
                if (record.LockId) {
                    DataShard.MoveChangeRecord(db, record.LockId, record.LockOffset, record.PathId);
                } else {
                    DataShard.MoveChangeRecord(db, record.Order, record.PathId);
                }
            }
        }

        for (auto& pr : DataShard.GetLockChangeRecords()) {
            for (auto& record : pr.second.Changes) {
                if (record.PathId == remapPrevId) {
                    record.PathId = remapNewId;
                    DataShard.MoveChangeRecord(db, record.LockId, record.LockOffset, record.PathId);
                }
            }
        }
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        if (tx->GetSchemeTxType() != TSchemaOperation::ETypeMoveIndex) {
            return EExecutionStatus::Executed;
        }

        const auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasMoveIndex()) {
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

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "TMoveIndexUnit Execute"
            << ": schemeTx# " << schemeTx.DebugString()
            << ": changeRecords size# " << ChangeRecords.size()
            << ", at tablet# " << DataShard.TabletID());

        DataShard.SuspendChangeSender(ctx);

        const auto& params = schemeTx.GetMoveIndex();
        DataShard.MoveUserIndex(op, params, ctx, txc);
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

THolder<TExecutionUnit> CreateMoveIndexUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TMoveIndexUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
