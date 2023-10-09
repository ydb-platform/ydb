#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCheckSnapshotTxUnit : public TExecutionUnit {
public:
    TCheckSnapshotTxUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CheckSnapshotTx, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    bool CheckTable(TOperation::TPtr op, ui64 ownerId, ui64 pathId) {
        // FIXME: tables must use proper owner id when addressed
        if (ownerId != DataShard.GetPathOwnerId() ||
            !DataShard.GetUserTables().contains(pathId))
        {
            BuildResult(op)->AddError(
                    NKikimrTxDataShard::TError::SCHEME_ERROR,
                    TStringBuilder()
                        << "Shard " << DataShard.TabletID()
                        << " has no table " << ownerId << ":" << pathId);

            op->Abort(EExecutionUnitKind::FinishPropose);

            return false;
        }

        return true;
    }

    bool CheckSnapshotRemove(TOperation::TPtr op, const TSnapshotKey& key) {
        const auto* snapshot = DataShard.GetSnapshotManager().FindAvailable(key);
        if (!snapshot) {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)->AddError(
                    NKikimrTxDataShard::TError::SNAPSHOT_NOT_EXIST,
                    TStringBuilder()
                        << "Shard " << DataShard.TabletID()
                        << " has no snapshot " << key);

            op->Abort(EExecutionUnitKind::FinishPropose);

            return false;
        }

        if (snapshot->HasFlags(TSnapshot::FlagScheme)) {
            BuildResult(op)->AddError(
                    NKikimrTxDataShard::TError::BAD_ARGUMENT,
                    TStringBuilder()
                        << "Shard " << DataShard.TabletID()
                        << " has snapshot " << key
                        << " marked as a scheme snapshot");

            op->Abort(EExecutionUnitKind::FinishPropose);

            return false;
        }

        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext&, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsSnapshotTx());
        Y_ABORT_UNLESS(!op->IsAborted());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        if (CheckRejectDataTx(op, ctx)) {
            op->Abort(EExecutionUnitKind::FinishPropose);

            return EExecutionStatus::Executed;
        }

        const auto& txBody = tx->GetSnapshotTx();

        size_t count = (
            (size_t)txBody.HasCreateVolatileSnapshot() +
            (size_t)txBody.HasDropVolatileSnapshot());

        if (count != 1) {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)->AddError(
                    NKikimrTxDataShard::TError::BAD_ARGUMENT,
                    "Snapshot tx cannot have multiple conflicting operations");

            op->Abort(EExecutionUnitKind::FinishPropose);

            return EExecutionStatus::Executed;
        }

        if (txBody.HasCreateVolatileSnapshot()) {
            const auto& params = txBody.GetCreateVolatileSnapshot();

            // TODO: may need to support multiple local tables
            const ui64 ownerId = params.GetOwnerId();
            const ui64 pathId = params.GetPathId();

            if (!CheckTable(op, ownerId, pathId)) {
                return EExecutionStatus::Executed;
            }

            if (op->IsImmediate()) {
                BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::BAD_REQUEST)->AddError(
                        NKikimrTxDataShard::TError::BAD_ARGUMENT,
                        "CreateVolatileSnapshot cannot be an immediate transaction");

                op->Abort(EExecutionUnitKind::FinishPropose);

                return EExecutionStatus::Executed;
            }
        }

        if (txBody.HasDropVolatileSnapshot()) {
            const auto& params = txBody.GetDropVolatileSnapshot();

            // TODO: may need to support multiple local tables
            const ui64 ownerId = params.GetOwnerId();
            const ui64 pathId = params.GetPathId();

            if (!CheckTable(op, ownerId, pathId)) {
                return EExecutionStatus::Executed;
            }

            const TSnapshotKey key(ownerId, pathId, params.GetStep(), params.GetTxId());

            if (!CheckSnapshotRemove(op, key)) {
                return EExecutionStatus::Executed;
            }
        }

        if (!op->IsImmediate()) {
            if (!Pipeline.AssignPlanInterval(op)) {
                TString err = TStringBuilder()
                    << "Can't propose tx " << op->GetTxId() << " at blocked shard "
                    << DataShard.TabletID();
                BuildResult(op)->AddError(NKikimrTxDataShard::TError::SHARD_IS_BLOCKED, err);
                op->Abort(EExecutionUnitKind::FinishPropose);

                LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);

                return EExecutionStatus::Executed;
            }

            BuildResult(op)->SetPrepared(op->GetMinStep(), op->GetMaxStep(), op->GetReceivedAt());

            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
                        "Prepared " << op->GetKind() << " transaction txId " << op->GetTxId()
                        << " at tablet " << DataShard.TabletID());
        }

        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        // nothing
    }
};

THolder<TExecutionUnit> CreateCheckSnapshotTxUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TCheckSnapshotTxUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
