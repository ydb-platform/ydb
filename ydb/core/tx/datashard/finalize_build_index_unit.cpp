#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TFinalizeBuildIndexUnit : public TExecutionUnit {
    THolder<TEvChangeExchange::TEvRemoveSender> RemoveSender;

public:
    TFinalizeBuildIndexUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::FinalizeBuildIndex, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasFinalizeBuildIndex()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetFinalizeBuildIndex();

        const auto pathId = PathIdFromPathId(params.GetPathId());
        Y_ABORT_UNLESS(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ABORT_UNLESS(version);

        TUserTable::TPtr tableInfo;
        if (params.HasOutcome() && params.GetOutcome().HasApply()) {
            const auto indexPathId = PathIdFromPathId(params.GetOutcome().GetApply().GetIndexPathId());

            tableInfo = DataShard.AlterTableSwitchIndexState(ctx, txc, pathId, version, indexPathId, NKikimrSchemeOp::EIndexStateReady);
        } else if (params.HasOutcome() && params.GetOutcome().HasCancel()) {
            const auto indexPathId = PathIdFromPathId(params.GetOutcome().GetCancel().GetIndexPathId());

            const auto& userTables = DataShard.GetUserTables();
            Y_ABORT_UNLESS(userTables.contains(pathId.LocalPathId));
            userTables.at(pathId.LocalPathId)->ForAsyncIndex(indexPathId, [&](const auto&) {
                RemoveSender.Reset(new TEvChangeExchange::TEvRemoveSender(indexPathId));
            });

            tableInfo = DataShard.AlterTableDropIndex(ctx, txc, pathId, version, indexPathId);
        } else {
            tableInfo = DataShard.AlterTableSchemaVersion(ctx, txc, pathId, version);
        }

        Y_ABORT_UNLESS(tableInfo);
        DataShard.AddUserTable(pathId, tableInfo);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        ui64 step = params.GetSnapshotStep();
        ui64 txId = params.GetSnapshotTxId();
        Y_ABORT_UNLESS(step != 0);

        if (const auto* record = DataShard.GetScanManager().Get(params.GetBuildIndexId())) {
            DataShard.CancelScan(tableInfo->LocalTid, record->ScanId);
            DataShard.GetScanManager().Drop(params.GetBuildIndexId());
        }

        const TSnapshotKey key(pathId, step, txId);
        DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr, const TActorContext& ctx) override {
        if (RemoveSender) {
            ctx.Send(DataShard.GetChangeSender(), RemoveSender.Release());
        }
    }
};

THolder<TExecutionUnit> CreateFinalizeBuildIndexUnit(
    TDataShard& dataShard,
    TPipeline& pipeline)
{
    return THolder(new TFinalizeBuildIndexUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
