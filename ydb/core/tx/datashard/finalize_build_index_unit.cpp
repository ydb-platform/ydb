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
        Y_VERIFY(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasFinalizeBuildIndex()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetFinalizeBuildIndex();

        const auto pathId = PathIdFromPathId(params.GetPathId());
        Y_VERIFY(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_VERIFY(version);

        TUserTable::TPtr tableInfo;
        if (params.HasOutcome() && params.GetOutcome().HasCancel()) {
            const auto& userTables = DataShard.GetUserTables();
            Y_VERIFY(userTables.contains(pathId.LocalPathId));
            const auto& indexes = userTables.at(pathId.LocalPathId)->Indexes;

            const auto indexPathId = PathIdFromPathId(params.GetOutcome().GetCancel().GetIndexPathId());
            auto it = indexes.find(indexPathId);

            if (it != indexes.end() && it->second.Type == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync) {
                RemoveSender.Reset(new TEvChangeExchange::TEvRemoveSender(indexPathId));
            }

            tableInfo = DataShard.AlterTableDropIndex(ctx, txc, pathId, version, indexPathId);
        } else {
            tableInfo = DataShard.AlterTableSchemaVersion(ctx, txc, pathId, version);
        }

        Y_VERIFY(tableInfo);
        DataShard.AddUserTable(pathId, tableInfo);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        ui64 step = params.GetSnapshotStep();
        ui64 txId = params.GetSnapshotTxId();
        Y_VERIFY(step != 0);

        if (DataShard.GetBuildIndexManager().Contains(params.GetBuildIndexId())) {
            auto  record = DataShard.GetBuildIndexManager().Get(params.GetBuildIndexId());
            DataShard.CancelScan(tableInfo->LocalTid, record.ScanId);
            DataShard.GetBuildIndexManager().Drop(params.GetBuildIndexId());
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
