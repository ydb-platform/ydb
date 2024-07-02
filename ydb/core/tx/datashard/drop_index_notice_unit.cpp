#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TDropIndexNoticeUnit : public TExecutionUnit {
    THolder<TEvChangeExchange::TEvRemoveSender> RemoveSender;

public:
    TDropIndexNoticeUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::DropIndexNotice, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasDropIndexNotice()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetDropIndexNotice();

        const auto pathId = PathIdFromPathId(params.GetPathId());
        Y_ABORT_UNLESS(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ABORT_UNLESS(version);

        TUserTable::TPtr tableInfo;
        if (params.HasIndexPathId()) {
            const auto indexPathId = PathIdFromPathId(params.GetIndexPathId());

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

THolder<TExecutionUnit> CreateDropIndexNoticeUnit(
    TDataShard& dataShard,
    TPipeline& pipeline)
{
    return THolder(new TDropIndexNoticeUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
