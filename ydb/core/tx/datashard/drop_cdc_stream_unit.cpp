#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TDropCdcStreamUnit : public TExecutionUnit {
    THolder<TEvChangeExchange::TEvRemoveSender> RemoveSender;

public:
    TDropCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::DropCdcStream, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_VERIFY(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasDropCdcStreamNotice()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetDropCdcStreamNotice();

        const auto pathId = TPathId(params.GetPathId().GetOwnerId(), params.GetPathId().GetLocalId());
        Y_VERIFY(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto streamPathId = TPathId(params.GetStreamPathId().GetOwnerId(), params.GetStreamPathId().GetLocalId());

        const auto version = params.GetTableSchemaVersion();
        Y_VERIFY(version);

        auto tableInfo = DataShard.AlterTableDropCdcStream(ctx, txc, pathId, version, streamPathId);
        DataShard.AddUserTable(pathId, tableInfo);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        RemoveSender.Reset(new TEvChangeExchange::TEvRemoveSender(streamPathId));

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

THolder<TExecutionUnit> CreateDropCdcStreamUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TDropCdcStreamUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
