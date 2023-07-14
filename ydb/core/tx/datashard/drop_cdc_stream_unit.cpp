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

        const auto pathId = PathIdFromPathId(params.GetPathId());
        Y_VERIFY(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto streamPathId = PathIdFromPathId(params.GetStreamPathId());

        const auto version = params.GetTableSchemaVersion();
        Y_VERIFY(version);

        auto tableInfo = DataShard.AlterTableDropCdcStream(ctx, txc, pathId, version, streamPathId);
        DataShard.AddUserTable(pathId, tableInfo);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        if (params.HasDropSnapshot()) {
            const auto& snapshot = params.GetDropSnapshot();
            Y_VERIFY(snapshot.GetStep() != 0);

            const TSnapshotKey key(pathId, snapshot.GetStep(), snapshot.GetTxId());
            DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);
        }

        auto& scanManager = DataShard.GetCdcStreamScanManager();
        scanManager.Forget(txc.DB, pathId, streamPathId);
        if (const auto* info = scanManager.Get(streamPathId)) {
            DataShard.CancelScan(tableInfo->LocalTid, info->ScanId);
            scanManager.Complete(streamPathId);
        }

        DataShard.GetCdcStreamHeartbeatManager().DropCdcStream(txc.DB, pathId, streamPathId);

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
