#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TAlterCdcStreamUnit : public TExecutionUnit {
public:
    TAlterCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::AlterCdcStream, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasAlterCdcStreamNotice()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetAlterCdcStreamNotice();
        const auto& streamDesc = params.GetStreamDescription();
        const auto streamPathId = PathIdFromPathId(streamDesc.GetPathId());
        const auto state = streamDesc.GetState();

        const auto pathId = PathIdFromPathId(params.GetPathId());
        Y_ABORT_UNLESS(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ABORT_UNLESS(version);

        TUserTable::TPtr tableInfo;
        switch (state) {
        case NKikimrSchemeOp::ECdcStreamStateDisabled:
            tableInfo = DataShard.AlterTableSwitchCdcStreamState(ctx, txc, pathId, version, streamPathId, state);
            DataShard.GetCdcStreamHeartbeatManager().DropCdcStream(txc.DB, pathId, streamPathId);
            break;

        case NKikimrSchemeOp::ECdcStreamStateReady:
            tableInfo = DataShard.AlterTableSwitchCdcStreamState(ctx, txc, pathId, version, streamPathId, state);

            if (params.HasDropSnapshot()) {
                const auto& snapshot = params.GetDropSnapshot();
                Y_ABORT_UNLESS(snapshot.GetStep() != 0);

                const TSnapshotKey key(pathId, snapshot.GetStep(), snapshot.GetTxId());
                DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);
            } else {
                Y_DEBUG_ABORT("Absent snapshot");
            }

            if (const auto heartbeatInterval = TDuration::MilliSeconds(streamDesc.GetResolvedTimestampsIntervalMs())) {
                DataShard.GetCdcStreamHeartbeatManager().AddCdcStream(txc.DB, pathId, streamPathId, heartbeatInterval);
            }
            break;

        default:
            Y_FAIL_S("Unexpected alter cdc stream"
                << ": params# " << params.ShortDebugString());
        }

        Y_ABORT_UNLESS(tableInfo);
        DataShard.AddUserTable(pathId, tableInfo);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        auto& scanManager = DataShard.GetCdcStreamScanManager();
        scanManager.Forget(txc.DB, pathId, streamPathId);
        if (const auto* info = scanManager.Get(streamPathId)) {
            DataShard.CancelScan(tableInfo->LocalTid, info->ScanId);
            scanManager.Complete(streamPathId);
        }

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
        DataShard.EmitHeartbeats();
    }
};

THolder<TExecutionUnit> CreateAlterCdcStreamUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TAlterCdcStreamUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
