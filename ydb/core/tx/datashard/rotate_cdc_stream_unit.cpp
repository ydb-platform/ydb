#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr::NDataShard {

class TRotateCdcStreamUnit : public TExecutionUnit {
    THolder<TEvChangeExchange::TEvAddSender> AddSender;

public:
    TRotateCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::RotateCdcStream, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto& schemeTx = tx->GetSchemeTx();
        if (!schemeTx.HasRotateCdcStreamNotice()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetRotateCdcStreamNotice();
        const auto& newStreamDesc = params.GetNewStreamDescription();
        const auto oldStreamPathId = TPathId::FromProto(params.GetOldStreamPathId());
        const auto newStreamPathId = TPathId::FromProto(newStreamDesc.GetPathId());

        const auto pathId = TPathId::FromProto(params.GetPathId());
        Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ENSURE(version);

        auto tableInfo = DataShard.AlterTableRotateCdcStream(ctx, txc, pathId, version, oldStreamPathId, newStreamDesc);

        Y_ENSURE(tableInfo);
        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.AddUserTable(pathId, tableInfo, &locksDb);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        auto& scanManager = DataShard.GetCdcStreamScanManager();
        scanManager.Forget(txc.DB, pathId, oldStreamPathId);
        if (const auto* info = scanManager.Get(oldStreamPathId)) {
            DataShard.CancelScan(tableInfo->LocalTid, info->ScanId);
            scanManager.Complete(oldStreamPathId);
        }

        DataShard.GetCdcStreamHeartbeatManager().DropCdcStream(txc.DB, pathId, oldStreamPathId);
        if (newStreamDesc.GetState() == NKikimrSchemeOp::ECdcStreamStateReady) {
            if (const auto heartbeatInterval = TDuration::MilliSeconds(newStreamDesc.GetResolvedTimestampsIntervalMs())) {
                DataShard.GetCdcStreamHeartbeatManager().AddCdcStream(txc.DB, pathId, newStreamPathId, heartbeatInterval);
            }
        }

        AddSender.Reset(new TEvChangeExchange::TEvAddSender(
            pathId, TEvChangeExchange::ESenderType::CdcStream, newStreamPathId
        ));

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr, const TActorContext& ctx) override {
        if (AddSender) {
            ctx.Send(DataShard.GetChangeSender(), AddSender.Release());
        }
        DataShard.EmitHeartbeats();
    }
};

THolder<TExecutionUnit> CreateRotateCdcStreamUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TRotateCdcStreamUnit(self, pipeline));
}

} // namespace NKikimr::NDataShard
