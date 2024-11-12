#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCreateCdcStreamUnit : public TExecutionUnit {
    THolder<TEvChangeExchange::TEvAddSender> AddSender;

public:
    TCreateCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreateCdcStream, false, self, pipeline)
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
        if (!schemeTx.HasCreateCdcStreamNotice()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetCreateCdcStreamNotice();
        const auto& streamDesc = params.GetStreamDescription();
        const auto streamPathId = PathIdFromPathId(streamDesc.GetPathId());

        const auto pathId = PathIdFromPathId(params.GetPathId());
        Y_ABORT_UNLESS(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ABORT_UNLESS(version);

        auto tableInfo = DataShard.AlterTableAddCdcStream(ctx, txc, pathId, version, streamDesc);
        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.AddUserTable(pathId, tableInfo, &locksDb);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        if (params.HasSnapshotName()) {
            Y_ABORT_UNLESS(streamDesc.GetState() == NKikimrSchemeOp::ECdcStreamStateScan);
            Y_ABORT_UNLESS(tx->GetStep() != 0);

            DataShard.GetSnapshotManager().AddSnapshot(txc.DB,
                TSnapshotKey(pathId, tx->GetStep(), tx->GetTxId()),
                params.GetSnapshotName(), TSnapshot::FlagScheme, TDuration::Zero());

            DataShard.GetCdcStreamScanManager().Add(txc.DB,
                pathId, streamPathId, TRowVersion(tx->GetStep(), tx->GetTxId()));
        }

        if (streamDesc.GetState() == NKikimrSchemeOp::ECdcStreamStateReady) {
            if (const auto heartbeatInterval = TDuration::MilliSeconds(streamDesc.GetResolvedTimestampsIntervalMs())) {
                DataShard.GetCdcStreamHeartbeatManager().AddCdcStream(txc.DB, pathId, streamPathId, heartbeatInterval);
            }
        }

        AddSender.Reset(new TEvChangeExchange::TEvAddSender(
            pathId, TEvChangeExchange::ESenderType::CdcStream, streamPathId
        ));

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr, const TActorContext& ctx) override {
        if (AddSender) {
            ctx.Send(DataShard.GetChangeSender(), AddSender.Release());
            DataShard.EmitHeartbeats();
        }
    }
};

THolder<TExecutionUnit> CreateCreateCdcStreamUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TCreateCdcStreamUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
