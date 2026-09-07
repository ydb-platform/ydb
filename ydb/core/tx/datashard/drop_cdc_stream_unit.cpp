#include "datashard_cdc_stream_common.h"
#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TDropCdcStreamUnit : public TCdcStreamUnitBase {
public:
    TDropCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TCdcStreamUnitBase(EExecutionUnitKind::DropCdcStream, false, self, pipeline)
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
        if (!schemeTx.HasDropCdcStreamNotice()) {
            return EExecutionStatus::Executed;
        }

        const auto& params = schemeTx.GetDropCdcStreamNotice();

        const auto pathId = TPathId::FromProto(params.GetPathId());
        Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());

        // Collect stream IDs to drop - works for both single and multiple
        TVector<TPathId> streamPathIds;
        for (const auto& streamId : params.GetStreamPathId()) {
            streamPathIds.push_back(TPathId::FromProto(streamId));
        }

        const auto version = params.GetTableSchemaVersion();
        Y_ENSURE(version);

        TUserTable::TPtr tableInfo;
        tableInfo = DataShard.AlterTableDropCdcStreams(ctx, txc, pathId, version, streamPathIds);

        for (const auto& streamPathId : streamPathIds) {
            DropCdcStream(txc, pathId, streamPathId, *tableInfo);
        }

        // Update table info once after processing all streams
        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.AddUserTable(pathId, tableInfo, &locksDb);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        if (params.HasDropSnapshot()) {
            const auto& snapshot = params.GetDropSnapshot();
            Y_ENSURE(snapshot.GetStep() != 0);

            const TSnapshotKey key(pathId, snapshot.GetStep(), snapshot.GetTxId());
            DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);
        }

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }
};

THolder<TExecutionUnit> CreateDropCdcStreamUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TDropCdcStreamUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
