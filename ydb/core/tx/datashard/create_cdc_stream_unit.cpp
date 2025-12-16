#include "datashard_cdc_stream_common.h"
#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCreateCdcStreamUnit : public TCdcStreamUnitBase {
    THolder<TEvChangeExchange::TEvAddSender> AddSender;

public:
    TCreateCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TCdcStreamUnitBase(EExecutionUnitKind::CreateCdcStream, false, self, pipeline)
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
        if (!schemeTx.HasCreateCdcStreamNotice() && !schemeTx.HasCreateIncrementalBackupSrc()) {
            return EExecutionStatus::Executed;
        }

        if (schemeTx.HasCreateIncrementalBackupSrc()) {
            const auto& backup = schemeTx.GetCreateIncrementalBackupSrc();
            
            TPathId pathId;
            ui64 schemaVersion = 0;
            bool hasWork = false;

            if (backup.HasDropCdcStreamNotice()) {
                const auto& notice = backup.GetDropCdcStreamNotice();
                pathId = TPathId::FromProto(notice.GetPathId());
                schemaVersion = notice.GetTableSchemaVersion();
                hasWork = true;
            } else if (backup.HasCreateCdcStreamNotice()) {
                const auto& notice = backup.GetCreateCdcStreamNotice();
                pathId = TPathId::FromProto(notice.GetPathId());
                schemaVersion = notice.GetTableSchemaVersion();
                hasWork = true;
            }

            if (!hasWork) {
                return EExecutionStatus::Executed;
            }

            Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());
            Y_ENSURE(schemaVersion);

            TUserTable::TPtr tableInfo;

            if (backup.HasDropCdcStreamNotice()) {
                const auto& notice = backup.GetDropCdcStreamNotice();
                Y_VERIFY_S(TPathId::FromProto(notice.GetPathId()) == pathId, "PathId mismatch in DropNotice");
                
                TVector<TPathId> streamsToDrop;
                for (const auto& protoId : notice.GetStreamPathId()) {
                    streamsToDrop.push_back(TPathId::FromProto(protoId));
                }

                if (!streamsToDrop.empty()) {
                    tableInfo = DataShard.AlterTableDropCdcStreams(ctx, txc, pathId, schemaVersion, streamsToDrop);

                    for (const auto& oldStreamPathId : streamsToDrop) {
                        StopCdcStream(txc, pathId, oldStreamPathId, tableInfo);
                    }
                }

                if (notice.HasDropSnapshot()) {
                    const auto& snapshot = notice.GetDropSnapshot();
                    if (snapshot.GetStep() != 0) {
                        const TSnapshotKey key(pathId, snapshot.GetStep(), snapshot.GetTxId());
                        DataShard.GetSnapshotManager().RemoveSnapshot(txc.DB, key);
                    }
                }
            }

            if (backup.HasCreateCdcStreamNotice()) {
                const auto& notice = backup.GetCreateCdcStreamNotice();
                Y_VERIFY_S(TPathId::FromProto(notice.GetPathId()) == pathId, "PathId mismatch in CreateNotice");

                const auto& streamDesc = notice.GetStreamDescription();
                const auto streamPathId = TPathId::FromProto(streamDesc.GetPathId());

                tableInfo = DataShard.AlterTableAddCdcStream(ctx, txc, pathId, schemaVersion, streamDesc);

                if (notice.HasSnapshotName()) {
                    Y_ENSURE(streamDesc.GetState() == NKikimrSchemeOp::ECdcStreamStateScan);
                    Y_ENSURE(tx->GetStep() != 0);

                    DataShard.GetSnapshotManager().AddSnapshot(txc.DB,
                        TSnapshotKey(pathId, tx->GetStep(), tx->GetTxId()),
                        notice.GetSnapshotName(), TSnapshot::FlagScheme, TDuration::Zero());

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
            }

            Y_ENSURE(tableInfo, "Table info must be initialized by Drop or Create action");

            TDataShardLocksDb locksDb(DataShard, txc);
            DataShard.AddUserTable(pathId, tableInfo, &locksDb);

            if (tableInfo->NeedSchemaSnapshots()) {
                DataShard.AddSchemaSnapshot(pathId, schemaVersion, op->GetStep(), op->GetTxId(), txc, ctx);
            }

            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
            op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

            return EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        const auto& params =
            schemeTx.HasCreateCdcStreamNotice() ?
            schemeTx.GetCreateCdcStreamNotice() :
            schemeTx.GetCreateIncrementalBackupSrc().GetCreateCdcStreamNotice();
            
        const auto& streamDesc = params.GetStreamDescription();
        const auto streamPathId = TPathId::FromProto(streamDesc.GetPathId());

        const auto pathId = TPathId::FromProto(params.GetPathId());
        Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());

        const auto version = params.GetTableSchemaVersion();
        Y_ENSURE(version);

        auto tableInfo = DataShard.AlterTableAddCdcStream(ctx, txc, pathId, version, streamDesc);
        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.AddUserTable(pathId, tableInfo, &locksDb);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, version, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        if (params.HasSnapshotName()) {
            Y_ENSURE(streamDesc.GetState() == NKikimrSchemeOp::ECdcStreamStateScan);
            Y_ENSURE(tx->GetStep() != 0);

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

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        TCdcStreamUnitBase::Complete(op, ctx);

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
