#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCreateCdcStreamUnit : public TExecutionUnit {
    THolder<TEvChangeExchange::TEvAddSender> AddSender;
    // Заменили одиночный Holder на вектор, так как DropNotice может содержать несколько стримов
    TVector<THolder<TEvChangeExchange::TEvRemoveSender>> RemoveSenders; 

public:
    TCreateCdcStreamUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreateCdcStream, false, self, pipeline)
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

        // Логика для инкрементального бэкапа (CopyTable + CDC)
        if (schemeTx.HasCreateIncrementalBackupSrc()) {
            const auto& backup = schemeTx.GetCreateIncrementalBackupSrc();
            
            // Определяем PathId таблицы (он должен быть одинаковым и в Drop, и в Create)
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

            // 1. Обработка DROP (удаление старых стримов)
            if (backup.HasDropCdcStreamNotice()) {
                const auto& notice = backup.GetDropCdcStreamNotice();
                // Проверка на совпадение версий и ID, если вдруг пришли разные (не должно случаться)
                Y_VERIFY_S(TPathId::FromProto(notice.GetPathId()) == pathId, "PathId mismatch in DropNotice");
                
                TVector<TPathId> streamsToDrop;
                for (const auto& protoId : notice.GetStreamPathId()) {
                    streamsToDrop.push_back(TPathId::FromProto(protoId));
                }

                if (!streamsToDrop.empty()) {
                    tableInfo = DataShard.AlterTableDropCdcStreams(ctx, txc, pathId, schemaVersion, streamsToDrop);

                    for (const auto& oldStreamPathId : streamsToDrop) {
                        auto& scanManager = DataShard.GetCdcStreamScanManager();
                        scanManager.Forget(txc.DB, pathId, oldStreamPathId);
                        
                        if (const auto* info = scanManager.Get(oldStreamPathId)) {
                            // Если скан активен, отменяем его
                            if (tableInfo) {
                                DataShard.CancelScan(tableInfo->LocalTid, info->ScanId);
                            }
                            scanManager.Complete(oldStreamPathId);
                        }

                        DataShard.GetCdcStreamHeartbeatManager().DropCdcStream(txc.DB, pathId, oldStreamPathId);
                        RemoveSenders.emplace_back(new TEvChangeExchange::TEvRemoveSender(oldStreamPathId));
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

            // 2. Обработка CREATE (добавление нового стрима)
            if (backup.HasCreateCdcStreamNotice()) {
                const auto& notice = backup.GetCreateCdcStreamNotice();
                Y_VERIFY_S(TPathId::FromProto(notice.GetPathId()) == pathId, "PathId mismatch in CreateNotice");

                const auto& streamDesc = notice.GetStreamDescription();
                const auto streamPathId = TPathId::FromProto(streamDesc.GetPathId());

                // Этот вызов вернет актуальный tableInfo (включающий изменения от Drop, если он был)
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

            // Финализация
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

        // Стандартная логика для обычного CreateCdcStream (не бэкап)
        const auto& params =
            schemeTx.HasCreateCdcStreamNotice() ?
            schemeTx.GetCreateCdcStreamNotice() :
            schemeTx.GetCreateIncrementalBackupSrc().GetCreateCdcStreamNotice(); // Fallback, хотя выше обработано
            
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

    void Complete(TOperation::TPtr, const TActorContext& ctx) override {
        // Отправляем события удаления (может быть несколько)
        if (!RemoveSenders.empty()) {
            const auto& changeSender = DataShard.GetChangeSender();
            if (changeSender) {
                for (auto& holder : RemoveSenders) {
                    ctx.Send(changeSender, holder.Release());
                }
            }
            RemoveSenders.clear();
        }

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
