#include "datashard_cdc_stream_common.h"
#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "cdc_schema_change.h"

#include <ydb/core/base/table_index.h>

namespace NKikimr {
namespace NDataShard {

class TCreateCdcStreamUnit : public TCdcStreamUnitBase {
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

        TPathId pathId;
        ui64 schemaVersion = 0;
        TUserTable::TPtr tableInfo;

        if (schemeTx.HasCreateIncrementalBackupSrc()) {
            const auto& backup = schemeTx.GetCreateIncrementalBackupSrc();

            if (backup.HasDropCdcStreamNotice()) {
                const auto& notice = backup.GetDropCdcStreamNotice();
                pathId = TPathId::FromProto(notice.GetPathId());
                schemaVersion = notice.GetTableSchemaVersion();
            } else if (backup.HasCreateCdcStreamNotice()) {
                const auto& notice = backup.GetCreateCdcStreamNotice();
                pathId = TPathId::FromProto(notice.GetPathId());
                schemaVersion = notice.GetTableSchemaVersion();
            } else {
                return EExecutionStatus::Executed;
            }

            Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());
            Y_ENSURE(schemaVersion);

            if (backup.HasDropCdcStreamNotice()) {
                const auto& notice = backup.GetDropCdcStreamNotice();
                Y_VERIFY_S(TPathId::FromProto(notice.GetPathId()) == pathId, "PathId mismatch in DropNotice");

                TVector<TPathId> streamsToDrop;
                for (const auto& protoId : notice.GetStreamPathId()) {
                    streamsToDrop.push_back(TPathId::FromProto(protoId));
                }

                if (!streamsToDrop.empty()) {
                    tableInfo = DataShard.AlterTableDropCdcStreams(ctx, txc, pathId, schemaVersion, streamsToDrop);
                    for (const auto& oldStreamId : streamsToDrop) {
                        DropCdcStream(txc, pathId, oldStreamId, *tableInfo);
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
                const auto streamId = TPathId::FromProto(streamDesc.GetPathId());

                tableInfo = DataShard.AlterTableAddCdcStream(ctx, txc, pathId, schemaVersion, streamDesc);

                AddCdcStream(txc, pathId, streamId, streamDesc,
                    notice.HasSnapshotName() ? notice.GetSnapshotName() : TString(),
                    op->GetStep(), op->GetTxId());
            }
        } else {
            const auto& params = schemeTx.GetCreateCdcStreamNotice();

            pathId = TPathId::FromProto(params.GetPathId());
            schemaVersion = params.GetTableSchemaVersion();

            Y_ENSURE(pathId.OwnerId == DataShard.GetPathOwnerId());
            Y_ENSURE(schemaVersion);

            const auto& streamDesc = params.GetStreamDescription();
            const auto streamPathId = TPathId::FromProto(streamDesc.GetPathId());

            tableInfo = DataShard.AlterTableAddCdcStream(ctx, txc, pathId, schemaVersion, streamDesc);

            AddCdcStream(txc, pathId, streamPathId, streamDesc,
                params.HasSnapshotName() ? params.GetSnapshotName() : TString(),
                op->GetStep(), op->GetTxId());
        }

        Y_ENSURE(tableInfo, "Table info must be initialized by Drop or Create action");

        TDataShardLocksDb locksDb(DataShard, txc);
        DataShard.ReplaceUserTable(pathId, tableInfo, locksDb);

        if (tableInfo->NeedSchemaSnapshots()) {
            DataShard.AddSchemaSnapshot(pathId, schemaVersion, op->GetStep(), op->GetTxId(), txc, ctx);
        }

        if (schemeTx.HasCreateCdcStreamNotice() && AppData()->FeatureFlags.GetEnableFulltextIndexReplication()) {
            const auto& stream = schemeTx.GetCreateCdcStreamNotice().GetStreamDescription();
            if (stream.GetSchemaChanges()) {
                NKikimrSchemeOp::TTableDescription schema;
                tableInfo->GetSchema(schema);
                for (const auto& index : schema.GetTableIndexes()) {
                    const bool rowId = index.GetName() == NTableIndex::NFulltext::RowIdUniqueIndexName
                        && index.GetType() == NKikimrSchemeOp::EIndexTypeGlobalUnique && index.KeyColumnNamesSize() == 1
                        && index.GetKeyColumnNames(0) == NTableIndex::NFulltext::RowIdColumn;
                    if (index.GetState() == NKikimrSchemeOp::EIndexStateReady
                        && (NTableIndex::IsFulltextOrJsonIndex(index.GetType()) || rowId))
                    {
                        // Discovery precedes stream creation. Capture indexes
                        // finalized in between in the new stream's first record.
                        PersistCdcSchemaChange(DataShard, txc, op, pathId, *tableInfo, TPathId::FromProto(stream.GetPathId()));
                        break;
                    }
                }
            }
        }

        BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        TCdcStreamUnitBase::Complete(op, ctx);
        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
    }
};

THolder<TExecutionUnit> CreateCreateCdcStreamUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TCreateCdcStreamUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
