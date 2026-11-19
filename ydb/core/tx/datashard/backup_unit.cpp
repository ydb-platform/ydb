#include "backup_restore_common.h"
#include "backup_restore_traits.h"
#include "execution_unit_ctors.h"
#include "export_iface.h"
#include "export_scan.h"
#include "export_s3.h"

#include <ydb/core/backup/common/table_checksum_snapshot.h>
#include <ydb/core/protos/datashard_config.pb.h>

namespace NKikimr {
namespace NDataShard {

class TBackupUnit : public TBackupRestoreUnitBase<TEvDataShard::TEvCancelBackup> {
    using IBuffer = NExportScan::IBuffer;

    bool IsExportChecksumSnapshot(const TSnapshot* snapshot) const {
        if (!snapshot || !snapshot->HasFlags(TSnapshot::FlagScheme | TSnapshot::FlagTimeout)) {
            return false;
        }
        const auto owner = NBackup::TTableChecksumSnapshotOwner::Parse(snapshot->Name);
        return owner && !owner->Importing && owner->SchemeShardId == DataShard.GetCurrentSchemeShardId();
    }

    bool IsBackupSnapshotReadable(const TSnapshotKey& key, ui32 localTableId, NTable::TDatabase& db) const {
        const TRowVersion version(key.Step, key.TxId);
        if (!version.Step || version.Step == Max<ui64>() || !version.TxId) {
            return false;
        }
        const auto& snapshots = DataShard.GetSnapshotManager();
        if (!snapshots.FindAvailable(key) &&
            !snapshots.IsSnapshotReadable(db, TTableId(key.OwnerId, key.PathId), version)) {
            return false;
        }
        return !db.GetRemovedRowVersions(localTableId).Contains(version, version.Next());
    }

protected:
    bool IsRelevant(TActiveTransaction* tx) const override {
        return tx->GetSchemeTx().HasBackup();
    }

    bool IsWaiting(TOperation::TPtr op) const override {
        return op->IsWaitingForScan() || op->IsWaitingForRestart();
    }

    void SetWaiting(TOperation::TPtr op) override {
        op->SetWaitingForScanFlag();
    }

    void ResetWaiting(TOperation::TPtr op) override {
        op->ResetWaitingForScanFlag();
        op->ResetWaitingForRestartFlag();
    }

    bool Run(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        Y_ENSURE(tx->GetSchemeTx().HasBackup());
        const auto& backup = tx->GetSchemeTx().GetBackup();

        const ui64 tableId = backup.GetTableId();
        Y_ENSURE(DataShard.GetUserTables().contains(tableId));

        const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;
        Y_ENSURE(txc.DB.GetScheme().GetTableInfo(localTableId));

        const TRowVersion snapshot(backup.GetSnapshotStep(), backup.GetSnapshotTxId());
        NKikimrSchemeOp::TTableDescription schema;
        DataShard.GetUserTables().at(tableId)->GetSchema(schema);
        const TSnapshotKey snapshotKey(DataShard.GetPathOwnerId(), tableId, snapshot.Step, snapshot.TxId);
        const bool hasChecksumResult = backup.HasTableChecksum() && (backup.GetTableChecksum().HasCount() ||
                                                                        backup.GetTableChecksum().HasErrorCode());
        const bool useSnapshot = schema.HasTableChecksumSnapshot() || hasChecksumResult ||
                                 IsExportChecksumSnapshot(DataShard.GetSnapshotManager().FindAvailable(snapshotKey));
        if (useSnapshot && !IsBackupSnapshotReadable(snapshotKey, localTableId, txc.DB)) {
            const TString error = "Retained export snapshot is no longer available on the table copy";
            auto* schemeOp = DataShard.FindSchemaTx(op->GetTxId());
            Y_ENSURE(schemeOp);
            schemeOp->Success = false;
            schemeOp->Error = error;
            NIceDb::TNiceDb db(txc.DB);
            DataShard.PersistSchemeTxResult(db, *schemeOp);
            Abort(op, ctx, error);
            return false;
        }

        auto* appData = AppData(ctx);
        const auto& columns = DataShard.GetUserTables().at(tableId)->Columns;
        std::shared_ptr<::NKikimr::NDataShard::IExport> exp;

        if (backup.HasYTSettings()) {
            if (backup.HasCompression()) {
                Abort(op, ctx, "Exports to YT do not support compression");
                return false;
            }

            if (auto* exportFactory = appData->DataShardExportFactory) {
                std::shared_ptr<IExport>(exportFactory->CreateExportToYt(backup, columns)).swap(exp);
                if (!exp) {
                    Abort(op, ctx, "Failed to create YT export");
                    return false;
                }
            } else {
                Abort(op, ctx, "Exports to YT are disabled");
                return false;
            }
        } else if (backup.HasS3Settings() || backup.HasFSSettings()) {
            NBackupRestoreTraits::ECompressionCodec codec;
            if (!TryCodecFromTask(backup, codec)) {
                Abort(op, ctx, TStringBuilder() << "Unsupported compression codec"
                    << ": " << backup.GetCompression().GetCodec());
                return false;
            }

            if (NBackupRestoreTraits::DataFormatFromTask(backup) == NBackupRestoreTraits::EDataFormat::Parquet) {
                if (!appData->FeatureFlags.GetEnableExportInParquet()) {
                    Abort(op, ctx, "Parquet export is disabled by feature flag EnableExportInParquet");
                    return false;
                }
                if (backup.HasEncryptionSettings()) {
                    Abort(op, ctx, "Encryption is not supported for parquet files");
                    return false;
                }
            }

            const TStringBuf exportKind = backup.HasFSSettings() ? "FS"sv : "S3"sv;
            if (auto* exportFactory = appData->DataShardExportFactory) {
                std::shared_ptr<IExport>(exportFactory->CreateExportToS3(backup, columns)).swap(exp);
                if (!exp) {
                    Abort(op, ctx, TStringBuilder() << "Failed to create " << exportKind << " export");
                    return false;
                }
            } else {
                Abort(op, ctx, TStringBuilder() << "Exports to " << exportKind << " are disabled");
                return false;
            }
        } else {
            Abort(op, ctx, "Unsupported backup task");
            return false;
        }

        auto createUploader = [self = DataShard.SelfId(), txId = op->GetTxId(), exp]() {
            return exp->CreateUploader(self, txId);
        };

        THolder<IBuffer> buffer{exp->CreateBuffer()};
        THolder<NTable::IScan> scan{CreateExportScan(std::move(buffer), createUploader)};

        const auto& taskName = appData->DataShardConfig.GetBackupTaskName();
        const auto taskPrio = appData->DataShardConfig.GetBackupTaskPriority();

        ui64 readAheadLo = appData->DataShardConfig.GetBackupReadAheadLo();
        if (ui64 readAheadLoOverride = DataShard.GetBackupReadAheadLoOverride(); readAheadLoOverride > 0) {
            readAheadLo = readAheadLoOverride;
        }

        ui64 readAheadHi = appData->DataShardConfig.GetBackupReadAheadHi();
        if (ui64 readAheadHiOverride = DataShard.GetBackupReadAheadHiOverride(); readAheadHiOverride > 0) {
            readAheadHi = readAheadHiOverride;
        }

        auto options = TScanOptions()
                           .SetResourceBroker(taskName, taskPrio)
                           .SetReadAhead(readAheadLo, readAheadHi)
                           .SetReadPrio(TScanOptions::EReadPrio::Low);
        if (useSnapshot) {
            // The checksum query and the data scan must read the same retained version
            options.SetSnapshotRowVersion(snapshot);
        }
        tx->SetScanTask(DataShard.QueueScan(localTableId, scan.Release(), op->GetTxId(), options));

        return true;
    }

    bool HasResult(TOperation::TPtr op) const override {
        return op->HasScanResult();
    }

    bool ProcessResult(TOperation::TPtr op, const TActorContext&) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        auto* result = CheckedCast<TExportScanProduct*>(op->ScanResult().Get());
        bool done = true;

        switch (result->Outcome) {
        case EExportOutcome::Success:
        case EExportOutcome::Error:
            if (auto* schemeOp = DataShard.FindSchemaTx(op->GetTxId())) {
                schemeOp->Success = result->Outcome == EExportOutcome::Success;
                schemeOp->Error = std::move(result->Error);
                schemeOp->BytesProcessed = result->BytesRead;
                schemeOp->RowsProcessed = result->RowsRead;
            } else {
                Y_ENSURE(false, "Cannot find schema tx: " << op->GetTxId());
            }
            break;
        case EExportOutcome::Aborted:
            done = false;
            break;
        }

        op->SetScanResult(nullptr);
        tx->SetScanTask(0);

        return done;
    }

    void Cancel(TActiveTransaction* tx, const TActorContext&) override {
        if (!tx->GetScanTask()) {
            return;
        }

        const ui64 tableId = tx->GetSchemeTx().GetBackup().GetTableId();

        Y_ENSURE(DataShard.GetUserTables().contains(tableId));
        const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;

        DataShard.CancelScan(localTableId, tx->GetScanTask());
        tx->SetScanTask(0);
    }

public:
    TBackupUnit(TDataShard& self, TPipeline& pipeline)
        : TBase(EExecutionUnitKind::Backup, self, pipeline)
    {
    }

}; // TBackupUnit

THolder<TExecutionUnit> CreateBackupUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TBackupUnit(self, pipeline));
}

} // NDataShard
} // NKikimr
