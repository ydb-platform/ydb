#include "backup_restore_common.h"
#include "backup_restore_traits.h"
#include "execution_unit_ctors.h"
#include "export_iface.h"
#include "export_scan.h"
#include "export_s3.h"

#include <ydb/core/protos/datashard_config.pb.h>

namespace NKikimr {
namespace NDataShard {

class TBackupUnit : public TBackupRestoreUnitBase<TEvDataShard::TEvCancelBackup> {
    using IBuffer = NExportScan::IBuffer;

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
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        Y_ABORT_UNLESS(tx->GetSchemeTx().HasBackup());
        const auto& backup = tx->GetSchemeTx().GetBackup();

        const ui64 tableId = backup.GetTableId();
        Y_ABORT_UNLESS(DataShard.GetUserTables().contains(tableId));

        const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;
        Y_ABORT_UNLESS(txc.DB.GetScheme().GetTableInfo(localTableId));

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
            } else {
                Abort(op, ctx, "Exports to YT are disabled");
                return false;
            }
        } else if (backup.HasS3Settings()) {
            NBackupRestoreTraits::ECompressionCodec codec;
            if (!TryCodecFromTask(backup, codec)) {
                Abort(op, ctx, TStringBuilder() << "Unsupported compression codec"
                    << ": " << backup.GetCompression().GetCodec());
                return false;
            }

            if (auto* exportFactory = appData->DataShardExportFactory) {
                std::shared_ptr<IExport>(exportFactory->CreateExportToS3(backup, columns)).swap(exp);
            } else {
                Abort(op, ctx, "Exports to S3 are disabled");
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

        tx->SetScanTask(DataShard.QueueScan(localTableId, scan.Release(), op->GetTxId(),
            TScanOptions()
                .SetResourceBroker(taskName, taskPrio)
                .SetReadAhead(readAheadLo, readAheadHi)
                .SetReadPrio(TScanOptions::EReadPrio::Low)
        ));

        return true;
    }

    bool HasResult(TOperation::TPtr op) const override {
        return op->HasScanResult();
    }

    bool ProcessResult(TOperation::TPtr op, const TActorContext&) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

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
                Y_FAIL_S("Cannot find schema tx: " << op->GetTxId());
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

        Y_ABORT_UNLESS(DataShard.GetUserTables().contains(tableId));
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
