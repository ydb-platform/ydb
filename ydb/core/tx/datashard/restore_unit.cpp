#include "backup_restore_common.h"
#include "execution_unit_ctors.h"
#include "import_common.h"
#include "import_s3.h"

namespace NKikimr {
namespace NDataShard {

class TRestoreUnit : public TBackupRestoreUnitBase<TEvDataShard::TEvCancelRestore> {
protected:
    bool IsRelevant(TActiveTransaction* tx) const override {
        return tx->GetSchemeTx().HasRestore();
    }

    bool IsWaiting(TOperation::TPtr op) const override {
        return op->IsWaitingForAsyncJob();
    }

    void SetWaiting(TOperation::TPtr op) override {
        op->SetWaitingForAsyncJobFlag();
    }

    void ResetWaiting(TOperation::TPtr op) override {
        op->ResetWaitingForAsyncJobFlag();
    }

    bool Run(TOperation::TPtr op, TTransactionContext&, const TActorContext& ctx) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        Y_ABORT_UNLESS(tx->GetSchemeTx().HasRestore());
        const auto& restore = tx->GetSchemeTx().GetRestore();

        const ui64 tableId = restore.GetTableId();
        Y_ABORT_UNLESS(DataShard.GetUserTables().contains(tableId));

        const TTableInfo tableInfo = TTableInfo(tableId, DataShard.GetUserTables().at(tableId));

        const auto settingsKind = restore.GetSettingsCase();
        switch (settingsKind) {
        case NKikimrSchemeOp::TRestoreTask::kS3Settings:
        #ifndef KIKIMR_DISABLE_S3_OPS
            tx->SetAsyncJobActor(ctx.Register(CreateS3Downloader(DataShard.SelfId(), op->GetTxId(), restore, tableInfo),
                TMailboxType::HTSwap, AppData(ctx)->BatchPoolId));
            break;
        #else
            Abort(op, ctx, "Imports from S3 are disabled");
            return false;
        #endif

        default:
            Abort(op, ctx, TStringBuilder() << "Unknown settings: " << static_cast<ui32>(settingsKind));
            return false;
        }

        return true;
    }

    bool HasResult(TOperation::TPtr op) const override {
        return op->HasAsyncJobResult();
    }

    bool ProcessResult(TOperation::TPtr op, const TActorContext&) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        auto* result = CheckedCast<TImportJobProduct*>(op->AsyncJobResult().Get());
        auto* schemeOp = DataShard.FindSchemaTx(op->GetTxId());

        schemeOp->Success = result->Success;
        schemeOp->Error = std::move(result->Error);
        schemeOp->BytesProcessed = result->BytesWritten;
        schemeOp->RowsProcessed = result->RowsWritten;

        op->SetAsyncJobResult(nullptr);
        tx->SetAsyncJobActor(TActorId());

        return true;
    }

    void Cancel(TActiveTransaction* tx, const TActorContext& ctx) override {
        tx->KillAsyncJobActor(ctx);
    }

public:
    TRestoreUnit(TDataShard& self, TPipeline& pipeline)
        : TBase(EExecutionUnitKind::Restore, self, pipeline)
    {
    }

}; // TRestoreUnit

THolder<TExecutionUnit> CreateRestoreUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TRestoreUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
