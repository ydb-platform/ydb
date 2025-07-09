#include "defs.h"
#include "execution_unit_ctors.h"
#include "datashard_active_transaction.h"
#include "datashard_impl.h"
#include "export_iface.h"
#include "incr_restore_scan.h"

#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/tablet_flat/flat_scan_spent.h>
#include <ydb/core/tx/replication/service/worker.h>
#include <ydb/core/backup/impl/table_writer.h>
#include <ydb/core/tx/datashard/change_exchange_helpers.h>
#include <ydb/core/tx/datashard/change_exchange_impl.h>

#define EXPORT_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)

namespace NKikimr {
namespace NDataShard {

using namespace NKikimrTxDataShard;
using namespace NExportScan;

class TCreateIncrementalRestoreSrcUnit : public TExecutionUnit {
protected:
    bool IsRelevant(TActiveTransaction* tx) const {
        return tx->GetSchemeTx().HasCreateIncrementalRestoreSrc();
    }

    bool IsWaiting(TOperation::TPtr op) const {
        return op->IsWaitingForScan() || op->IsWaitingForRestart();
    }

    void SetWaiting(TOperation::TPtr op) {
        op->SetWaitingForScanFlag();
    }

    void ResetWaiting(TOperation::TPtr op) {
        op->ResetWaitingForScanFlag();
        op->ResetWaitingForRestartFlag();
    }

    void Abort(TOperation::TPtr op, const TActorContext& ctx, const TString& error) {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, error);

        BuildResult(op)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, error);
        ResetWaiting(op);

        Cancel(tx, ctx);
    }

    THolder<NTable::IScan> CreateScan(
        const ::NKikimrSchemeOp::TRestoreIncrementalBackup& incrBackup,
        ui64 txId)
    {
        TPathId tablePathId = TPathId::FromProto(incrBackup.GetSrcPathId());
        TPathId dstTablePathId = TPathId::FromProto(incrBackup.GetDstPathId());
        const ui64 tableId = incrBackup.GetSrcPathId().GetLocalId();

        return CreateIncrementalRestoreScan(
                DataShard.SelfId(),
                [=, tabletID = DataShard.TabletID(), generation = DataShard.Generation(), tabletActor = DataShard.SelfId()](const TActorContext& ctx, TActorId parent) {
                    return ctx.Register(
                        CreateIncrRestoreChangeSender(
                            parent,
                            NDataShard::TDataShardId{
                                .TabletId = tabletID,
                                .Generation = generation,
                                .ActorId = tabletActor,
                            },
                            tablePathId,
                            dstTablePathId));
                },
                tablePathId,
                DataShard.GetUserTables().at(tableId),
                dstTablePathId,
                txId,
                DataShard.GetCurrentSchemeShardId(), // Pass SchemeShard TabletID
                {});
    }

    bool Run(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        Y_ENSURE(tx->GetSchemeTx().HasCreateIncrementalRestoreSrc());
        const auto& restoreSrc = tx->GetSchemeTx().GetCreateIncrementalRestoreSrc();

        const ui64 tableId = restoreSrc.GetSrcPathId().GetLocalId();
        Y_ENSURE(DataShard.GetUserTables().contains(tableId));

        const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;
        Y_ENSURE(txc.DB.GetScheme().GetTableInfo(localTableId));

        Y_ENSURE(restoreSrc.HasDstPathId());

        THolder<NTable::IScan> scan{CreateScan(restoreSrc, op->GetTxId())};

        auto* appData = AppData(ctx);
        const auto& taskName = appData->DataShardConfig.GetRestoreTaskName();
        const auto taskPrio = appData->DataShardConfig.GetRestoreTaskPriority();

        ui64 readAheadLo = appData->DataShardConfig.GetIncrementalRestoreReadAheadLo();
        if (ui64 readAheadLoOverride = DataShard.GetIncrementalRestoreReadAheadLoOverride(); readAheadLoOverride > 0) {
            readAheadLo = readAheadLoOverride;
        }

        ui64 readAheadHi = appData->DataShardConfig.GetIncrementalRestoreReadAheadHi();
        if (ui64 readAheadHiOverride = DataShard.GetIncrementalRestoreReadAheadHiOverride(); readAheadHiOverride > 0) {
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

    bool HasResult(TOperation::TPtr op) const {
        return op->HasScanResult();
    }

    bool ProcessResult(TOperation::TPtr op, const TActorContext&) {
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

    void Cancel(TActiveTransaction* tx, const TActorContext&) {
        if (!tx->GetScanTask()) {
            return;
        }

        const ui64 tableId = tx->GetSchemeTx().GetBackup().GetTableId();

        Y_ENSURE(DataShard.GetUserTables().contains(tableId));
        const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;

        DataShard.CancelScan(localTableId, tx->GetScanTask());
        tx->SetScanTask(0);
    }

    void PersistResult(TOperation::TPtr op, TTransactionContext& txc) {
        auto* schemeOp = DataShard.FindSchemaTx(op->GetTxId());
        Y_ENSURE(schemeOp);

        NIceDb::TNiceDb db(txc.DB);
        DataShard.PersistSchemeTxResult(db, *schemeOp);
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override final {
        Y_ENSURE(op->IsSchemeTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        if (!IsRelevant(tx)) {
            return EExecutionStatus::Executed;
        }

        if (!IsWaiting(op)) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Starting a " << GetKind() << " operation"
                << " at " << DataShard.TabletID());

            if (!Run(op, txc, ctx)) {
                return EExecutionStatus::Executed;
            }

            SetWaiting(op);
            Y_DEBUG_ABORT_UNLESS(!HasResult(op));
        }

        if (HasResult(op)) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, "" << GetKind() << " complete"
                << " at " << DataShard.TabletID());

            ResetWaiting(op);
            if (ProcessResult(op, ctx)) {
                PersistResult(op, txc);
            } else {
                Y_DEBUG_ABORT_UNLESS(!HasResult(op));
                op->SetWaitingForRestartFlag();
                ctx.Schedule(TDuration::Seconds(1), new TDataShard::TEvPrivate::TEvRestartOperation(op->GetTxId()));
            }
        }

        while (op->HasPendingInputEvents()) {
            ProcessEvent(op->InputEvents().front(), op, ctx);
            op->InputEvents().pop();
        }

        if (IsWaiting(op)) {
            return EExecutionStatus::Continue;
        }

        return EExecutionStatus::Executed;
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override final {
        if (!IsWaiting(op)) {
            return true;
        }

        if (HasResult(op)) {
            return true;
        }

        if (op->HasPendingInputEvents()) {
            return true;
        }

        return false;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override final {
    }


    void Handle(TEvIncrementalRestoreScan::TEvFinished::TPtr& ev, TOperation::TPtr op, const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD, 
                   "IncrementalRestoreScan finished for txId: " << ev->Get()->TxId 
                   << " at DataShard: " << DataShard.TabletID());
        
        // Additional completion handling can be added here if needed
        // (e.g., updating operation status, sending additional notifications)
        
        ResetWaiting(op);
    }

    void ProcessEvent(TAutoPtr<NActors::IEventHandle>& ev, TOperation::TPtr op, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            OHFunc(TEvIncrementalRestoreScan::TEvFinished, Handle);
            // OHFunc(TEvCancel, Handle);
        }
        Y_UNUSED(op, ctx);
    }

public:
    TCreateIncrementalRestoreSrcUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreateIncrementalRestoreSrc, false, self, pipeline)
    {
    }

}; // TRestoreIncrementalBackupSrcUnit

THolder<TExecutionUnit> CreateIncrementalRestoreSrcUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TCreateIncrementalRestoreSrcUnit(self, pipeline));
}

void TDataShard::Handle(TEvIncrementalRestoreScan::TEvFinished::TPtr& ev, const TActorContext& ctx) {
    TOperation::TPtr op = Pipeline.FindOp(ev->Get()->TxId);
    if (op) {
        ForwardEventToOperation(ev, op, ctx);
    }
}

} // NDataShard
} // NKikimr
