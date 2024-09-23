#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include "read_table_scan.h"

namespace NKikimr {
namespace NDataShard {

class TReadTableScanUnit : public TExecutionUnit {
public:
    TReadTableScanUnit(TDataShard &dataShard,
                       TPipeline &pipeline);
    ~TReadTableScanUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
    void ProcessEvent(TAutoPtr<NActors::IEventHandle> &ev,
                      TOperation::TPtr op,
                      const NActors::TActorContext &ctx);
    void Handle(TEvTxProcessing::TEvInterruptTransaction::TPtr &ev,
                TOperation::TPtr op,
                const TActorContext &ctx);
    void Abort(const TString &err,
               TOperation::TPtr op,
               const TActorContext &ctx);
};

TReadTableScanUnit::TReadTableScanUnit(TDataShard &dataShard,
                                       TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::ReadTableScan, false, dataShard, pipeline)
{
}

TReadTableScanUnit::~TReadTableScanUnit()
{
}

bool TReadTableScanUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    // Pass aborted operations
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op))
        return true;

    if (!op->IsWaitingForScan())
        return !op->HasRuntimeConflicts();

    if (op->HasScanResult())
        return true;

    if (op->HasPendingInputEvents())
        return true;

    return false;
}

EExecutionStatus TReadTableScanUnit::Execute(TOperation::TPtr op,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    // Pass aborted operations (e.g. while waiting for stream clearance, or because of a split/merge)
    if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
        op->ResetWaitingForScanFlag();

        // Ignore scan result (if any)
        if (op->HasScanResult()) {
            tx->SetScanTask(0);
            op->SetScanResult(nullptr);
        }

        if (tx->GetScanSnapshotId()) {
            DataShard.DropScanSnapshot(tx->GetScanSnapshotId());
            tx->SetScanSnapshotId(0);
        } else if (tx->GetScanTask()) {
            auto tid = tx->GetDataTx()->GetReadTableTransaction().GetTableId().GetTableId();
            auto info = DataShard.GetUserTables().at(tid);

            DataShard.CancelScan(info->LocalTid, tx->GetScanTask());
            tx->SetScanTask(0);
        }

        return EExecutionStatus::Executed;
    }

    bool hadWrites = false;

    if (!op->IsWaitingForScan()) {
        const auto& record = tx->GetDataTx()->GetReadTableTransaction();

        if (record.HasSnapshotStep() && record.HasSnapshotTxId()) {
            Y_ABORT_UNLESS(op->HasAcquiredSnapshotKey(), "Missing snapshot reference in ReadTable tx");

            bool wait = false;
            TRowVersion snapshot(record.GetSnapshotStep(), record.GetSnapshotTxId());
            for (auto* info : DataShard.GetVolatileTxManager().GetVolatileTxByVersion()) {
                if (!(info->Version <= snapshot)) {
                    break;
                }
                op->AddVolatileDependency(info->TxId);
                bool ok = DataShard.GetVolatileTxManager().AttachWaitingRemovalOperation(info->TxId, op->GetTxId());
                Y_VERIFY_S(ok, "Unexpected failure to attach TxId# " << op->GetTxId() << " to volatile tx " << info->TxId);
                wait = true;
            }

            if (wait) {
                // Wait until all volatile transactions below snapshot are removed
                // This guarantees they are either committed or aborted and will
                // be visible without any special tx map.
                return EExecutionStatus::Continue;
            }
        } else if (!DataShard.IsMvccEnabled()) {
            Y_ABORT_UNLESS(tx->GetScanSnapshotId(), "Missing snapshot in ReadTable tx");
        }

        auto tid = record.GetTableId().GetTableId();
        auto info = DataShard.GetUserTables().at(tid);

        auto scan = CreateReadTableScan(op->GetTxId(), DataShard.TabletID(), info, record,
                                        tx->GetStreamSink(), DataShard.SelfId());

        TScanOptions options;

        if (record.HasSnapshotStep() && record.HasSnapshotTxId()) {
            // With persistent snapshots we don't need to mark any preceding transactions
            auto readVersion = TRowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());
            options.SetSnapshotRowVersion(readVersion);
        } else if (DataShard.IsMvccEnabled()) {
            // Note: this mode is only used in legacy tests and may not work with volatile transactions
            // With mvcc we have to mark all preceding transactions as logically complete
            auto readVersion = DataShard.GetReadWriteVersions(tx).ReadVersion;
            hadWrites |= Pipeline.MarkPlannedLogicallyCompleteUpTo(readVersion, txc);
            if (op->IsMvccSnapshotRepeatable() || !op->IsImmediate()) {
                hadWrites |= DataShard.PromoteCompleteEdge(op.Get(), txc);
            }
            options.SetSnapshotRowVersion(readVersion);
        } else {
            // Without mvcc transactions are already marked using legacy rules
            options.SetSnapshotId(tx->GetScanSnapshotId());

            tx->SetScanSnapshotId(0);
        }

        // FIXME: we need to tie started scan to a write above being committed

        tx->SetScanTask(DataShard.QueueScan(info->LocalTid, scan, op->GetTxId(), options));

        op->SetWaitingForScanFlag();

        Y_DEBUG_ABORT_UNLESS(!op->HasScanResult());
    }

    if (op->HasScanResult()) {
        auto *result = CheckedCast<TReadTableProd*>(op->ScanResult().Get());

        LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD,
                    "ReadTable scan complete for " << *op << " at "
                    << DataShard.TabletID() << " error: " << result->Error << ", IsFatalError: " << result->IsFatalError);

        tx->SetScanTask(0);

        if (result->IsFatalError) {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR)
                ->SetExecutionError(NKikimrTxDataShard::TError::PROGRAM_ERROR, result->Error);
        } else if (result->SchemaChanged) {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::ERROR)
                ->AddError(NKikimrTxDataShard::TError::SCHEME_CHANGED, result->Error);
        } else if (result->Error) {
            BuildResult(op)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE,
                                      result->Error);
        } else {
            BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
        }
        op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

        op->SetScanResult(nullptr);
        op->ResetWaitingForScanFlag();
    }

    while (op->HasPendingInputEvents()) {
        ProcessEvent(op->InputEvents().front(), op, ctx);
        op->InputEvents().pop();
    }

    if (op->IsWaitingForScan())
        return EExecutionStatus::Continue;

    if (hadWrites)
        return EExecutionStatus::ExecutedNoMoreRestarts;

    return EExecutionStatus::Executed;
}

void TReadTableScanUnit::ProcessEvent(TAutoPtr<NActors::IEventHandle> &ev,
                                      TOperation::TPtr op,
                                      const NActors::TActorContext &ctx)
{
    switch (ev->GetTypeRewrite()) {
        OHFunc(TEvTxProcessing::TEvInterruptTransaction, Handle);
        IgnoreFunc(TEvTxProcessing::TEvStreamClearancePending);
        IgnoreFunc(TEvTxProcessing::TEvStreamClearanceResponse);
    default:
        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                    "TReadTableScanUnit::ProcessEvent unhandled event type: " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
        Y_DEBUG_ABORT("unexpected event %" PRIu64, (ui64)ev->GetTypeRewrite());
    }
}

void TReadTableScanUnit::Handle(TEvTxProcessing::TEvInterruptTransaction::TPtr &,
                                TOperation::TPtr op,
                                const TActorContext &ctx)
{
    if (op->IsWaitingForScan()) {
        Abort(TStringBuilder() << "Interrupted operation " << *op << " at "
              << DataShard.TabletID() << " while waiting for scan finish",
              op, ctx);
    }
}

void TReadTableScanUnit::Abort(const TString &err,
                               TOperation::TPtr op,
                               const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    BuildResult(op)->AddError(NKikimrTxDataShard::TError::WRONG_SHARD_STATE, err);
    if (tx->GetScanSnapshotId()) {
        DataShard.DropScanSnapshot(tx->GetScanSnapshotId());
        tx->SetScanSnapshotId(0);
    } else if (tx->GetScanTask()) {
        auto tid = tx->GetDataTx()->GetReadTableTransaction().GetTableId().GetTableId();
        auto info = DataShard.GetUserTables().at(tid);

        DataShard.CancelScan(info->LocalTid, tx->GetScanTask());
        tx->SetScanTask(0);
    }

    LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD, err);

    op->ResetWaitingForScanFlag();
}

void TReadTableScanUnit::Complete(TOperation::TPtr,
                                  const TActorContext &)
{
}

THolder<TExecutionUnit> CreateReadTableScanUnit(TDataShard &dataShard,
                                                TPipeline &pipeline)
{
    return THolder(new TReadTableScanUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
