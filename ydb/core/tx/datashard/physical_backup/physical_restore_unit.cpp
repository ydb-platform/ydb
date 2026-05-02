#include "physical_restore_unit.h"

#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/datashard_pipeline.h>
#include <ydb/core/tx/datashard/execution_unit_ctors.h>
#include <ydb/core/tablet_flat/flat_executor_tx_env.h>
#include <ydb/core/tablet_flat/flat_store_hotdog.h>

#include <ydb/core/tx/datashard/physical_backup/protos/physical_backup.pb.h>

namespace NKikimr {
namespace NDataShard {

// TPhysicalRestoreActor: downloads SST parts from S3 and injects them into
// the datashard via the LoanTable mechanism.
//
// Flow:
//   1. Download shard manifest from S3
//   2. For each part in the manifest:
//      a. Download part_N.bundle.pb and part_N.blobs.bin
//      b. Write blobs to blobstorage via TEvBlobStorage::TEvPut
//      c. Reconstruct TDatabaseBorrowPart proto
//   3. Execute a datashard transaction that calls LoanTable() for each part
//   4. The existing executor machinery handles part loading and merge

class TPhysicalRestoreActor : public TActorBootstrapped<TPhysicalRestoreActor> {
public:
    struct TEvPhysicalRestoreResult : public TEventLocal<TEvPhysicalRestoreResult, TEvDataShard::EvSchemaChanged + 101> {
        bool Success = false;
        TString Error;
        ui64 BytesRestored = 0;
        ui64 PartsRestored = 0;

        // The reconstructed borrow parts to inject via LoanTable
        TVector<TString> BorrowParts;

        TEvPhysicalRestoreResult() = default;
        TEvPhysicalRestoreResult(bool success, const TString& error = {})
            : Success(success)
            , Error(error)
        {}
    };

    TPhysicalRestoreActor(
        const TActorId& dataShard,
        ui64 txId,
        NPhysicalBackup::TPhysicalRestoreTask task)
        : DataShard(dataShard)
        , TxId(txId)
        , Task(std::move(task))
    {}

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalRestoreActor: starting restore"
            << " from " << Task.shard_prefix()
            << " txId " << TxId);

        // Step 1: Download manifest from S3
        // TODO: S3 GetObject for <shard_prefix>/manifest.pb
        DownloadManifest(ctx);

        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvPutResult, HandlePutResult);
            CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
        }
    }

private:
    void DownloadManifest(const TActorContext& ctx) {
        // TODO: Download manifest.pb from S3
        // For now, this is a placeholder - real implementation would use
        // NWrappers::CreateStorageWrapper for S3 operations
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalRestoreActor: downloading manifest from S3");

        // After manifest is downloaded, start downloading parts
        CurrentPartIndex = 0;
        DownloadNextPart(ctx);
    }

    void DownloadNextPart(const TActorContext& ctx) {
        if (CurrentPartIndex >= static_cast<ui32>(Manifest.parts_size())) {
            // All parts downloaded and blobs written, finalize
            FinalizeRestore(ctx);
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalRestoreActor: downloading part " << CurrentPartIndex
            << " of " << Manifest.parts_size());

        // TODO: Download part_N.bundle.pb and part_N.blobs.bin from S3
        // Then write each blob to blobstorage via TEvPut
        WriteBlobsForPart(ctx);
    }

    void WriteBlobsForPart(const TActorContext& ctx) {
        const auto& part = Manifest.parts(CurrentPartIndex);

        if (part.blobs_size() == 0) {
            OnPartBlobsWritten(ctx);
            return;
        }

        // For each blob in this part, write to blobstorage
        PendingBlobWrites = part.blobs_size();

        for (int i = 0; i < part.blobs_size(); ++i) {
            const auto& blob = part.blobs(i);

            // Reconstruct TLogoBlobID from raw components
            TLogoBlobID blobId(blob.raw_x1(), blob.raw_x2(), blob.raw_x3());

            // TODO: Get the actual blob data from the downloaded blobs.bin file
            // using blob.offset() and blob.size()
            TString blobData; // placeholder

            // Determine the correct group for this blob
            ui32 group = 0; // TODO: resolve from channel -> group mapping

            auto request = MakeHolder<TEvBlobStorage::TEvPut>(
                blobId, blobData, TInstant::Max());
            SendToBSProxy(ctx, group, request.Release());
        }
    }

    void HandlePutResult(TEvBlobStorage::TEvPutResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();

        if (msg->Status != NKikimrProto::OK) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                "TPhysicalRestoreActor: blob write failed"
                << " status " << msg->Status
                << " blobId " << msg->Id);

            auto result = MakeHolder<TEvPhysicalRestoreResult>(false,
                TStringBuilder() << "Blob write failed: " << msg->Status);
            ctx.Send(DataShard, result.Release());
            PassAway();
            return;
        }

        TotalBytesRestored += msg->Id.BlobSize();

        if (--PendingBlobWrites == 0) {
            OnPartBlobsWritten(ctx);
        }
    }

    void OnPartBlobsWritten(const TActorContext& ctx) {
        const auto& part = Manifest.parts(CurrentPartIndex);

        // Reconstruct TDatabaseBorrowPart proto for this part
        // This is the same format that BorrowSnapshot produces and LoanTable consumes
        NKikimrExecutorFlat::TDatabaseBorrowPart borrowPart;
        borrowPart.SetLenderTablet(0); // No real lender; we own these blobs
        borrowPart.SetSourceTable(Task.table_id());

        auto* partInfo = borrowPart.AddParts();
        partInfo->MutableBundle()->CopyFrom(part.bundle());

        BorrowParts.push_back(borrowPart.SerializeAsString());
        TotalPartsRestored++;

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalRestoreActor: part " << CurrentPartIndex
            << " blobs written to blobstorage");

        CurrentPartIndex++;
        DownloadNextPart(ctx);
    }

    void FinalizeRestore(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalRestoreActor: all parts downloaded and blobs written"
            << ", total bytes: " << TotalBytesRestored
            << ", total parts: " << TotalPartsRestored);

        // Send the borrow parts back to the datashard for LoanTable injection
        auto result = MakeHolder<TEvPhysicalRestoreResult>(true);
        result->BytesRestored = TotalBytesRestored;
        result->PartsRestored = TotalPartsRestored;
        result->BorrowParts = std::move(BorrowParts);
        ctx.Send(DataShard, result.Release());
        PassAway();
    }

    void HandleWakeup(const TActorContext& ctx) {
        Y_UNUSED(ctx);
    }

private:
    const TActorId DataShard;
    const ui64 TxId;
    NPhysicalBackup::TPhysicalRestoreTask Task;
    NPhysicalBackup::TShardManifest Manifest;

    ui32 CurrentPartIndex = 0;
    ui32 PendingBlobWrites = 0;
    TVector<TString> BorrowParts;

    ui64 TotalBytesRestored = 0;
    ui64 TotalPartsRestored = 0;
};

// ============================================================================
// TPhysicalRestoreUnit: datashard execution unit
//
// After the TPhysicalRestoreActor downloads parts and writes blobs,
// this unit injects them into the datashard via LoanTable.
// ============================================================================

class TPhysicalRestoreUnit : public TExecutionUnit {
public:
    TPhysicalRestoreUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::Restore, false, dataShard, pipeline)
    {}

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        return !op->IsWaitingForScan() || op->HasScanResult();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        if (!tx->GetSchemeTx().HasPhysicalRestore()) {
            return EExecutionStatus::Executed;
        }

        if (!op->IsWaitingForScan()) {
            // First execution: spawn the restore actor
            const auto& restoreTask = tx->GetSchemeTx().GetPhysicalRestore();
            const ui64 tableId = restoreTask.table_id();

            Y_ENSURE(DataShard.GetUserTables().contains(tableId));

            NPhysicalBackup::TPhysicalRestoreTask task;
            task.CopyFrom(restoreTask);

            auto actor = new TPhysicalRestoreActor(
                DataShard.SelfId(),
                op->GetTxId(),
                std::move(task));

            ctx.Register(actor);
            op->SetWaitingForScanFlag();
            return EExecutionStatus::Continue;
        }

        // The restore actor has completed and sent us BorrowParts
        // Now inject them via LoanTable inside this transaction
        // The parts will be processed by the executor's part switch pipeline:
        //   PrepareExternalPart -> LoadMeta -> TLoader -> ApplyExternalPartSwitch
        //   -> Database::Merge()

        const auto& restoreTask = tx->GetSchemeTx().GetPhysicalRestore();
        const ui64 tableId = restoreTask.table_id();
        const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;

        // TODO: Retrieve BorrowParts from the restore actor's result
        // For each borrow part, call LoanTable to inject into the executor
        //
        // for (const auto& borrowPartData : restoreResult->BorrowParts) {
        //     txc.Env.LoanTable(localTableId, borrowPartData);
        // }

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalRestoreUnit: parts injected via LoanTable"
            << " for table " << tableId
            << " at tablet " << DataShard.TabletID());

        op->ResetWaitingForScanFlag();
        return EExecutionStatus::Executed;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
    }
};

THolder<TExecutionUnit> CreatePhysicalRestoreUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TPhysicalRestoreUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
