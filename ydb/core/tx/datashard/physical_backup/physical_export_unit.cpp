#include "physical_export_unit.h"

#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/datashard_pipeline.h>
#include <ydb/core/tx/datashard/execution_unit_ctors.h>
#include <ydb/core/tx/datashard/export_iface.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_part_store.h>
#include <ydb/core/tablet_flat/flat_store_hotdog.h>

#include <ydb/core/tx/datashard/physical_backup/protos/physical_backup.pb.h>

namespace NKikimr {
namespace NDataShard {

// TPhysicalExportActor: an actor that enumerates SST parts from a datashard,
// fetches their blob data from blobstorage, and uploads to S3.
//
// Flow:
//   1. Receive parts list + blob IDs from the execution unit
//   2. For each part:
//      a. Fetch all blobs via TEvBlobStorage::TEvGet
//      b. Serialize TBundle proto + blob data
//      c. Upload to S3 via multipart upload
//   3. Upload shard manifest
//   4. Send completion event back to the datashard

class TPhysicalExportActor : public TActorBootstrapped<TPhysicalExportActor> {
public:
    struct TPartData {
        NKikimrExecutorFlat::TBundle BundleProto;
        TVector<TLogoBlobID> BlobIds;
        ui64 RowCount = 0;
        ui64 DataBytes = 0;
    };

    struct TEvPhysicalExportResult : public TEventLocal<TEvPhysicalExportResult, TEvDataShard::EvSchemaChanged + 100> {
        bool Success = false;
        TString Error;
        ui64 BytesExported = 0;
        ui64 PartsExported = 0;

        TEvPhysicalExportResult() = default;
        TEvPhysicalExportResult(bool success, const TString& error = {})
            : Success(success)
            , Error(error)
        {}
    };

    TPhysicalExportActor(
        const TActorId& dataShard,
        ui64 txId,
        NPhysicalBackup::TPhysicalExportTask task,
        NPhysicalBackup::TShardManifest manifest,
        TVector<TPartData> parts)
        : DataShard(dataShard)
        , TxId(txId)
        , Task(std::move(task))
        , Manifest(std::move(manifest))
        , Parts(std::move(parts))
    {}

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalExportActor: starting export of " << Parts.size() << " parts"
            << " for shard " << Task.shard_index()
            << " txId " << TxId);

        if (Parts.empty()) {
            // No parts to export, just upload manifest
            UploadManifest(ctx);
            return;
        }

        // Start fetching blobs for the first part
        CurrentPartIndex = 0;
        FetchBlobsForPart(ctx);

        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvGetResult, HandleGetResult);
            // S3 upload events would go here
            CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
        }
    }

private:
    void FetchBlobsForPart(const TActorContext& ctx) {
        Y_ABORT_UNLESS(CurrentPartIndex < Parts.size());
        const auto& part = Parts[CurrentPartIndex];

        if (part.BlobIds.empty()) {
            // No blobs in this part (shouldn't happen for real parts)
            OnPartBlobsFetched(ctx);
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalExportActor: fetching " << part.BlobIds.size()
            << " blobs for part " << CurrentPartIndex);

        // Create a batch TEvGet request for all blobs in this part
        auto request = MakeHolder<TEvBlobStorage::TEvGet>(
            /* mustRestoreFirst */ false,
            /* isIndexOnly */ false);

        for (const auto& blobId : part.BlobIds) {
            request->Queries.emplace_back(blobId, /* shift */ 0, /* size */ 0);
        }

        // Send to the blobstorage proxy group
        // The group is derived from the blob ID's channel
        const ui32 group = blobId.Channel(); // Simplified; real impl needs DS group resolution
        SendToBSProxy(ctx, group, request.Release());

        CurrentBlobData.clear();
        CurrentBlobData.reserve(part.BlobIds.size());
    }

    void HandleGetResult(TEvBlobStorage::TEvGetResult::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();

        if (msg->Status != NKikimrProto::OK) {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD,
                "TPhysicalExportActor: blob fetch failed"
                << " status " << msg->Status
                << " for part " << CurrentPartIndex);

            auto result = MakeHolder<TEvPhysicalExportResult>(false,
                TStringBuilder() << "Blob fetch failed: " << msg->Status);
            ctx.Send(DataShard, result.Release());
            PassAway();
            return;
        }

        for (ui32 i = 0; i < msg->ResponseSz; ++i) {
            const auto& resp = msg->Responses[i];
            if (resp.Status != NKikimrProto::OK) {
                auto result = MakeHolder<TEvPhysicalExportResult>(false,
                    TStringBuilder() << "Blob " << resp.Id << " fetch failed: " << resp.Status);
                ctx.Send(DataShard, result.Release());
                PassAway();
                return;
            }
            CurrentBlobData.push_back(resp.Buffer.ConvertToString());
        }

        OnPartBlobsFetched(ctx);
    }

    void OnPartBlobsFetched(const TActorContext& ctx) {
        // Build the part entry in the manifest
        auto& part = Parts[CurrentPartIndex];
        auto* manifestPart = Manifest.mutable_parts(CurrentPartIndex);

        // Record blob offsets in the concatenated blob file
        ui64 offset = 0;
        for (ui32 i = 0; i < CurrentBlobData.size(); ++i) {
            auto* blobEntry = manifestPart->mutable_blobs(i);
            blobEntry->set_offset(offset);
            blobEntry->set_size(CurrentBlobData[i].size());
            offset += CurrentBlobData[i].size();
        }
        manifestPart->set_total_blob_bytes(offset);

        TotalBytesExported += offset;
        TotalPartsExported++;

        // TODO: Upload part_N.bundle.pb + part_N.blobs.bin to S3
        // For now, log progress
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalExportActor: part " << CurrentPartIndex
            << " fetched, " << CurrentBlobData.size() << " blobs"
            << ", " << offset << " bytes");

        // Move to next part
        CurrentPartIndex++;
        if (CurrentPartIndex < Parts.size()) {
            FetchBlobsForPart(ctx);
        } else {
            UploadManifest(ctx);
        }
    }

    void UploadManifest(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalExportActor: all parts exported"
            << ", total bytes: " << TotalBytesExported
            << ", total parts: " << TotalPartsExported);

        // TODO: Upload manifest.pb to S3

        auto result = MakeHolder<TEvPhysicalExportResult>(true);
        result->BytesExported = TotalBytesExported;
        result->PartsExported = TotalPartsExported;
        ctx.Send(DataShard, result.Release());
        PassAway();
    }

    void HandleWakeup(const TActorContext& ctx) {
        // Retry logic placeholder
        Y_UNUSED(ctx);
    }

private:
    const TActorId DataShard;
    const ui64 TxId;
    NPhysicalBackup::TPhysicalExportTask Task;
    NPhysicalBackup::TShardManifest Manifest;
    TVector<TPartData> Parts;

    ui32 CurrentPartIndex = 0;
    TVector<TString> CurrentBlobData;

    ui64 TotalBytesExported = 0;
    ui64 TotalPartsExported = 0;
};

// ============================================================================
// TPhysicalExportUnit: datashard execution unit
//
// This unit runs inside the datashard's tablet executor transaction.
// It enumerates SST parts, collects blob IDs, and spawns the
// TPhysicalExportActor to handle async blob fetching and S3 upload.
// ============================================================================

class TPhysicalExportUnit : public TExecutionUnit {
public:
    TPhysicalExportUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::Backup, false, dataShard, pipeline)
    {}

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        // Ready when not waiting, or when we have a result
        return !op->IsWaitingForScan() || op->HasScanResult();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        // Check if this is a physical export task
        if (!tx->GetSchemeTx().HasPhysicalExport()) {
            return EExecutionStatus::Executed;
        }

        const auto& exportTask = tx->GetSchemeTx().GetPhysicalExport();
        const ui64 tableId = exportTask.table_id();

        Y_ENSURE(DataShard.GetUserTables().contains(tableId));
        const ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;

        // Build manifest and enumerate parts
        NPhysicalBackup::TShardManifest manifest;
        manifest.set_shard_index(exportTask.shard_index());
        manifest.set_source_tablet_id(DataShard.TabletID());

        // Set key range from shard boundaries
        const auto& tableInfo = *DataShard.GetUserTables().at(tableId);
        auto* keyRange = manifest.mutable_key_range();
        keyRange->set_from_key(tableInfo.Range.From.GetBuffer());
        keyRange->set_to_key(tableInfo.Range.To.GetBuffer());
        keyRange->set_from_inclusive(tableInfo.Range.FromInclusive);
        keyRange->set_to_inclusive(tableInfo.Range.ToInclusive);

        // Set MVCC state
        auto* mvccState = manifest.mutable_mvcc_state();
        const auto& snapMgr = DataShard.GetSnapshotManager();
        auto completeEdge = snapMgr.GetCompleteEdge();
        auto incompleteEdge = snapMgr.GetIncompleteEdge();
        auto immediateWriteEdge = snapMgr.GetImmediateWriteEdge();
        mvccState->set_complete_edge_step(completeEdge.Step);
        mvccState->set_complete_edge_tx_id(completeEdge.TxId);
        mvccState->set_incomplete_edge_step(incompleteEdge.Step);
        mvccState->set_incomplete_edge_tx_id(incompleteEdge.TxId);
        mvccState->set_immediate_write_edge_step(immediateWriteEdge.Step);
        mvccState->set_immediate_write_edge_tx_id(immediateWriteEdge.TxId);

        // Enumerate SST parts
        TVector<TPhysicalExportActor::TPartData> parts;
        ui64 totalRows = 0;
        ui64 totalBytes = 0;
        ui32 partIndex = 0;

        txc.DB.EnumerateTableParts(localTableId, [&](const NTable::TPartView& partView) {
            auto* partStore = dynamic_cast<const NTable::TPartStore*>(partView.Part.Get());
            if (!partStore) {
                return; // Skip non-store parts (cold parts handled separately)
            }

            TPhysicalExportActor::TPartData partData;

            // Serialize TBundle proto
            NTabletFlatExecutor::TPageCollectionProtoHelper helper(false);
            helper.Do(&partData.BundleProto, partView);

            // Collect all blob IDs
            for (ui32 room = 0; room < partStore->PageCollections.size(); ++room) {
                const auto* packet = partStore->Packet(room);
                if (packet) {
                    packet->SaveAllBlobIdsTo(partData.BlobIds);
                }
            }

            partData.RowCount = partView.Part->Stat.Rows;
            partData.DataBytes = partView.Part->Stat.Bytes;
            totalRows += partData.RowCount;
            totalBytes += partData.DataBytes;

            // Add to manifest
            auto* manifestPart = manifest.add_parts();
            manifestPart->set_part_index(partIndex);
            manifestPart->mutable_bundle()->CopyFrom(partData.BundleProto);
            manifestPart->set_row_count(partData.RowCount);
            manifestPart->set_data_bytes(partData.DataBytes);

            // Pre-populate blob entries (offsets filled by actor after fetch)
            for (const auto& blobId : partData.BlobIds) {
                auto* blobEntry = manifestPart->add_blobs();
                blobEntry->set_raw_x1(blobId.IsEmpty() ? 0 : blobId.GetRawX1());
                blobEntry->set_raw_x2(blobId.IsEmpty() ? 0 : blobId.GetRawX2());
                blobEntry->set_raw_x3(blobId.IsEmpty() ? 0 : blobId.GetRawX3());
            }

            parts.push_back(std::move(partData));
            partIndex++;
        });

        // Set shard stats
        auto* stats = manifest.mutable_stats();
        stats->set_row_count(totalRows);
        stats->set_data_size(totalBytes);
        stats->set_part_count(parts.size());

        // Enumerate TxStatus parts
        txc.DB.EnumerateTableTxStatusParts(localTableId, [&](const NTable::TIntrusiveConstPtr<NTable::TTxStatusPart>& txStatus) {
            auto* entry = manifest.add_tx_status_parts();
            entry->set_tx_status_index(manifest.tx_status_parts_size() - 1);
            entry->set_epoch(txStatus->Epoch.ToProto());
        });
        stats->set_tx_status_part_count(manifest.tx_status_parts_size());

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            "TPhysicalExportUnit: enumerated " << parts.size() << " parts"
            << ", " << totalRows << " rows"
            << ", " << totalBytes << " bytes"
            << " for table " << tableId
            << " at tablet " << DataShard.TabletID());

        // Spawn the async export actor
        NPhysicalBackup::TPhysicalExportTask task;
        task.CopyFrom(exportTask);

        auto actor = new TPhysicalExportActor(
            DataShard.SelfId(),
            op->GetTxId(),
            std::move(task),
            std::move(manifest),
            std::move(parts));

        ctx.Register(actor);

        // Wait for the actor to complete
        op->SetWaitingForScanFlag();
        return EExecutionStatus::Continue;
    }

    void Complete(TOperation::TPtr, const TActorContext&) override {
    }
};

THolder<TExecutionUnit> CreatePhysicalExportUnit(TDataShard& dataShard, TPipeline& pipeline) {
    return THolder(new TPhysicalExportUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
