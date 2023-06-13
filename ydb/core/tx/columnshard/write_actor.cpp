#include "columnshard_impl.h"

#include <ydb/core/blobstorage/dsproxy/blobstorage_backoff.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {
namespace {

class TWriteActor : public TActorBootstrapped<TWriteActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_WRITE_ACTOR;
    }

    TWriteActor(ui64 tabletId,
                const NOlap::ISnapshotSchema::TPtr& snapshotSchema,
                const TActorId& dstActor,
                TBlobBatch&& blobBatch,
                bool blobGrouppingEnabled,
                TAutoPtr<TEvColumnShard::TEvWrite> writeEv,
                TAutoPtr<TEvPrivate::TEvWriteIndex> writeIndexEv,
                const TInstant& deadline)
        : TabletId(tabletId)
        , SnapshotSchema(snapshotSchema)
        , DstActor(dstActor)
        , BlobBatch(std::move(blobBatch))
        , BlobGrouppingEnabled(blobGrouppingEnabled)
        , WriteEv(writeEv)
        , WriteIndexEv(writeIndexEv)
        , Deadline(deadline)
    {
        Y_VERIFY(SnapshotSchema);
        Y_VERIFY(WriteEv || WriteIndexEv);
        Y_VERIFY(!WriteEv || !WriteIndexEv);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr& ev, const TActorContext& ctx) {
        TEvBlobStorage::TEvPutResult* msg = ev->Get();
        auto status = msg->Status;

        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
            YellowMoveChannels.insert(msg->Id.Channel());
        }
        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
            YellowStopChannels.insert(msg->Id.Channel());
        }

        if (status != NKikimrProto::OK) {
            LOG_S_ERROR("Unsuccessful TEvPutResult for blob " << msg->Id.ToString()
                << " status: " << status << " reason: " << msg->ErrorReason);
            return SendResultAndDie(ctx, status);
        }

        LOG_S_TRACE("TEvPutResult for blob " << msg->Id.ToString());

        BlobBatch.OnBlobWriteResult(ev);

        if (BlobBatch.AllBlobWritesCompleted()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        LOG_S_WARN("TEvWakeup: write timeout at tablet " << TabletId << " (write)");

        SendResultAndDie(ctx, NKikimrProto::TIMEOUT);
    }

    void SendResultAndDie(const TActorContext& ctx, NKikimrProto::EReplyStatus status) {
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                status = NKikimrProto::TIMEOUT;
            }
        }

        SendResult(ctx, status);
        Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                return SendResultAndDie(ctx, NKikimrProto::TIMEOUT);
            }

            const TDuration timeout = Deadline - now;
            ctx.Schedule(timeout, new TEvents::TEvWakeup());
        }

        if (WriteEv) {
            SendWriteRequest(ctx);
        } else {
            SendMultiWriteRequest(ctx);
        }
        Become(&TThis::StateWait);
    }

    void SendWriteRequest(const TActorContext& ctx) {
        Y_VERIFY(WriteEv->GetPutStatus() == NKikimrProto::UNKNOWN);

        auto& record = Proto(WriteEv.Get());
        ui64 pathId = record.GetTableId();
        ui64 writeId = record.GetWriteId();
        auto& srcData = record.GetData();
        TString meta;
        if (record.HasMeta()) {
            meta = record.GetMeta().GetSchema();
            if (meta.empty() || record.GetMeta().GetFormat() != NKikimrTxColumnShard::FORMAT_ARROW) {
                LOG_S_INFO("Bad metadata for writeId " << writeId << " pathId " << pathId << " at tablet " << TabletId);
                return SendResultAndDie(ctx, NKikimrProto::ERROR);
            }
        }

        // Heavy operations inside. We cannot run them in tablet event handler.
        TString strError;
        std::shared_ptr<arrow::RecordBatch>& batch = WriteEv->WrittenBatch;
        {
            TCpuGuard guard(ResourceUsage);
            batch = SnapshotSchema->PrepareForInsert(srcData, meta, strError);
        }
        if (!batch) {
            LOG_S_INFO("Bad data for writeId " << writeId << ", pathId " << pathId
                << " (" << strError << ") at tablet " << TabletId);
            return SendResultAndDie(ctx, NKikimrProto::ERROR);
        }

        TString data;
        {
            TCpuGuard guard(ResourceUsage);
            data = NArrow::SerializeBatchNoCompression(batch);
        }
        if (data.size() > TLimits::GetMaxBlobSize()) {
            LOG_S_INFO("Extracted data (" << data.size() << " bytes) is bigger than source ("
                << srcData.size() << " bytes) and limit, writeId " << writeId << " pathId " << pathId
                << " at tablet " << TabletId);

            return SendResultAndDie(ctx, NKikimrProto::ERROR);
        }

        record.SetData(data); // modify for TxWrite

        { // Update meta
            ui64 dirtyTime = AppData(ctx)->TimeProvider->Now().Seconds();
            Y_VERIFY(dirtyTime);

            NKikimrTxColumnShard::TLogicalMetadata outMeta;
            outMeta.SetNumRows(batch->num_rows());
            outMeta.SetRawBytes(NArrow::GetBatchDataSize(batch));
            outMeta.SetDirtyWriteTimeSeconds(dirtyTime);

            meta.clear();
            if (!outMeta.SerializeToString(&meta)) {
                LOG_S_ERROR("Canot set metadata for blob, writeId " << writeId << " pathId " << pathId
                    << " at tablet " << TabletId);
                return SendResultAndDie(ctx, NKikimrProto::ERROR);
            }
        }
        record.MutableMeta()->SetLogicalMeta(meta);

        if (data.size() > WriteEv->MaxSmallBlobSize) {
            WriteEv->BlobId = DoSendWriteBlobRequest(data, ctx);
        } else {
            TUnifiedBlobId smallBlobId = BlobBatch.AddSmallBlob(data);
            Y_VERIFY(smallBlobId.IsSmallBlob());
            WriteEv->BlobId = smallBlobId;
        }

        Y_VERIFY(WriteEv->BlobId.BlobSize() == data.size());

        LOG_S_DEBUG("Writing " << WriteEv->BlobId.ToStringNew() << " writeId " << writeId << " pathId " << pathId
            << " at tablet " << TabletId);

        if (BlobBatch.AllBlobWritesCompleted()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
    }

    void SendMultiWriteRequest(const TActorContext& ctx) {
        Y_VERIFY(WriteIndexEv);
        Y_VERIFY(WriteIndexEv->GetPutStatus() == NKikimrProto::UNKNOWN);

        auto indexChanges = WriteIndexEv->IndexChanges;
        LOG_S_DEBUG("Writing " << WriteIndexEv->Blobs.size() << " blobs at " << TabletId);

        const std::vector<TString>& blobs = WriteIndexEv->Blobs;
        Y_VERIFY(blobs.size() > 0);
        size_t blobsPos = 0;

        // Send accumulated data and update records with the blob Id
        auto fnFlushAcummultedBlob = [this, &ctx] (TString& accumulatedBlob, NOlap::TPortionInfo& portionInfo,
            std::vector<std::pair<size_t, TString>>& recordsInBlob)
        {
            Y_VERIFY(accumulatedBlob.size() > 0);
            Y_VERIFY(recordsInBlob.size() > 0);
            auto blobId = DoSendWriteBlobRequest(accumulatedBlob, ctx);
            LOG_S_TRACE("Write Index Blob " << blobId << " with " << recordsInBlob.size() << " records");
            for (const auto& rec : recordsInBlob) {
                size_t i = rec.first;
                const TString& recData = rec.second;
                auto& blobRange = portionInfo.Records[i].BlobRange;
                blobRange.BlobId = blobId;
                Y_VERIFY(blobRange.Offset + blobRange.Size <= accumulatedBlob.size());
                Y_VERIFY(blobRange.Size == recData.size());

                if (WriteIndexEv->CacheData) {
                    // Save original (non-accumulted) blobs with the corresponding TBlobRanges in order to
                    // put them into cache at commit time
                    WriteIndexEv->IndexChanges->Blobs[blobRange] = recData;
                }
            }
            accumulatedBlob.clear();
            recordsInBlob.clear();
        };

        TString accumulatedBlob;
        std::vector<std::pair<size_t, TString>> recordsInBlob;

        Y_VERIFY(indexChanges->AppendedPortions.empty() || indexChanges->PortionsToEvict.empty());
        size_t portionsToWrite = indexChanges->AppendedPortions.size() + indexChanges->PortionsToEvict.size();
        bool eviction = indexChanges->PortionsToEvict.size() > 0;

        for (size_t pos = 0; pos < portionsToWrite; ++pos) {
            auto& portionInfo = eviction ? indexChanges->PortionsToEvict[pos].first
                                         : indexChanges->AppendedPortions[pos];
            auto& records = portionInfo.Records;

            accumulatedBlob.clear();
            recordsInBlob.clear();

            // There could be eviction mix between normal eviction and eviction without data changes
            // TODO: better portions to blobs mathching
            if (eviction && !indexChanges->PortionsToEvict[pos].second.DataChanges) {
                continue;
            }

            for (size_t i = 0; i < records.size(); ++i, ++blobsPos) {
                Y_VERIFY(blobsPos < blobs.size());
                const TString& currentBlob = blobs[blobsPos];
                Y_VERIFY(currentBlob.size());

                if ((accumulatedBlob.size() + currentBlob.size() > TLimits::GetMaxBlobSize()) ||
                    (accumulatedBlob.size() && !BlobGrouppingEnabled))
                {
                    fnFlushAcummultedBlob(accumulatedBlob, portionInfo, recordsInBlob);
                }

                // Accumulate data chunks into a single blob and save record indices of these chunks
                records[i].BlobRange.Offset = accumulatedBlob.size();
                records[i].BlobRange.Size = currentBlob.size();
                accumulatedBlob.append(currentBlob);
                recordsInBlob.emplace_back(i, currentBlob);
            }
            if (accumulatedBlob.size() != 0) {
                fnFlushAcummultedBlob(accumulatedBlob, portionInfo, recordsInBlob);
            }
        }
        Y_VERIFY(blobsPos == blobs.size());
    }

    TUnifiedBlobId DoSendWriteBlobRequest(const TString& data, const TActorContext& ctx) {
        ResourceUsage.Network += data.size();
        return BlobBatch.SendWriteBlobRequest(data, Deadline, ctx);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvPutResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }

private:
    ui64 TabletId;
     NOlap::ISnapshotSchema::TPtr SnapshotSchema;
    TActorId DstActor;
    TBlobBatch BlobBatch;
    bool BlobGrouppingEnabled;
    TAutoPtr<TEvColumnShard::TEvWrite> WriteEv;
    TAutoPtr<TEvPrivate::TEvWriteIndex> WriteIndexEv;
    TInstant Deadline;
    THashSet<ui32> YellowMoveChannels;
    THashSet<ui32> YellowStopChannels;
    TUsage ResourceUsage;

    void SaveResourceUsage() {
        if (WriteEv) {
            WriteEv->ResourceUsage.Add(ResourceUsage);
        } else {
            WriteIndexEv->ResourceUsage.Add(ResourceUsage);
        }
        ResourceUsage = TUsage();
    }

    void SendResult(const TActorContext& ctx, NKikimrProto::EReplyStatus status) {
        SaveResourceUsage();
        if (WriteEv) {
            WriteEv->SetPutStatus(status, std::move(YellowMoveChannels), std::move(YellowStopChannels));
            WriteEv->BlobBatch = std::move(BlobBatch);
            ctx.Send(DstActor, WriteEv.Release());
        } else {
            WriteIndexEv->SetPutStatus(status, std::move(YellowMoveChannels), std::move(YellowStopChannels));
            WriteIndexEv->BlobBatch = std::move(BlobBatch);
            ctx.Send(DstActor, WriteIndexEv.Release());
        }
    }
};

} // namespace

IActor* CreateWriteActor(ui64 tabletId, const NOlap::ISnapshotSchema::TPtr& snapshotSchema,
                        const TActorId& dstActor, TBlobBatch&& blobBatch, bool blobGrouppingEnabled,
                        TAutoPtr<TEvColumnShard::TEvWrite> ev, const TInstant& deadline) {
    return new TWriteActor(tabletId, snapshotSchema, dstActor, std::move(blobBatch), blobGrouppingEnabled, ev, {}, deadline);
}

IActor* CreateWriteActor(ui64 tabletId, const NOlap::ISnapshotSchema::TPtr& snapshotSchema,
                        const TActorId& dstActor, TBlobBatch&& blobBatch, bool blobGrouppingEnabled,
                        TAutoPtr<TEvPrivate::TEvWriteIndex> ev, const TInstant& deadline) {
    return new TWriteActor(tabletId, snapshotSchema, dstActor, std::move(blobBatch), blobGrouppingEnabled, {}, ev, deadline);
}

}
