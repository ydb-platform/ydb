#include "columnshard_impl.h"

#include <ydb/core/blobstorage/dsproxy/blobstorage_backoff.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

namespace {

class IBlobConstructor {
public:
    using TPtr = std::shared_ptr<IBlobConstructor>;

    enum class EStatus {
        Ok,
        Finished,
        Error
    };

    virtual ~IBlobConstructor() {}
    virtual const TString& GetBlob() const = 0;
    virtual bool RegisterBlobId(const TUnifiedBlobId& blobId) = 0;
    virtual EStatus BuildNext(TUsage& resourceUsage, const TAppData& appData) = 0;

    virtual TAutoPtr<IEventBase> BuildResult(NKikimrProto::EReplyStatus status, TBlobBatch&& blobBatch, THashSet<ui32>&& yellowMoveChannels, THashSet<ui32>&& yellowStopChannels, const TUsage& resourceUsage) = 0;
};

class TWriteActor : public TActorBootstrapped<TWriteActor> {
    ui64 TabletId;
    TActorId DstActor;
    TUsage ResourceUsage;

    TBlobBatch BlobBatch;
    IBlobConstructor::TPtr BlobsConstructor;
    
    THashSet<ui32> YellowMoveChannels;
    THashSet<ui32> YellowStopChannels;
    TInstant Deadline;
    std::optional<ui64> MaxSmallBlobSize;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_WRITE_ACTOR;
    }

    TWriteActor(ui64 tabletId, const TActorId& dstActor, TBlobBatch&& blobBatch, IBlobConstructor::TPtr blobsConstructor, const TInstant& deadline, std::optional<ui64> maxSmallBlobSize = {})
        : TabletId(tabletId)
        , DstActor(dstActor)
        , BlobBatch(std::move(blobBatch))
        , BlobsConstructor(blobsConstructor)
        , Deadline(deadline)
        , MaxSmallBlobSize(maxSmallBlobSize)
    {}
    
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
            LOG_S_ERROR("Unsuccessful TEvPutResult for blob " << msg->Id.ToString() << " status: " << status << " reason: " << msg->ErrorReason);
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

    void SendResultAndDie(const TActorContext& ctx, const NKikimrProto::EReplyStatus status) {
        NKikimrProto::EReplyStatus putStatus = status;
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                putStatus = NKikimrProto::TIMEOUT;
            }
        }
        auto ev = BlobsConstructor->BuildResult(putStatus, std::move(BlobBatch), 
                std::move(YellowMoveChannels),
                std::move(YellowStopChannels),
                ResourceUsage);
        ctx.Send(DstActor, ev.Release());
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
       
        auto status = IBlobConstructor::EStatus::Finished;
        while (true) {
            status = BlobsConstructor->BuildNext(ResourceUsage, *AppData(ctx));
            if (status != IBlobConstructor::EStatus::Ok) {
                break;
            }
            auto blobId = SendWriteBlobRequest(BlobsConstructor->GetBlob(), ctx);
            BlobsConstructor->RegisterBlobId(blobId);
            
        }
        if (status != IBlobConstructor::EStatus::Finished) {
            return SendResultAndDie(ctx, NKikimrProto::ERROR);
        }
        if (BlobBatch.AllBlobWritesCompleted()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
        Become(&TThis::StateWait);
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
    TUnifiedBlobId SendWriteBlobRequest(const TString& data, const TActorContext& ctx) {
        ResourceUsage.Network += data.size();
        if (MaxSmallBlobSize && data.size() <= *MaxSmallBlobSize) {
            TUnifiedBlobId smallBlobId = BlobBatch.AddSmallBlob(data);
            Y_VERIFY(smallBlobId.IsSmallBlob());
            return smallBlobId;
        }
        return BlobBatch.SendWriteBlobRequest(data, Deadline, ctx);
    }
};

class TIndexedBlobConstructor : public IBlobConstructor {
    TAutoPtr<TEvColumnShard::TEvWrite> WriteEv;
    NOlap::ISnapshotSchema::TPtr SnapshotSchema;

    TString DataPrepared;
    TString MetaString;
    std::shared_ptr<arrow::RecordBatch> Batch;
    ui64 Iteration = 0;


public:
    TIndexedBlobConstructor(TAutoPtr<TEvColumnShard::TEvWrite> writeEv, NOlap::ISnapshotSchema::TPtr snapshotSchema)
        : WriteEv(writeEv)
        , SnapshotSchema(snapshotSchema)
    {}

    const TString& GetBlob() const override {
        return DataPrepared;
    }

    EStatus BuildNext(TUsage& resourceUsage, const TAppData& appData) override {
        if (!!DataPrepared) {
            return EStatus::Finished;
        }

        const auto& evProto = Proto(WriteEv.Get());
        const ui64 pathId = evProto.GetTableId();
        const ui64 writeId = evProto.GetWriteId();

        TString serializedScheme;
        if (evProto.HasMeta()) {
            serializedScheme = evProto.GetMeta().GetSchema();
            if (serializedScheme.empty() || evProto.GetMeta().GetFormat() != NKikimrTxColumnShard::FORMAT_ARROW) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_parsing_fails")("write_id", writeId)("path_id", pathId);
                return EStatus::Error;
            }
        }
        const auto& data = evProto.GetData();

        // Heavy operations inside. We cannot run them in tablet event handler.
        TString strError;
        {
            TCpuGuard guard(resourceUsage);
            Batch = SnapshotSchema->PrepareForInsert(data, serializedScheme, strError);
        }
        if (!Batch) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_data")("write_id", writeId)("path_id", pathId)("error", strError);
            return EStatus::Error;
        }

        {
            TCpuGuard guard(resourceUsage);
            DataPrepared = NArrow::SerializeBatchNoCompression(Batch);
        }

        if (DataPrepared.size() > TLimits::GetMaxBlobSize()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_data_too_big")("write_id", writeId)("path_id", pathId);
            return EStatus::Error;
        }
        
        ui64 dirtyTime = appData.TimeProvider->Now().Seconds();
        Y_VERIFY(dirtyTime);
        NKikimrTxColumnShard::TLogicalMetadata outMeta;
        outMeta.SetNumRows(Batch->num_rows());
        outMeta.SetRawBytes(NArrow::GetBatchDataSize(Batch));
        outMeta.SetDirtyWriteTimeSeconds(dirtyTime);

        if (!outMeta.SerializeToString(&MetaString)) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "ev_write_bad_metadata")("write_id", writeId)("path_id", pathId);
            return EStatus::Error;;
        }

        ++Iteration;
        return EStatus::Ok;
    }

    bool RegisterBlobId(const TUnifiedBlobId& blobId) override {
        Y_VERIFY(Iteration == 1);
        WriteEv->BlobId = blobId;
        Y_VERIFY(WriteEv->BlobId.BlobSize() == DataPrepared.size());
        return true;
    }

    virtual TAutoPtr<IEventBase> BuildResult(NKikimrProto::EReplyStatus status, TBlobBatch&& blobBatch, THashSet<ui32>&& yellowMoveChannels, THashSet<ui32>&& yellowStopChannels, const TUsage& resourceUsage) override {
        WriteEv->WrittenBatch = Batch;

        auto& record = Proto(WriteEv.Get());
        record.SetData(DataPrepared); // modify for TxWrite
        record.MutableMeta()->SetLogicalMeta(MetaString);

        WriteEv->ResourceUsage.Add(resourceUsage);
        WriteEv->SetPutStatus(status, std::move(yellowMoveChannels), std::move(yellowStopChannels));
        WriteEv->BlobBatch = std::move(blobBatch);
        return WriteEv.Release();
    }
};

class TCompactedBlobsConstructor : public IBlobConstructor {
    TAutoPtr<TEvPrivate::TEvWriteIndex> WriteIndexEv;
    const NOlap::TColumnEngineChanges& IndexChanges;
    const std::vector<TString>& Blobs;

    const bool BlobGrouppingEnabled;
    const bool CacheData;

    TString AccumulatedBlob;
    std::vector<std::pair<size_t, TString>> RecordsInBlob;

    ui64 CurrentPortion = 0;
    ui64 LastPortion = 0;

    ui64 CurrentBlob = 0;
    ui64 CurrentPortionRecord = 0;

    TVector<NOlap::TPortionInfo> PortionUpdates; 

public:
    TCompactedBlobsConstructor(TAutoPtr<TEvPrivate::TEvWriteIndex> writeIndexEv, bool blobGrouppingEnabled)
        : WriteIndexEv(writeIndexEv)
        , IndexChanges(*WriteIndexEv->IndexChanges)
        , Blobs(WriteIndexEv->Blobs)
        , BlobGrouppingEnabled(blobGrouppingEnabled)
        , CacheData(WriteIndexEv->CacheData)
    {
        auto indexChanges = WriteIndexEv->IndexChanges;
        LastPortion = indexChanges->AppendedPortions.size() + indexChanges->PortionsToEvict.size();
        Y_VERIFY(Blobs.size() > 0);
    }

    virtual const TString& GetBlob() const override {
        return AccumulatedBlob;
    }

    virtual bool RegisterBlobId(const TUnifiedBlobId& blobId) override {
        Y_VERIFY(AccumulatedBlob.size() > 0);
        Y_VERIFY(RecordsInBlob.size() > 0);

        auto& portionInfo = PortionUpdates.back();        
        LOG_S_TRACE("Write Index Blob " << blobId << " with " << RecordsInBlob.size() << " records");
        for (const auto& rec : RecordsInBlob) {
            size_t i = rec.first;
            const TString& recData = rec.second;
            auto& blobRange = portionInfo.Records[i].BlobRange;
            blobRange.BlobId = blobId;
            Y_VERIFY(blobRange.Offset + blobRange.Size <= AccumulatedBlob.size());
            Y_VERIFY(blobRange.Size == recData.size());

            if (CacheData) {
                // Save original (non-accumulted) blobs with the corresponding TBlobRanges in order to
                // put them into cache at commit time
                WriteIndexEv->IndexChanges->Blobs[blobRange] = recData;
            }
        }
        return true;
    }

    virtual EStatus BuildNext(TUsage& resourceUsage, const TAppData& /*appData*/) override {
        Y_UNUSED(resourceUsage);
        if (CurrentPortion == LastPortion) {
            Y_VERIFY(CurrentBlob == Blobs.size());
            return EStatus::Finished;
        }

        AccumulatedBlob.clear();
        RecordsInBlob.clear();
  
        if (CurrentPortionRecord == 0) {
            PortionUpdates.push_back(GetPortionInfo(CurrentPortion));
            // There could be eviction mix between normal eviction and eviction without data changes
            // TODO: better portions to blobs mathching
            const bool eviction = IndexChanges.PortionsToEvict.size() > 0;
            if (eviction) {
                while (CurrentPortion < LastPortion && !IndexChanges.PortionsToEvict[CurrentPortion].second.DataChanges) {
                    ++CurrentPortion;
                    PortionUpdates.push_back(GetPortionInfo(CurrentPortion));
                    continue;
                }
                if (CurrentPortion == LastPortion) {
                    return EStatus::Finished;
                }
            }
        }

        auto& portionInfo = GetPortionInfo(CurrentPortion);
        NOlap::TPortionInfo& newPortionInfo = PortionUpdates.back();

        const auto& records = portionInfo.Records;    
        for (; CurrentPortionRecord < records.size(); ++CurrentPortionRecord, ++CurrentBlob) {
            Y_VERIFY(CurrentBlob < Blobs.size());
            const TString& currentBlob = Blobs[CurrentBlob];
            Y_VERIFY(currentBlob.size());

            if ((!AccumulatedBlob.empty() && AccumulatedBlob.size() + currentBlob.size() > TLimits::GetMaxBlobSize()) ||
                (AccumulatedBlob.size() && !BlobGrouppingEnabled))
            {
                return EStatus::Ok;
            }

            // Accumulate data chunks into a single blob and save record indices of these chunks
            newPortionInfo.Records[CurrentPortionRecord].BlobRange.Offset = AccumulatedBlob.size();
            newPortionInfo.Records[CurrentPortionRecord].BlobRange.Size = currentBlob.size();
            AccumulatedBlob.append(currentBlob);
            RecordsInBlob.emplace_back(CurrentPortionRecord, currentBlob);
        }

        ++CurrentPortion;
        CurrentPortionRecord = 0;
        return AccumulatedBlob.empty() ? EStatus::Finished : EStatus::Ok;
    }

    virtual TAutoPtr<IEventBase> BuildResult(NKikimrProto::EReplyStatus status, TBlobBatch&& blobBatch, THashSet<ui32>&& yellowMoveChannels, THashSet<ui32>&& yellowStopChannels, const TUsage& resourceUsage) override {
        for (ui64 index = 0; index < PortionUpdates.size(); ++index) {
            const auto& portionInfo = PortionUpdates[index];
            const bool eviction = IndexChanges.PortionsToEvict.size() > 0;
            if (eviction) {
                Y_VERIFY(index < IndexChanges.PortionsToEvict.size());
                WriteIndexEv->IndexChanges->PortionsToEvict[index].first = portionInfo;
            } else {
                Y_VERIFY(index < IndexChanges.AppendedPortions.size());
                WriteIndexEv->IndexChanges->AppendedPortions[index] = portionInfo;
            }
        }

        WriteIndexEv->ResourceUsage.Add(resourceUsage);
        WriteIndexEv->SetPutStatus(status, std::move(yellowMoveChannels), std::move(yellowStopChannels));
        WriteIndexEv->BlobBatch = std::move(blobBatch);
        return WriteIndexEv.Release();
    }

private:
    const NOlap::TPortionInfo& GetPortionInfo(const ui64 index) const {
        bool eviction = IndexChanges.PortionsToEvict.size() > 0;
        if (eviction) {
            Y_VERIFY(index < IndexChanges.PortionsToEvict.size());
            return IndexChanges.PortionsToEvict[index].first;
        } else {
            Y_VERIFY(index < IndexChanges.AppendedPortions.size());
            return IndexChanges.AppendedPortions[index];
        }
    }
};

} // namespace

IActor* CreateWriteActor(ui64 tabletId, const NOlap::ISnapshotSchema::TPtr& snapshotSchema,
                        const TActorId& dstActor, TBlobBatch&& blobBatch, bool,
                        TAutoPtr<TEvColumnShard::TEvWrite> ev, const TInstant& deadline, const ui64 maxSmallBlobSize) {
    auto constructor = std::make_shared<TIndexedBlobConstructor>(ev,snapshotSchema);
    return new TWriteActor(tabletId, dstActor, std::move(blobBatch), constructor, deadline, maxSmallBlobSize);
}

IActor* CreateWriteActor(ui64 tabletId, const TActorId& dstActor, TBlobBatch&& blobBatch, bool blobGrouppingEnabled,
                        TAutoPtr<TEvPrivate::TEvWriteIndex> ev, const TInstant& deadline) {
    auto constructor = std::make_shared<TCompactedBlobsConstructor>(ev, blobGrouppingEnabled);
    return new TWriteActor(tabletId, dstActor, std::move(blobBatch), constructor, deadline);
}

}
