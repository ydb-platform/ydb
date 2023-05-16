#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/indexed_read_data.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {
namespace {

class TReadActor : public TActorBootstrapped<TReadActor> {
private:
    void BuildResult(const TActorContext& ctx) {
        auto ready = IndexedData.GetReadyResults(Max<i64>());
        LOG_S_TRACE("Ready results with " << ready.size() << " batches at tablet " << TabletId << " (read)");

        size_t next = 1;
        for (auto it = ready.begin(); it != ready.end(); ++it, ++next) {
            bool lastOne = Finished() && (next == ready.size());
            SendResult(ctx, it->ResultBatch, lastOne);
        }
    }
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_READ_ACTOR;
    }

    TReadActor(ui64 tabletId,
               const TActorId& dstActor,
               std::unique_ptr<TEvColumnShard::TEvReadResult>&& event,
               NOlap::TReadMetadata::TConstPtr readMetadata,
               const TInstant& deadline,
               const TActorId& columnShardActorId,
               ui64 requestCookie, const TScanCounters& counters)
        : TabletId(tabletId)
        , DstActor(dstActor)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
        , Result(std::move(event))
        , ReadMetadata(readMetadata)
        , IndexedData(ReadMetadata, IndexedBlobsForFetch, true, counters, TDataTasksProcessorContainer())
        , Deadline(deadline)
        , ColumnShardActorId(columnShardActorId)
        , RequestCookie(requestCookie)
        , ReturnedBatchNo(0)
    {}

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev, const TActorContext& ctx) {
        LOG_S_TRACE("TEvReadBlobRangeResult at tablet " << TabletId << " (read)");

        auto& event = *ev->Get();
        const TUnifiedBlobId& blobId = event.BlobRange.BlobId;

        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob " << blobId
                << " status " << NKikimrProto::EReplyStatus_Name(event.Status)
                << " at tablet " << TabletId << " (read)");
            SendErrorResult(ctx, NKikimrTxColumnShard::EResultStatus::ERROR);
            return DieFinished(ctx);
        }

        Y_VERIFY(event.Data.size() == event.BlobRange.Size, "%zu, %d", event.Data.size(), event.BlobRange.Size);

        if (IndexedBlobs.contains(event.BlobRange)) {
            if (!WaitIndexed.contains(event.BlobRange)) {
                return; // ignore duplicate parts
            }
            WaitIndexed.erase(event.BlobRange);
            IndexedData.AddIndexed(event.BlobRange, event.Data);
        } else if (CommittedBlobs.contains(blobId)) {
            auto cmt = WaitCommitted.extract(NOlap::TCommittedBlob{blobId, NOlap::TSnapshot::Zero()});
            if (cmt.empty()) {
                return; // ignore duplicates
            }
            const NOlap::TCommittedBlob& cmtBlob = cmt.key();
            ui32 batchNo = cmt.mapped();
            IndexedData.AddNotIndexed(batchNo, event.Data, cmtBlob.GetSnapshot());
        } else {
            LOG_S_ERROR("TEvReadBlobRangeResult returned unexpected blob at tablet "
                << TabletId << " (read)");
            return;
        }

        BuildResult(ctx);
        DieFinished(ctx);
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        LOG_S_INFO("TEvWakeup: read timeout at tablet " << TabletId << " (read)");

        SendTimeouts(ctx);
        DieFinished(ctx);
    }

    void SendErrorResult(const TActorContext& ctx, NKikimrTxColumnShard::EResultStatus status) {
        Y_VERIFY(status != NKikimrTxColumnShard::EResultStatus::SUCCESS);
        SendResult(ctx, {}, true, status);

        WaitIndexed.clear();
        WaitCommitted.clear();
    }

    void SendResult(const TActorContext& ctx, const std::shared_ptr<arrow::RecordBatch>& batch, bool finished = false,
                    NKikimrTxColumnShard::EResultStatus status = NKikimrTxColumnShard::EResultStatus::SUCCESS) {
        auto chunkEvent = std::make_unique<TEvColumnShard::TEvReadResult>(*Result);
        auto& proto = Proto(chunkEvent.get());

        TString data;
        if (batch) {
            data = NArrow::SerializeBatchNoCompression(batch);

            auto metadata = proto.MutableMeta();
            metadata->SetFormat(NKikimrTxColumnShard::FORMAT_ARROW);
            metadata->SetSchema(GetSerializedSchema(batch));
        }

        if (status == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
            Y_VERIFY(!data.empty());
        }

        proto.SetBatch(ReturnedBatchNo);
        proto.SetData(data);
        proto.SetStatus(status);
        proto.SetFinished(finished);
        ++ReturnedBatchNo;

        if (finished) {
            auto stats = ReadMetadata->ReadStats;
            auto* protoStats = proto.MutableMeta()->MutableReadStats();
            protoStats->SetBeginTimestamp(stats->BeginTimestamp.MicroSeconds());
            protoStats->SetDurationUsec(stats->Duration().MicroSeconds());
            protoStats->SetSelectedIndex(stats->SelectedIndex);
            protoStats->SetIndexGranules(stats->IndexGranules);
            protoStats->SetIndexPortions(stats->IndexPortions);
            protoStats->SetIndexBatches(stats->IndexBatches);
            protoStats->SetNotIndexedBatches(stats->CommittedBatches);
            protoStats->SetSchemaColumns(stats->SchemaColumns);
            protoStats->SetFilterColumns(stats->FilterColumns);
            protoStats->SetAdditionalColumns(stats->AdditionalColumns);
            protoStats->SetDataFilterBytes(stats->DataFilterBytes);
            protoStats->SetDataAdditionalBytes(stats->DataAdditionalBytes);
            protoStats->SetPortionsBytes(stats->PortionsBytes);
            protoStats->SetSelectedRows(stats->SelectedRows);
        }

        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                proto.SetStatus(NKikimrTxColumnShard::EResultStatus::TIMEOUT);
            }
        }

        ctx.Send(DstActor, chunkEvent.release());
    }

    bool Finished() const {
        return WaitCommitted.empty() && WaitIndexed.empty();
    }

    void DieFinished(const TActorContext& ctx) {
        if (Finished()) {
            LOG_S_DEBUG("Finished read (with " << ReturnedBatchNo << " batches sent) at tablet " << TabletId);
            Send(ColumnShardActorId, new TEvPrivate::TEvReadFinished(RequestCookie));
            Die(ctx);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        ui32 notIndexed = 0;
        for (size_t i = 0; i < ReadMetadata->CommittedBlobs.size(); ++i, ++notIndexed) {
            const auto& cmtBlob = ReadMetadata->CommittedBlobs[i];

            CommittedBlobs.emplace(cmtBlob.GetBlobId());
            WaitCommitted.emplace(cmtBlob, notIndexed);
        }

        IndexedData.InitRead(notIndexed);
        while (IndexedBlobsForFetch.size()) {
            const auto blobRange = IndexedBlobsForFetch.pop_front();
            WaitIndexed.insert(blobRange);
            IndexedBlobs.emplace(blobRange);
        }

        // Add cached batches without read
        for (auto& [blobId, batch] : ReadMetadata->CommittedBatches) {
            auto cmt = WaitCommitted.extract(NOlap::TCommittedBlob{blobId, NOlap::TSnapshot::Zero()});
            Y_VERIFY(!cmt.empty());

            const NOlap::TCommittedBlob& cmtBlob = cmt.key();
            ui32 batchNo = cmt.mapped();
            IndexedData.AddNotIndexed(batchNo, batch, cmtBlob.GetSnapshot());
        }

        LOG_S_DEBUG("Starting read (" << WaitIndexed.size() << " indexed, "
            << ReadMetadata->CommittedBlobs.size() << " committed, "
            << WaitCommitted.size() << " not cached committed) at tablet " << TabletId);

        bool earlyExit = false;
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                earlyExit = true;
            } else {
                const TDuration timeout = Deadline - now;
                ctx.Schedule(timeout, new TEvents::TEvWakeup());
            }
        }

        if (earlyExit) {
            SendTimeouts(ctx);
            ctx.Send(SelfId(), new TEvents::TEvPoisonPill());
        } else {
            // TODO: Keep inflight
            for (auto& [cmtBlob, batchNo] : WaitCommitted) {
                auto& blobId = cmtBlob.GetBlobId();
                SendReadRequest(ctx, NBlobCache::TBlobRange(blobId, 0, blobId.BlobSize()));
            }
            for (auto&& blobRange : IndexedBlobs) {
                SendReadRequest(ctx, blobRange);
            }
            if (WaitCommitted.empty() && IndexedBlobs.empty()) {
                BuildResult(ctx);
            }
        }

        Become(&TThis::StateWait);
    }

    void SendTimeouts(const TActorContext& ctx) {
        SendErrorResult(ctx, NKikimrTxColumnShard::EResultStatus::TIMEOUT);
    }

    void SendReadRequest(const TActorContext& ctx, const NBlobCache::TBlobRange& blobRange) {
        Y_UNUSED(ctx);
        Y_VERIFY(blobRange.Size);

        auto& externBlobs = ReadMetadata->ExternBlobs;
        bool fallback = externBlobs && externBlobs->contains(blobRange.BlobId);

        NBlobCache::TReadBlobRangeOptions readOpts {
            .CacheAfterRead = true,
            .ForceFallback = fallback,
            .IsBackgroud = false
        };
        Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRange(blobRange, std::move(readOpts)));
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }

private:
    ui64 TabletId;
    TActorId DstActor;
    TActorId BlobCacheActorId;
    std::unique_ptr<TEvColumnShard::TEvReadResult> Result;
    NOlap::TReadMetadata::TConstPtr ReadMetadata;
    NOlap::TFetchBlobsQueue IndexedBlobsForFetch;
    NOlap::TIndexedReadData IndexedData;
    TInstant Deadline;
    TActorId ColumnShardActorId;
    const ui64 RequestCookie;
    THashSet<NBlobCache::TBlobRange> IndexedBlobs;
    THashSet<TUnifiedBlobId> CommittedBlobs;
    THashSet<NBlobCache::TBlobRange> WaitIndexed;
    std::unordered_map<NOlap::TCommittedBlob, ui32, THash<NOlap::TCommittedBlob>> WaitCommitted;
    ui32 ReturnedBatchNo;
    mutable TString SerializedSchema;

    TString GetSerializedSchema(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        auto resultSchema = ReadMetadata->GetResultSchema();
        Y_VERIFY(resultSchema);

        // TODO: make real ResultSchema with SSA effects
        if (resultSchema->Equals(batch->schema())) {
            if (!SerializedSchema.empty()) {
                return SerializedSchema;
            }
            SerializedSchema = NArrow::SerializeSchema(*resultSchema);
            return SerializedSchema;
        }

        return NArrow::SerializeSchema(*batch->schema());
    }
};

} // namespace

IActor* CreateReadActor(ui64 tabletId,
                        const TActorId& dstActor,
                        std::unique_ptr<TEvColumnShard::TEvReadResult>&& event,
                        NOlap::TReadMetadata::TConstPtr readMetadata,
                        const TInstant& deadline,
                        const TActorId& columnShardActorId,
                        ui64 requestCookie, const TScanCounters& counters)
{
    return new TReadActor(tabletId, dstActor, std::move(event), readMetadata,
                          deadline, columnShardActorId, requestCookie, counters);
}

}
