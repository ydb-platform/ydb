#include "columnshard_impl.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include "blob_cache.h"

namespace NKikimr::NColumnShard {

class TIndexingActor : public TActorBootstrapped<TIndexingActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_INDEXING_ACTOR;
    }

    TIndexingActor(ui64 tabletId, const TActorId& parent)
        : TabletId(tabletId)
        , Parent(parent)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
    {}

    void Handle(TEvPrivate::TEvIndexing::TPtr& ev, const TActorContext& ctx) {
        LOG_S_DEBUG("TEvIndexing at tablet " << TabletId << " (index)");

        LastActivationTime = TAppData::TimeProvider->Now();
        auto& event = *ev->Get();
        TxEvent = std::move(event.TxEvent);
        Y_VERIFY(TxEvent);
        auto& indexChanges = TxEvent->IndexChanges;
        Y_VERIFY(indexChanges);
        indexChanges->CachedBlobs = std::move(TxEvent->CachedBlobs);

        auto& blobsToIndex = indexChanges->DataToIndex;
        for (size_t i = 0; i < blobsToIndex.size(); ++i) {
            auto& blobId = blobsToIndex[i].BlobId;
            if (indexChanges->CachedBlobs.contains(blobId)) {
                continue;
            }

            auto res = BlobsToRead.emplace(blobId, i);
            Y_VERIFY(res.second, "Duplicate blob in DataToIndex: %s", blobId.ToStringNew().c_str());
            SendReadRequest(NBlobCache::TBlobRange(blobId, 0, blobId.BlobSize()));
        }

        if (BlobsToRead.empty()) {
            Index(ctx);
        }
    }

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev, const TActorContext& ctx) {
        LOG_S_TRACE("TEvReadBlobRangeResult (waiting " << BlobsToRead.size()
            << ") at tablet " << TabletId << " (index)");

        auto& event = *ev->Get();
        const TUnifiedBlobId& blobId = event.BlobRange.BlobId;
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob " << blobId
                << " status " << NKikimrProto::EReplyStatus_Name(event.Status)
                << " at tablet " << TabletId << " (index)");

            BlobsToRead.erase(blobId);
            TxEvent->PutStatus = event.Status;
            if (TxEvent->PutStatus == NKikimrProto::UNKNOWN) {
                TxEvent->PutStatus = NKikimrProto::ERROR;
            }
            return;
        }

        TString blobData = event.Data;
        Y_VERIFY(blobData.size() == blobId.BlobSize());

        if (!BlobsToRead.contains(blobId)) {
            return;
        }

        ui32 pos = BlobsToRead[blobId];
        BlobsToRead.erase(blobId);

        Y_VERIFY(TxEvent);
        auto& indexChanges = TxEvent->IndexChanges;
        Y_VERIFY(indexChanges);
        Y_VERIFY(indexChanges->DataToIndex[pos].BlobId == blobId);
        indexChanges->Blobs[event.BlobRange] = blobData;

        if (BlobsToRead.empty()) {
            Index(ctx);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvIndexing, Handle);
            HFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            default:
                break;
        }
    }

private:
    ui64 TabletId;
    TActorId Parent;
    TActorId BlobCacheActorId;
    std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
    THashMap<TUnifiedBlobId, ui32> BlobsToRead;
    TInstant LastActivationTime;

    void SendReadRequest(const NBlobCache::TBlobRange& blobRange) {
        Y_VERIFY(blobRange.Size);
        NBlobCache::TReadBlobRangeOptions readOpts {
            .CacheAfterRead = false,
            .ForceFallback = false,
            .IsBackgroud = true,
            .WithDeadline = false
        };
        Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRange(blobRange, std::move(readOpts)));
    }

    void Index(const TActorContext& ctx) {
        Y_VERIFY(TxEvent);
        if (TxEvent->PutStatus == NKikimrProto::UNKNOWN) {
            LOG_S_DEBUG("Indexing started at tablet " << TabletId);

            TCpuGuard guard(TxEvent->ResourceUsage);
            TxEvent->Blobs = NOlap::TColumnEngineForLogs::IndexBlobs(TxEvent->IndexInfo, TxEvent->Tiering, TxEvent->IndexChanges);

            LOG_S_DEBUG("Indexing finished at tablet " << TabletId);
        } else {
            LOG_S_ERROR("Indexing failed at tablet " << TabletId);
        }

        TxEvent->Duration = TAppData::TimeProvider->Now() - LastActivationTime;
        ctx.Send(Parent, TxEvent.release());
        //Die(ctx); // It's alive till tablet's death
    }
};

IActor* CreateIndexingActor(ui64 tabletId, const TActorId& parent) {
    return new TIndexingActor(tabletId, parent);
}

}
