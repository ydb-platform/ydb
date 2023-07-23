#include "columnshard_impl.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include "blob_cache.h"

namespace NKikimr::NColumnShard {
namespace {

using NOlap::TBlobRange;

class TEvictionActor : public TActorBootstrapped<TEvictionActor> {
private:
    const TIndexationCounters Counters;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_EVICTION_ACTOR;
    }

    TEvictionActor(ui64 tabletId, const TActorId& parent, const TIndexationCounters& counters)
        : Counters(counters)
        , TabletId(tabletId)
        , Parent(parent)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
    {}

    void Handle(TEvPrivate::TEvEviction::TPtr& ev, const TActorContext& /*ctx*/) {
        auto& event = *ev->Get();
        TxEvent = std::move(event.TxEvent);
        Y_VERIFY(TxEvent);
        Y_VERIFY(Blobs.empty() && !NumRead);

        auto& indexChanges = TxEvent->IndexChanges;
        Y_VERIFY(indexChanges);

        LOG_S_DEBUG("Portions eviction: " << *indexChanges << " at tablet " << TabletId);

        for (auto& [blobId, ranges] : event.GroupedBlobRanges) {
            Y_VERIFY(!ranges.empty());

            for (const auto& blobRange : ranges) {
                Y_VERIFY(blobId == blobRange.BlobId);
                Counters.ReadBytes->Add(blobRange.Size);
                Blobs[blobRange] = {};
            }
            SendReadRequest(std::move(ranges), event.Externals.contains(blobId));
        }
    }

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev, const TActorContext& ctx) {
        LOG_S_TRACE("TEvReadBlobRangeResult (got " << NumRead << " of " << Blobs.size()
            << ") at tablet " << TabletId << " (eviction)");

        auto& event = *ev->Get();
        const TBlobRange& blobId = event.BlobRange;
        Y_VERIFY(Blobs.contains(blobId));
        if (!Blobs[blobId].empty()) {
            return;
        }

        if (event.Status == NKikimrProto::EReplyStatus::OK) {
            Y_VERIFY(event.Data.size());

            TString blobData = event.Data;
            Y_VERIFY(blobData.size() == blobId.Size, "%u vs %u", (ui32)blobData.size(), blobId.Size);
            Blobs[blobId] = blobData;
        } else {
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob " << blobId.ToString()
                << " status " << NKikimrProto::EReplyStatus_Name(event.Status)
                << " at tablet " << TabletId << " (eviction)");
            if (event.Status == NKikimrProto::UNKNOWN) {
                TxEvent->SetPutStatus(NKikimrProto::ERROR);
            } else {
                TxEvent->SetPutStatus(event.Status);
            }
        }

        ++NumRead;
        if (NumRead == Blobs.size()) {
            EvictPortions(ctx);
            Clear();
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvEviction, Handle);
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
    THashMap<TBlobRange, TString> Blobs;
    ui32 NumRead{0};

    void Clear() {
        Blobs.clear();
        NumRead = 0;
    }

    void SendReadRequest(std::vector<NBlobCache::TBlobRange>&& ranges, bool isExternal) {
        Y_VERIFY(!ranges.empty());

        NBlobCache::TReadBlobRangeOptions readOpts {
            .CacheAfterRead = false,
            .ForceFallback = isExternal,
            .IsBackgroud = true
        };
        Send(BlobCacheActorId,
             new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(ranges), std::move(readOpts)));
    }

    void EvictPortions(const TActorContext& ctx) {
        Y_VERIFY(TxEvent);
        if (TxEvent->GetPutStatus() != NKikimrProto::EReplyStatus::UNKNOWN) {
            LOG_S_INFO("Portions eviction not started at tablet " << TabletId);
            ctx.Send(Parent, TxEvent.release());
            return;
        }

        LOG_S_DEBUG("Portions eviction started at tablet " << TabletId);
        {
            auto guard = TxEvent->PutResult->StartCpuGuard();

            TxEvent->IndexChanges->SetBlobs(std::move(Blobs));
            NOlap::TConstructionContext context(TxEvent->IndexInfo, TxEvent->Tiering, Counters);
            TxEvent->Blobs = std::move(TxEvent->IndexChanges->ConstructBlobs(context).DetachResult());
            if (TxEvent->Blobs.empty()) {
                TxEvent->SetPutStatus(NKikimrProto::OK);
            }
        }
        ui32 blobsSize = TxEvent->Blobs.size();
        ctx.Send(Parent, TxEvent.release());

        LOG_S_DEBUG("Portions eviction finished (" << blobsSize << " new blobs) at tablet " << TabletId);
    }
};

} // namespace

IActor* CreateEvictionActor(ui64 tabletId, const TActorId& parent, const TIndexationCounters& counters) {
    return new TEvictionActor(tabletId, parent, counters);
}

}
