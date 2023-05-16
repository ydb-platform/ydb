#include "columnshard_impl.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/index_logic_logs.h>
#include "blob_cache.h"

namespace NKikimr::NColumnShard {
namespace {

using NOlap::TBlobRange;

class TCompactionActor : public TActorBootstrapped<TCompactionActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_COMPACTION_ACTOR;
    }

    TCompactionActor(ui64 tabletId, const TActorId& parent)
        : TabletId(tabletId)
        , Parent(parent)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
    {}

    void Handle(TEvPrivate::TEvCompaction::TPtr& ev, const TActorContext& /*ctx*/) {
        LastActivationTime = TAppData::TimeProvider->Now();
        auto& event = *ev->Get();
        TxEvent = std::move(event.TxEvent);
        Y_VERIFY(TxEvent);
        Y_VERIFY(Blobs.empty() && !NumRead);

        auto& indexChanges = TxEvent->IndexChanges;
        Y_VERIFY(indexChanges);
        Y_VERIFY(indexChanges->CompactionInfo);

        LOG_S_DEBUG("Granules compaction: " << *indexChanges << " at tablet " << TabletId);

        for (auto& [blobId, ranges] : event.GroupedBlobRanges) {
            Y_VERIFY(!ranges.empty());

            for (const auto& blobRange : ranges) {
                Y_VERIFY(blobId == blobRange.BlobId);
                Blobs[blobRange] = {};
            }
            SendReadRequest(std::move(ranges), event.Externals.contains(blobId));
        }
    }

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev, const TActorContext& ctx) {
        LOG_S_TRACE("TEvReadBlobRangeResult (got " << NumRead << " of " << Blobs.size()
            << ") at tablet " << TabletId << " (compaction)");

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
                << " at tablet " << TabletId << " (compaction)");
            TxEvent->PutStatus = event.Status;
            if (TxEvent->PutStatus == NKikimrProto::UNKNOWN) {
                TxEvent->PutStatus = NKikimrProto::ERROR;
            }
        }

        ++NumRead;
        if (NumRead == Blobs.size()) {
            CompactGranules(ctx);
            Clear();
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCompaction, Handle);
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
    TInstant LastActivationTime;

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

    void CompactGranules(const TActorContext& ctx) {
        Y_VERIFY(TxEvent);
        if (TxEvent->PutStatus != NKikimrProto::EReplyStatus::UNKNOWN) {
            LOG_S_INFO("Granules compaction not started at tablet " << TabletId);
            ctx.Send(Parent, TxEvent.release());
            return;
        }

        LOG_S_DEBUG("Granules compaction started at tablet " << TabletId);
        {
            TCpuGuard guard(TxEvent->ResourceUsage);

            TxEvent->IndexChanges->SetBlobs(std::move(Blobs));

            NOlap::TCompactionLogic compactionLogic(TxEvent->IndexInfo, TxEvent->Tiering);
            TxEvent->Blobs = compactionLogic.Apply(TxEvent->IndexChanges);
            if (TxEvent->Blobs.empty()) {
                TxEvent->PutStatus = NKikimrProto::OK; // nothing to write, commit
            }
        }
        TxEvent->Duration = TAppData::TimeProvider->Now() - LastActivationTime;
        ui32 blobsSize = TxEvent->Blobs.size();
        ctx.Send(Parent, TxEvent.release());

        LOG_S_DEBUG("Granules compaction finished (" << blobsSize << " new blobs) at tablet " << TabletId);
        //Die(ctx); // It's alive till tablet's death
    }
};

} // namespace

IActor* CreateCompactionActor(ui64 tabletId, const TActorId& parent) {
    return new TCompactionActor(tabletId, parent);
}

}
