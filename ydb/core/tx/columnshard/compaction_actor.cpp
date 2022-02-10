#include "columnshard_impl.h" 
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include "blob_cache.h" 
 
namespace NKikimr::NColumnShard { 
 
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
 
    void Handle(TEvPrivate::TEvCompaction::TPtr& ev, const TActorContext& ctx) { 
        auto& event = *ev->Get(); 
        TxEvent = std::move(event.TxEvent); 
        Y_VERIFY(TxEvent); 
        Y_VERIFY(Blobs.empty() && !NumRead); 
 
        auto& indexChanges = TxEvent->IndexChanges; 
        Y_VERIFY(indexChanges); 
        Y_VERIFY(indexChanges->CompactionInfo); 
 
        LOG_S_DEBUG("Granules compaction: " << *indexChanges << " at tablet " << TabletId); 
 
        auto& switchedPortions = indexChanges->SwitchedPortions; 
        Y_VERIFY(switchedPortions.size()); 
 
        for (auto& portionInfo : switchedPortions) { 
            Y_VERIFY(!portionInfo.Empty()); 
            std::vector<NBlobCache::TBlobRange> ranges;
            for (auto& rec : portionInfo.Records) { 
                auto& blobRange = rec.BlobRange; 
                Blobs[blobRange] = {}; 
                // Group only ranges from the same blob into one request
                if (!ranges.empty() && ranges.back().BlobId != blobRange.BlobId) {
                    SendReadRequest(ctx, std::move(ranges));
                    ranges = {};
                }
                ranges.push_back(blobRange);
            } 
            SendReadRequest(ctx, std::move(ranges));
        } 
    } 
 
    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev, const TActorContext& ctx) { 
        LOG_S_TRACE("TEvReadBlobRangeResult (got " << NumRead << " of " << Blobs.size() 
            << ") at tablet " << TabletId << " (compaction)"); 
 
        auto& event = *ev->Get(); 
        const TBlobRange& blobId = event.BlobRange;
        Y_VERIFY(Blobs.count(blobId)); 
        if (!Blobs[blobId].empty()) { 
            return; 
        } 
 
        if (event.Status == NKikimrProto::EReplyStatus::OK) { 
            Y_VERIFY(event.Data.size()); 
 
            TString blobData = event.Data; 
            Y_VERIFY(blobData.size() == blobId.Size, "%u vs %u", (ui32)blobData.size(), blobId.Size);
            Blobs[blobId] = blobData; 
        } else { 
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob " << blobId.ToString() << " status " << event.Status 
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
 
    void Clear() { 
        Blobs.clear(); 
        NumRead = 0; 
    } 
 
    void SendReadRequest(const TActorContext&, std::vector<NBlobCache::TBlobRange>&& ranges) {
        if (ranges.empty())
            return;

        Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(ranges), false));
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
 
            TxEvent->Blobs = NOlap::TColumnEngineForLogs::CompactBlobs(TxEvent->IndexInfo, TxEvent->IndexChanges);
        }
        ui32 blobsSize = TxEvent->Blobs.size(); 
        ctx.Send(Parent, TxEvent.release()); 
 
        LOG_S_DEBUG("Granules compaction finished (" << blobsSize << " new blobs) at tablet " << TabletId); 
        //Die(ctx); // It's alive till tablet's death 
    } 
}; 
 
IActor* CreateCompactionActor(ui64 tabletId, const TActorId& parent) {
    return new TCompactionActor(tabletId, parent);
} 
 
} 
