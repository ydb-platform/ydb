#include "columnshard_impl.h"
#include "blob_cache.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/index_logic_logs.h>

using NKikimr::NOlap::TBlobRange;

namespace NKikimr::NColumnShard {
namespace {

class TCompactionActor: public TActorBootstrapped<TCompactionActor> {
private:
    const TIndexationCounters InternalCounters = TIndexationCounters("InternalCompaction");
    const TIndexationCounters SplitCounters = TIndexationCounters("SplitCompaction");
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_COMPACTION_ACTOR;
    }

    TCompactionActor(ui64 tabletId, const TActorId& parent)
        : TabletId(tabletId)
        , Parent(parent)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId()) {
    }

    void Handle(TEvPrivate::TEvCompaction::TPtr& ev, const TActorContext& /*ctx*/) {
        Y_VERIFY(!TxEvent);
        Y_VERIFY(Blobs.empty() && !NumRead);
        LastActivationTime = TAppData::TimeProvider->Now();
        auto& event = *ev->Get();
        TxEvent = std::move(event.TxEvent);
        IsSplitCurrently = NOlap::TCompactionLogic::IsSplit(TxEvent->IndexChanges);

        auto& indexChanges = TxEvent->IndexChanges;
        Y_VERIFY(indexChanges);
        Y_VERIFY(indexChanges->CompactionInfo);

        LOG_S_DEBUG("Granules compaction: " << *indexChanges << " at tablet " << TabletId);

        for (auto& [blobId, ranges] : event.GroupedBlobRanges) {
            Y_VERIFY(!ranges.empty());

            for (const auto& blobRange : ranges) {
                Y_VERIFY(blobId == blobRange.BlobId);
                Blobs[blobRange] = {};
                GetCurrentCounters().ReadBytes->Add(blobRange.Size);
            }
            SendReadRequest(std::move(ranges), event.Externals.contains(blobId));
        }
    }

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev, const TActorContext& ctx) {
        LOG_S_TRACE("TEvReadBlobRangeResult (got " << NumRead << " of " << Blobs.size() << ") at tablet " << TabletId
                                                   << " (compaction)");

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
        } else if (event.Status == NKikimrProto::EReplyStatus::NODATA) {
            Y_ASSERT(false);
            LOG_S_WARN("TEvReadBlobRangeResult cannot get blob "
                << blobId.ToString() << " status " << NKikimrProto::EReplyStatus_Name(event.Status) << " at tablet "
                << TabletId << " (compaction)");
        } else {
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob "
                        << blobId.ToString() << " status " << NKikimrProto::EReplyStatus_Name(event.Status) << " at tablet "
                        << TabletId << " (compaction)");
            if (event.Status == NKikimrProto::UNKNOWN) {
                TxEvent->SetPutStatus(NKikimrProto::ERROR);
            } else {
                TxEvent->SetPutStatus(event.Status);
            }
        }

        if (++NumRead == Blobs.size()) {
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
    bool IsSplitCurrently = false;
    ui64 TabletId;
    TActorId Parent;
    TActorId BlobCacheActorId;
    std::unique_ptr<TEvPrivate::TEvWriteIndex> TxEvent;
    THashMap<TBlobRange, TString> Blobs;
    ui32 NumRead{0};
    TInstant LastActivationTime;

    void Clear() {
        TxEvent.reset();
        Blobs.clear();
        NumRead = 0;
    }

    const TIndexationCounters& GetCurrentCounters() const {
        if (IsSplitCurrently) {
            return SplitCounters;
        } else {
            return InternalCounters;
        }
    }

    void SendReadRequest(std::vector<NBlobCache::TBlobRange>&& ranges, bool isExternal) {
        Y_VERIFY(!ranges.empty());

        NBlobCache::TReadBlobRangeOptions readOpts{.CacheAfterRead = false, .ForceFallback = isExternal, .IsBackgroud = true};
        Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(ranges), std::move(readOpts)));
    }

    void CompactGranules(const TActorContext& ctx) {
        Y_VERIFY(TxEvent);
        if (TxEvent->GetPutStatus() != NKikimrProto::EReplyStatus::UNKNOWN) {
            LOG_S_INFO("Granules compaction not started at tablet " << TabletId);
            ctx.Send(Parent, TxEvent.release());
            return;
        }

        LOG_S_DEBUG("Granules compaction started at tablet " << TabletId);
        {
            TCpuGuard guard(TxEvent->ResourceUsage);

            TxEvent->IndexChanges->SetBlobs(std::move(Blobs));
            {
                NOlap::TCompactionLogic compactionLogic(TxEvent->IndexInfo, TxEvent->Tiering, GetCurrentCounters());
                TxEvent->Blobs = std::move(compactionLogic.Apply(TxEvent->IndexChanges).DetachResult());
            }
            if (TxEvent->Blobs.empty()) {
                TxEvent->SetPutStatus(NKikimrProto::OK); // nothing to write, commit
            }
        }
        TxEvent->Duration = TAppData::TimeProvider->Now() - LastActivationTime;
        ui32 blobsSize = TxEvent->Blobs.size();
        ctx.Send(Parent, TxEvent.release());

        LOG_S_DEBUG("Granules compaction finished (" << blobsSize << " new blobs) at tablet " << TabletId);
        // Die(ctx); // It's alive till tablet's death
    }
};

class TCompactionGroupActor: public TActorBootstrapped<TCompactionGroupActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_COMPACTION_ACTOR;
    }

    TCompactionGroupActor(ui64 tabletId, const TActorId& parent, const ui64 size)
        : TabletId(tabletId)
        , Parent(parent)
        , Idle(size == 0 ? 1 : size)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWait);

        for (auto& worker : Idle) {
            worker = ctx.Register(new TCompactionActor(TabletId, ctx.SelfID));
        }
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TEvPrivate::TEvWriteIndex, Handle);
            HFunc(TEvPrivate::TEvCompaction, Handle);
            default:
                break;
        }
    }

private:
    void Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
        for (const auto& worker : Active) {
            ctx.Send(worker, new TEvents::TEvPoisonPill);
        }
        for (const auto& worker : Idle) {
            ctx.Send(worker, new TEvents::TEvPoisonPill);
        }

        Die(ctx);
    }

    void Handle(TEvPrivate::TEvWriteIndex::TPtr& ev, const TActorContext& ctx) {
        if (auto ai = Active.find(ev->Sender); ai != Active.end()) {
            ctx.Send(ev->Forward(Parent));
            Idle.push_back(*ai);
            Active.erase(ai);
        } else {
            Y_VERIFY(false);
        }
    }

    void Handle(TEvPrivate::TEvCompaction::TPtr& ev, const TActorContext& ctx) {
        Y_VERIFY(!Idle.empty());

        ctx.Send(ev->Forward(Idle.back()));

        Active.insert(Idle.back());
        Idle.pop_back();
    }

private:
    const ui64 TabletId;
    const TActorId Parent;
    /// Set of workers perform compactions.
    std::unordered_set<TActorId> Active;
    /// List of idle workers.
    std::vector<TActorId> Idle;
};

} // namespace

IActor* CreateCompactionActor(ui64 tabletId, const TActorId& parent, const ui64 workers) {
    return new TCompactionGroupActor(tabletId, parent, workers);
}

} // namespace NKikimr::NColumnShard
