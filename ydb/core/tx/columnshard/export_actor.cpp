#include "columnshard_impl.h"
#include "blob_cache.h"

#include <ydb/core/blobstorage/dsproxy/blobstorage_backoff.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {
namespace {

class TExportActor : public TActorBootstrapped<TExportActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_EXPORT_ACTOR;
    }

    TExportActor(ui64 tabletId, const TActorId& parent, TAutoPtr<TEvPrivate::TEvExport> ev)
        : TabletId(tabletId)
        , Parent(parent)
        , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
        , Event(ev.Release())
    {
        Y_VERIFY(Event);
    }

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev, const TActorContext& ctx) {
        LOG_S_TRACE("TEvReadBlobRangeResult (waiting " << BlobsToRead.size() << ") at tablet " << TabletId << " (export)");

        auto& event = *ev->Get();
        const TUnifiedBlobId& blobId = event.BlobRange.BlobId;
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            LOG_S_ERROR("TEvReadBlobRangeResult cannot get blob " << blobId
                << " status " << NKikimrProto::EReplyStatus_Name(event.Status)
                << " at tablet " << TabletId << " (export)");

            BlobsToRead.erase(blobId);
            Event->Status = event.Status;
            if (Event->Status == NKikimrProto::UNKNOWN) {
                Event->Status = NKikimrProto::ERROR;
            }
            return;
        }

        TString blobData = event.Data;
        Y_VERIFY(blobData.size() == blobId.BlobSize());

        if (!BlobsToRead.contains(blobId)) {
            return;
        }

        BlobsToRead.erase(blobId);

        Y_VERIFY(Event);
        {
            auto it = Event->Blobs.find(blobId);
            Y_VERIFY(it != Event->Blobs.end());
            it->second = blobData;
        }

        if (BlobsToRead.empty()) {
            SendResultAndDie(ctx);
        }
    }

    void Bootstrap(const TActorContext& /*ctx*/) {
        LOG_S_DEBUG("Exporting " << Event->Blobs.size() << " blobs at tablet " << TabletId);

        SendReads();
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            default:
                break;
        }
    }

private:
    ui64 TabletId;
    TActorId Parent;
    TActorId BlobCacheActorId;
    std::unique_ptr<TEvPrivate::TEvExport> Event;
    THashSet<TUnifiedBlobId> BlobsToRead;

    void SendReads() {
        for (auto& [blobId, _] : Event->Blobs) {
            BlobsToRead.emplace(blobId);
            SendReadRequest(NBlobCache::TBlobRange(blobId, 0, blobId.BlobSize()));
        }
    }

    void SendReadRequest(const NBlobCache::TBlobRange& blobRange) {
        Y_VERIFY(!blobRange.Offset);
        Y_VERIFY(blobRange.Size);

        NBlobCache::TReadBlobRangeOptions readOpts {
            .CacheAfterRead = false,
            .ForceFallback = false,
            .IsBackgroud = true
        };
        Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRange(blobRange, std::move(readOpts)));
    }

    void SendResultAndDie(const TActorContext& ctx) {
        auto s3Actor = Event->DstActor;
        Event->DstActor = Parent;
        ctx.Send(s3Actor, Event.release());
        Die(ctx);
    }
};

} // namespace

IActor* CreateExportActor(const ui64 tabletId, const TActorId& dstActor, TAutoPtr<TEvPrivate::TEvExport> ev) {
    return new TExportActor(tabletId, dstActor, ev);
}

}
