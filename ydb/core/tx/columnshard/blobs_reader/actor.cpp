#include "actor.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

void TActor::Handle(TEvStartReadTask::TPtr& ev) {
    ACFL_DEBUG("event", "TEvReadTask");
    Y_VERIFY(ev->Get()->GetTask());
    for (auto&& [uBlobId, ranges] : ev->Get()->GetTask()->GetBlobsGroupped()) {
        for (auto&& bRange : ranges) {
            BlobTasks[bRange].emplace_back(ev->Get()->GetTask());
        }
        NBlobCache::TReadBlobRangeOptions readOpts{.CacheAfterRead = false, .ForceFallback = false, .IsBackgroud = true, .WithDeadline = true};
        Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(ranges), std::move(readOpts)));
    }
    ev->Get()->GetTask()->StartBlobsFetching();
}

void TActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    ACFL_TRACE("event", "TEvReadBlobRangeResult");

    auto& event = *ev->Get();
    auto it = BlobTasks.find(event.BlobRange);
    Y_VERIFY(it != BlobTasks.end());
    for (auto&& i : it->second) {
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            i->AddError(event.BlobRange, ITask::TErrorStatus::Fail(event.Status, "cannot get blob"));
        } else {
            i->AddData(event.BlobRange, event.Data);
        }
    }
    BlobTasks.erase(it);
}

TActor::TActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , Parent(parent)
    , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
{

}

}
