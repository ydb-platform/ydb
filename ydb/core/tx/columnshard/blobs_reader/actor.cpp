#include "actor.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

TAtomicCounter TActor::WaitingBlobsCount = 0;

void TActor::Handle(TEvStartReadTask::TPtr& ev) {
    THashSet<TBlobRange> rangesInProgress;
    for (auto&& agent : ev->Get()->GetTask()->GetAgents()) {
        for (auto&& b : agent->GetRangesForRead()) {
            for (auto&& r : b.second) {
                auto it = BlobTasks.find(r);
                if (it != BlobTasks.end()) {
                    ACFL_DEBUG("event", "TEvReadTask")("enqueued_blob_id", r);
                    rangesInProgress.emplace(r);
                } else {
                    ACFL_TRACE("event", "TEvReadTask")("blob_id", r);
                    it = BlobTasks.emplace(r, std::vector<std::shared_ptr<ITask>>()).first;
                    WaitingBlobsCount.Inc();
                }
                it->second.emplace_back(ev->Get()->GetTask());
            }
        }
    }
    ev->Get()->GetTask()->StartBlobsFetching(rangesInProgress);
    ACFL_DEBUG("task", ev->Get()->GetTask()->DebugString());
    AFL_VERIFY(ev->Get()->GetTask()->GetExpectedBlobsSize());
}

void TActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    ACFL_TRACE("event", "TEvReadBlobRangeResult")("blob_id", ev->Get()->BlobRange);

    auto& event = *ev->Get();
    auto it = BlobTasks.find(event.BlobRange);
    AFL_VERIFY(it != BlobTasks.end())("blob_id", event.BlobRange);
    for (auto&& i : it->second) {
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            i->AddError(event.BlobRange, ITask::TErrorStatus::Fail(event.Status, "cannot get blob"));
        } else {
            i->AddData(event.BlobRange, event.Data);
        }
    }
    WaitingBlobsCount.Dec();
    BlobTasks.erase(it);
}

TActor::TActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , Parent(parent)
    , BlobCacheActorId(NBlobCache::MakeBlobCacheServiceId())
{

}

}
