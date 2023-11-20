#include "read_coordinator.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

TAtomicCounter TReadCoordinatorActor::WaitingBlobsCount = 0;

void TReadCoordinatorActor::Handle(TEvStartReadTask::TPtr& ev) {
    const auto& externalTaskId = ev->Get()->GetTask()->GetExternalTaskId();
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("external_task_id", externalTaskId);
    THashSet<TBlobRange> rangesInProgress;
    for (auto&& agent : ev->Get()->GetTask()->GetAgents()) {
        for (auto&& b : agent->GetRangesForRead()) {
            for (auto&& r : b.second) {
                auto it = BlobTasks.find(r);
                if (it != BlobTasks.end()) {
                    ACFL_DEBUG("event", "TEvReadTask")("enqueued_blob_id", r);
                    rangesInProgress.emplace(r);
                } else {
                    ACFL_DEBUG("event", "TEvReadTask")("blob_id", r);
                    it = BlobTasks.emplace(r, std::vector<std::shared_ptr<ITask>>()).first;
                    WaitingBlobsCount.Inc();
                }
                it->second.emplace_back(ev->Get()->GetTask());
            }
        }
    }
    ev->Get()->GetTask()->StartBlobsFetching(rangesInProgress);
    ACFL_DEBUG("task", ev->Get()->GetTask()->DebugString());
    AFL_VERIFY(ev->Get()->GetTask()->GetAllRangesSize());
}

void TReadCoordinatorActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    ACFL_TRACE("event", "TEvReadBlobRangeResult")("blob_id", ev->Get()->BlobRange);

    auto& event = *ev->Get();
    auto it = BlobTasks.find(event.BlobRange);
    AFL_VERIFY(it != BlobTasks.end())("blob_id", event.BlobRange);
    for (auto&& i : it->second) {
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            i->AddError(event.BlobRange, IBlobsReadingAction::TErrorStatus::Fail(event.Status, "cannot get blob"));
        } else {
            i->AddData(event.BlobRange, event.Data);
        }
    }
    WaitingBlobsCount.Dec();
    BlobTasks.erase(it);
}

TReadCoordinatorActor::TReadCoordinatorActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , Parent(parent) {

}

TReadCoordinatorActor::~TReadCoordinatorActor() {
    for (auto&& i : BlobTasks) {
        for (auto&& t : i.second) {
            t->Abort();
        }
    }
}

}
