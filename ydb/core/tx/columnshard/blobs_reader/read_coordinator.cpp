#include "read_coordinator.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

void TReadCoordinatorActor::Handle(TEvStartReadTask::TPtr& ev) {
    const auto& externalTaskId = ev->Get()->GetTask()->GetExternalTaskId();
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("external_task_id", externalTaskId);
    THashSet<TBlobRange> rangesInProgress;
    BlobTasks.AddTask(ev->Get()->GetTask());
    ev->Get()->GetTask()->StartBlobsFetching(rangesInProgress);
    ACFL_DEBUG("task", ev->Get()->GetTask()->DebugString());
}

void TReadCoordinatorActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    ACFL_TRACE("event", "TEvReadBlobRangeResult")("blob_id", ev->Get()->BlobRange);

    auto& event = *ev->Get();
    auto tasks = BlobTasks.Extract(event.DataSourceId, event.BlobRange);
    for (auto&& i : tasks) {
        if (event.Status != NKikimrProto::EReplyStatus::OK) {
            i->AddError(event.DataSourceId, event.BlobRange, IBlobsReadingAction::TErrorStatus::Fail(event.Status, "cannot get blob"));
        } else {
            i->AddData(event.DataSourceId, event.BlobRange, event.Data);
        }
    }
}

TReadCoordinatorActor::TReadCoordinatorActor(ui64 tabletId, const TActorId& parent)
    : TabletId(tabletId)
    , Parent(parent) {

}

TReadCoordinatorActor::~TReadCoordinatorActor() {
    auto tasks = BlobTasks.ExtractTasksAll();
    for (auto&& i : tasks) {
        i->Abort();
    }
}

}
