#include "actor.h"

namespace NKikimr::NOlap::NBlobOperations::NRead {

TAtomicCounter TActor::WaitingBlobsCount = 0;

void TActor::Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev) {
    if (!Task) {
        return;
    }
    ACFL_TRACE("event", "TEvReadBlobRangeResult")("blob_id", ev->Get()->BlobRange);

    auto& event = *ev->Get();

    bool aborted = false;
    if (event.Status != NKikimrProto::EReplyStatus::OK) {
        WaitingBlobsCount.Sub(Task->GetWaitingCount());
        if (!Task->AddError(event.BlobRange, IBlobsReadingAction::TErrorStatus::Fail(event.Status, "cannot get blob: " + event.Data.substr(0, 1024)))) {
            aborted = true;
        }
    } else {
        WaitingBlobsCount.Dec();
        Task->AddData(event.BlobRange, event.Data);
    }
    if (aborted || Task->IsFinished()) {
        Task = nullptr;
        PassAway();
    }

}

TActor::TActor(const std::shared_ptr<ITask>& task)
    : Task(task)
{

}

TActor::~TActor() {
    if (Task) {
        Task->Abort();
    }
}

void TActor::Bootstrap() {
    const auto& externalTaskId = Task->GetExternalTaskId();
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("external_task_id", externalTaskId);
    Task->StartBlobsFetching({});
    ACFL_DEBUG("task", Task->DebugString());
    WaitingBlobsCount.Add(Task->GetReadRangesCount());
    AFL_VERIFY(Task->GetAllRangesSize());
    Become(&TThis::StateWait);
    if (Task->IsFinished()) {
        PassAway();
    }
}

}
