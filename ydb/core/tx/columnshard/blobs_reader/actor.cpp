#include "actor.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>
#include <ydb/library/actors/struct_log/log_stack.h>

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
        WaitingBlobsCount.Sub(Task->GetWaitingRangesCount());
        if (!Task->AddError(event.DataSourceId, event.BlobRange,
                IBlobsReadingAction::TErrorStatus::Fail(
                    event.Status, "cannot get blob: " + event.Data.substr(0, 1024) + ", detailed error: " + event.DetailedError))) {
            aborted = true;
        }
    } else {
        WaitingBlobsCount.Dec();
        Task->AddData(event.DataSourceId, event.BlobRange, event.Data);
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
    NActors::NStructuredLog::TLogStack::TLogGuard gLogContext;
    YDB_LOG_UPDATE_CONTEXT(
        {"external_task_id", externalTaskId});
    Task->StartBlobsFetching({});
    ACFL_DEBUG("task", Task->DebugString());
    WaitingBlobsCount.Add(Task->GetWaitingRangesCount());
    Become(&TThis::StateWait);
    if (Task->IsFinished()) {
        PassAway();
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
