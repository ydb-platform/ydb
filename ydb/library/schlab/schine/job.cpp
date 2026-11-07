#include "job.h"
#include <new>

namespace NKikimr::NSchLab {

TJobPool::~TJobPool() {
    while (!Free.Empty()) {
        delete Free.PopFront();
    }
}

TIntrusivePtr<TJob> TJobPool::Acquire() {
    TJob* job;
    if (Free.Empty()) {
        job = new TJob;
        ++Created;
    } else {
        job = Free.PopFront();
        // Reconstruct all fields, including refcount and list hook.
        job->~TJob();
        job = new (job) TJob;
    }
    job->Pool = this;
    Ref(); // One pool reference per checked-out job, not per job reference.
    return job;
}

void TJobPool::Release(TJob* job) noexcept {
    Y_ABORT_UNLESS(job->RefCount() == 0 && job->Empty());
    Free.PushBack(job);
    UnRef(); // May destroy the pool and its free jobs, including this one.
}

void TRecycleJob::Destroy(TJob* job) noexcept {
    if (auto* pool = job->Pool) {
        pool->Release(job);
    } else {
        delete job;
    }
}

} // namespace NKikimr::NSchLab
