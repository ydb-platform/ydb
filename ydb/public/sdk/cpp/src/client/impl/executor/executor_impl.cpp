#define INCLUDE_YDB_INTERNAL_H
#include "executor_impl.h"
#undef INCLUDE_YDB_INTERNAL_H

namespace NYdb::inline V3 {

TThreadPoolExecutor::TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool)
    : ThreadPool_(threadPool)
    , IsStarted_(true)
    , DontStop_(true)
{
}

TThreadPoolExecutor::TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize)
    : ThreadPool_(threadPool)
    , IsStarted_(false)
    , DontStop_(false)
    , ThreadCount_(threadCount)
    , MaxQueueSize_(maxQueueSize)
{
}

void TThreadPoolExecutor::DoStart() {
    if (IsStarted_) {
        return;
    }
    ThreadPool_->Start(ThreadCount_, MaxQueueSize_);
}

void TThreadPoolExecutor::Stop() {
    if (DontStop_) {
        return;
    }
    ThreadPool_->Stop();
}


void TThreadPoolExecutor::Post(std::function<void()>&& f) {
    ThreadPool_->SafeAddFunc(std::move(f));
}

bool TThreadPoolExecutor::IsAsync() const {
    return true;
}

}
