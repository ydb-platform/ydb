#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/thread_pool/pool.h>
#undef INCLUDE_YDB_INTERNAL_H


namespace NYdb::inline Dev {

class TThreadPoolExecutor : public IExecutor {
public:
    TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool)
        : ThreadPool_(threadPool)
        , IsStarted_(true)
        , DontStop_(true)
    {
    }

    TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize)
        : ThreadPool_(threadPool)
        , IsStarted_(false)
        , DontStop_(false)
        , ThreadCount_(threadCount)
        , MaxQueueSize_(maxQueueSize)
    {
    }

    void DoStart() override {
        if (IsStarted_) {
            return;
        }
        ThreadPool_->Start(ThreadCount_, MaxQueueSize_);
    }

    void Stop() override {
        if (DontStop_) {
            return;
        }
        ThreadPool_->Stop();
    }

    void Post(std::function<void()>&& f) override {
        ThreadPool_->SafeAddFunc(std::move(f));
    }

    bool IsAsync() const override {
        return true;
    }

private:
    std::shared_ptr<IThreadPool> ThreadPool_;

    const bool IsStarted_;
    const bool DontStop_;
    const std::size_t ThreadCount_ = 0;
    const std::size_t MaxQueueSize_ = 0;
};

std::shared_ptr<IExecutor> CreateThreadPoolExecutor(std::size_t threadCount, std::size_t maxQueueSize) {
    return std::make_shared<TThreadPoolExecutor>(CreateThreadPool(threadCount), threadCount, maxQueueSize);
}

std::shared_ptr<IExecutor> CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize) {
    return std::make_shared<TThreadPoolExecutor>(threadPool, threadCount, maxQueueSize);
}

std::shared_ptr<IExecutor> CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool) {
    return std::make_shared<TThreadPoolExecutor>(threadPool);
}

} // namespace NYdb::inline Dev
