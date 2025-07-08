#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

namespace NYdb::inline Dev::NExec {

class TThreadPoolExecutor : public NExec::IExecutor {
public:
    TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize)
        : ThreadPool_(threadPool)
        , ThreadCount_(threadCount)
        , MaxQueueSize_(maxQueueSize)
    {
    }

    void Start() override {
        ThreadPool_->Start(ThreadCount_, MaxQueueSize_);
    }

    void Stop() override {
        ThreadPool_->Stop();
    }

    void Post(std::function<void()>&& f) override {
        ThreadPool_->SafeAddFunc(std::move(f));
    }

private:
    std::shared_ptr<IThreadPool> ThreadPool_;
    const std::size_t ThreadCount_;
    const std::size_t MaxQueueSize_;
};

std::shared_ptr<NExec::IExecutor> CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize) {
    return std::make_shared<TThreadPoolExecutor>(threadPool, threadCount, maxQueueSize);
}

} // namespace NYdb::inline Dev
