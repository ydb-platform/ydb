#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

namespace NYdb::inline Dev {

class TThreadPoolExecutor : public IExecutor {
public:
    TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool)
        : ThreadPool(threadPool)
    {
    }

    void Post(std::function<void()>&& f) override {
        ThreadPool->SafeAddFunc(std::move(f));
    }

private:
    std::shared_ptr<IThreadPool> ThreadPool;
};

std::shared_ptr<IExecutor> CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool) {
    return std::make_shared<TThreadPoolExecutor>(threadPool);
}

} // namespace NYdb::inline Dev
