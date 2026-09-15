#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#include <util/thread/pool.h>


namespace NYdb::inline V3 {

class TThreadPoolExecutor : public IExecutor {
public:
    TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool);
    TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize);

    void DoStart() override;
    void Stop() override;
    void Post(std::function<void()>&& f) override;
    bool IsAsync() const override;

private:
    std::shared_ptr<IThreadPool> ThreadPool_;

    const bool IsStarted_;
    const bool DontStop_;
    const std::size_t ThreadCount_ = 0;
    const std::size_t MaxQueueSize_ = 0;
};

}
