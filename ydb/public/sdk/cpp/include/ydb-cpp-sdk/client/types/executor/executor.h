#pragma once

#ifndef YDB_SDK_OSS
#include <util/thread/pool.h>
#endif

#include <functional>
#include <memory>
#include <mutex>

namespace NYdb::inline Dev {

class IExecutor {
public:
    using TPtr = std::shared_ptr<IExecutor>;
    using TFunction = std::function<void()>;

    // Start method.
    // This method is idempotent.
    void Start() {
        std::lock_guard guard(StartLock);
        if (!Started) {
            DoStart();
            Started = true;
        }
    }

    virtual void Stop() = 0;

    // Post function to execute.
    virtual void Post(TFunction&& f) = 0;

    // Is executor asynchronous.
    virtual bool IsAsync() const = 0;

    virtual ~IExecutor() = default;

protected:
    virtual void DoStart() = 0;

    bool Started = false;
    std::mutex StartLock;
};

// Create default executor for thread pool.
IExecutor::TPtr CreateThreadPoolExecutor(std::size_t threadCount, std::size_t maxQueueSize = 0);

#ifndef YDB_SDK_OSS
// Create executor adapter for util thread pool.
// Thread pool is started and stopped by SDK.
IExecutor::TPtr CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize = 0);

// Create executor adapter for util thread pool.
// Thread pool is expected to have been started and stopped by user.
IExecutor::TPtr CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool);
#endif

} // namespace NYdb
