#pragma once

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

} // namespace NYdb
