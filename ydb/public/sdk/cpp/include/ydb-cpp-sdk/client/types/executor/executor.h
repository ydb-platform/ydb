#pragma once

#include <util/thread/pool.h>

#include <functional>

namespace NYdb::inline Dev::NExec {

class IExecutor {
public:
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Post(std::function<void()>&& f) = 0;

    virtual ~IExecutor() = default;
};

// Create executor adapter for util thread pool.
std::shared_ptr<IExecutor> CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize = 0);

} // namespace NYdb
