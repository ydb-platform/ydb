#pragma once

#include <util/thread/pool.h>

#include <functional>

namespace NYdb::inline Dev::NExec {

class IExecutor {
public:
    virtual void Post(std::function<void()>&& f) = 0;

    virtual ~IExecutor() = default;
};

// Create executor adapter for util thread pool.
// Thread pool is expected to have been started.
std::shared_ptr<IExecutor> CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool);

} // namespace NYdb::inline Dev
