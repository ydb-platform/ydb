#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#include <util/thread/pool.h>

namespace NYdb::NAdapters {

// Create executor adapter for util thread pool.
// Thread pool is started and stopped by SDK.
IExecutor::TPtr CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize = 0);

// Create executor adapter for util thread pool.
// Thread pool is expected to have been started and stopped by user.
IExecutor::TPtr CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool);

} // namespace NYdb
