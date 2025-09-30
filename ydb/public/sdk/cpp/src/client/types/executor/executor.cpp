#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/executor/executor_impl.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/thread_pool/pool.h>
#undef INCLUDE_YDB_INTERNAL_H


namespace NYdb::inline Dev {

std::shared_ptr<IExecutor> CreateThreadPoolExecutor(std::size_t threadCount, std::size_t maxQueueSize) {
    return std::make_shared<TThreadPoolExecutor>(CreateThreadPool(threadCount), threadCount, maxQueueSize);
}

#ifndef YDB_SDK_OSS
std::shared_ptr<IExecutor> CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool, std::size_t threadCount, std::size_t maxQueueSize) {
    return std::make_shared<TThreadPoolExecutor>(threadPool, threadCount, maxQueueSize);
}

std::shared_ptr<IExecutor> CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool) {
    return std::make_shared<TThreadPoolExecutor>(threadPool);
}
#endif

}
