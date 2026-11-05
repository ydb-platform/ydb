#include "topic_client_settings.h"

#include <ydb/public/sdk/cpp/adapters/executor/executor.h>

#include <util/thread/pool.h>

namespace NYql {

NYdb::NTopic::TTopicClientSettings MakeCommonTopicClientSettings(ui64 handlersExecutorThreadsNum, ui64 compressionExecutorThreadsNum) {
    NYdb::NTopic::TTopicClientSettings settings;

    if (handlersExecutorThreadsNum) {
        auto threadPool = CreateThreadPool(
            handlersExecutorThreadsNum, 0, IThreadPool::TParams().SetThreadNamePrefix("ydb_sdk_client").SetBlocking(true).SetCatching(false));
        settings.DefaultHandlersExecutor(
            NYdb::NAdapters::CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool>(threadPool.Release())));
    }

    if (compressionExecutorThreadsNum) {
        auto threadPool = CreateThreadPool(compressionExecutorThreadsNum, 0,
            IThreadPool::TParams().SetThreadNamePrefix("ydb_sdk_compression").SetBlocking(true).SetCatching(false));
        settings.DefaultCompressionExecutor(
            NYdb::NAdapters::CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool>(threadPool.Release())));
    }

    return settings;
}

}   // namespace NYql
