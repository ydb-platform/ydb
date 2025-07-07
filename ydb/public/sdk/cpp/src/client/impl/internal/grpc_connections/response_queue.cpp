#define INCLUDE_YDB_INTERNAL_H
#include "response_queue.h"

#include <ydb/public/sdk/cpp/src/client/impl/internal/thread_pool/pool.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>


namespace NYdb::inline Dev {

class TOwnedResponseQueue: public IResponseQueue {
public:
    TOwnedResponseQueue(size_t clientThreadsNum, size_t maxQueuedResponses)
        : ClientThreadsNum_(clientThreadsNum)
        , MaxQueuedResponses_(maxQueuedResponses)
        , ThreadPool_(CreateThreadPool(clientThreadsNum))
    {
    }

    void Start() override {
        ThreadPool_->Start(ClientThreadsNum_, MaxQueuedResponses_);
    }

    void Stop() override {
        ThreadPool_->Stop();
    }

    void Post(IObjectInQueue* action) override {
        Y_ENSURE(ThreadPool_->Add(action));
    }

private:
    const std::size_t ClientThreadsNum_;
    const std::size_t MaxQueuedResponses_;

    std::unique_ptr<IThreadPool> ThreadPool_;
};

class TExecutorResponseQueue : public IResponseQueue {
public:
    TExecutorResponseQueue(std::shared_ptr<IExecutor> executor)
        : Executor_(executor)
    {
    }

    void Start() override { }

    void Stop() override { }

    void Post(IObjectInQueue* action) override {
        Executor_->Post([action]() {
            action->Process(nullptr);
        });
    }

private:
    std::shared_ptr<IExecutor> Executor_;
};

std::unique_ptr<IResponseQueue> CreateResponseQueue(std::shared_ptr<IConnectionsParams> params) {
    if (params->GetExecutor()) {
        return std::make_unique<TExecutorResponseQueue>(params->GetExecutor());
    }
    return std::make_unique<TOwnedResponseQueue>(params->GetClientThreadsNum(), params->GetMaxQueuedResponses());
}

} // namespace NYdb
