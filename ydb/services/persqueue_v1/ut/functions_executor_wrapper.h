#pragma once

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <util/system/mutex.h>

#include <vector>

namespace NKikimr::NPersQueueTests {

class FunctionExecutorWrapper : public NYdb::NPersQueue::IExecutor {
public:
    using TExecutorPtr = NYdb::NPersQueue::IExecutor::TPtr;

    explicit FunctionExecutorWrapper(TExecutorPtr executor);

    bool IsAsync() const override;
    void Post(TFunction&& f) override;

    void StartFuncs(const std::vector<size_t>& indicies);

    size_t GetFuncsCount() const;

private:
    void DoStart() override;

    TExecutorPtr Executor;
    TMutex Mutex;
    std::vector<TFunction> Funcs;
};

TIntrusivePtr<FunctionExecutorWrapper> CreateThreadPoolExecutorWrapper(size_t threads);
TIntrusivePtr<FunctionExecutorWrapper> CreateSyncExecutorWrapper();

}
