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

    size_t GetPlannedCount() const;
    size_t GetRunningCount() const;
    size_t GetExecutedCount() const;

    void RunAllTasks();

private:
    void DoStart() override;

    TFunction MakeTask(TFunction func);
    void RunTask(TFunction&& func);

    TExecutorPtr Executor;
    TMutex Mutex;
    std::vector<TFunction> Funcs;
    std::atomic<size_t> Planned = 0;
    std::atomic<size_t> Running = 0;
    std::atomic<size_t> Executed = 0;
};

TIntrusivePtr<FunctionExecutorWrapper> CreateThreadPoolExecutorWrapper(size_t threads);
TIntrusivePtr<FunctionExecutorWrapper> CreateSyncExecutorWrapper();

}
