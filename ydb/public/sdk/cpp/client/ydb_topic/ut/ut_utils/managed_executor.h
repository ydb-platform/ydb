#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/include/executor.h>

#include <util/system/mutex.h>

#include <vector>

namespace NYdb::NTopic::NTests {

class TManagedExecutor : public IExecutor {
public:
    using TExecutorPtr = IExecutor::TPtr;

    explicit TManagedExecutor(TExecutorPtr executor);

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

TIntrusivePtr<TManagedExecutor> CreateThreadPoolManagedExecutor(size_t threads);
TIntrusivePtr<TManagedExecutor> CreateSyncManagedExecutor();

}
