#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/executor.h>

#include <util/system/mutex.h>

#include <vector>

namespace NYdb::NTopic::NTests {

class TManagedExecutor : public IExecutor {
public:
    using TExecutorPtr = IExecutor::TPtr;

    explicit TManagedExecutor(TExecutorPtr executor);

    bool IsAsync() const override;
    void Post(TFunction&& f) override;

<<<<<<< HEAD:ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/managed_executor.h
=======
    void Stop() override;

    void StartRandomFunc();
>>>>>>> 771638ae94f (YQ-5187 fixed hanging in PQ read session (#36220)):ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h
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
