#include "managed_executor.h"


namespace NYdb::inline Dev::NTopic::NTests {

TManagedExecutor::TManagedExecutor(TExecutorPtr executor) :
    Executor{std::move(executor)}
{
}

bool TManagedExecutor::IsAsync() const
{
    return Executor->IsAsync();
}

void TManagedExecutor::Post(TFunction &&f)
{
    std::lock_guard lock(Mutex);

    Funcs.push_back(std::move(f));
    ++Planned;
}

void TManagedExecutor::DoStart()
{
    Executor->Start();
}

auto TManagedExecutor::MakeTask(TFunction func) -> TFunction
{
    return [this, func = std::move(func)]() {
        ++Running;

        func();

        --Running;
        ++Executed;
    };
}

void TManagedExecutor::RunTask(TFunction&& func)
{
    Y_ABORT_UNLESS(Planned > 0);
    --Planned;
    Executor->Post(MakeTask(std::move(func)));
}

void TManagedExecutor::StartFuncs(const std::vector<size_t>& indicies)
{
    std::lock_guard lock(Mutex);

    for (auto index : indicies) {
            Y_ABORT_UNLESS(index < Funcs.size());
            Y_ABORT_UNLESS(Funcs[index]);

        RunTask(std::move(Funcs[index]));
    }
}

size_t TManagedExecutor::GetFuncsCount() const
{
    std::lock_guard lock(Mutex);

    return Funcs.size();
}

size_t TManagedExecutor::GetPlannedCount() const
{
    return Planned;
}

size_t TManagedExecutor::GetRunningCount() const
{
    return Running;
}

size_t TManagedExecutor::GetExecutedCount() const
{
    return Executed;
}

void TManagedExecutor::RunAllTasks()
{
    std::lock_guard lock(Mutex);

    for (auto& func : Funcs) {
        if (func) {
            RunTask(std::move(func));
        }
    }
}

TIntrusivePtr<TManagedExecutor> CreateThreadPoolManagedExecutor(size_t threads)
{
    return MakeIntrusive<TManagedExecutor>(NYdb::NTopic::CreateThreadPoolExecutor(threads));
}

TIntrusivePtr<TManagedExecutor> CreateSyncManagedExecutor()
{
    return MakeIntrusive<TManagedExecutor>(NYdb::NTopic::CreateSyncExecutor());
}

}
