#include "functions_executor_wrapper.h"

namespace NKikimr::NPersQueueTests {

FunctionExecutorWrapper::FunctionExecutorWrapper(TExecutorPtr executor) :
    Executor{std::move(executor)}
{
}

bool FunctionExecutorWrapper::IsAsync() const
{
    return Executor->IsAsync();
}

void FunctionExecutorWrapper::Post(TFunction &&f)
{
    with_lock (Mutex) {
        Funcs.push_back(std::move(f));
        ++Planned;
    }
}

void FunctionExecutorWrapper::DoStart()
{
    Executor->Start();
}

void FunctionExecutorWrapper::Stop()
{
    Executor->Stop();
}

auto FunctionExecutorWrapper::MakeTask(TFunction func) -> TFunction
{
    return [this, func = std::move(func)]() {
        ++Running;

        func();

        --Running;
        ++Executed;
    };
}

void FunctionExecutorWrapper::RunTask(TFunction&& func)
{
    Y_ABORT_UNLESS(Planned > 0);
    --Planned;
    Executor->Post(MakeTask(std::move(func)));
}

void FunctionExecutorWrapper::StartFuncs(const std::vector<size_t>& indicies)
{
    with_lock (Mutex) {
        for (auto index : indicies) {
            Y_ABORT_UNLESS(index < Funcs.size());
            Y_ABORT_UNLESS(Funcs[index]);

            RunTask(std::move(Funcs[index]));
        }
    }
}

size_t FunctionExecutorWrapper::GetFuncsCount() const
{
    with_lock (Mutex) {
        return Funcs.size();
    }
}

size_t FunctionExecutorWrapper::GetPlannedCount() const
{
    return Planned;
}

size_t FunctionExecutorWrapper::GetRunningCount() const
{
    return Running;
}

size_t FunctionExecutorWrapper::GetExecutedCount() const
{
    return Executed;
}

void FunctionExecutorWrapper::RunAllTasks()
{
    with_lock (Mutex) {
        for (auto& func : Funcs) {
            if (func) {
                RunTask(std::move(func));
            }
        }
    }
}

std::shared_ptr<FunctionExecutorWrapper> CreateThreadPoolExecutorWrapper(size_t threads)
{
    return std::make_shared<FunctionExecutorWrapper>(NYdb::CreateThreadPoolExecutor(threads));
}

std::shared_ptr<FunctionExecutorWrapper> CreateSyncExecutorWrapper()
{
    return std::make_shared<FunctionExecutorWrapper>(NYdb::NTopic::CreateSyncExecutor());
}

}
