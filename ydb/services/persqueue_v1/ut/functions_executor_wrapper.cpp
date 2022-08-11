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
    }
}

void FunctionExecutorWrapper::DoStart()
{
    Executor->Start();
}

void FunctionExecutorWrapper::StartFuncs(const std::vector<size_t>& indicies)
{
    with_lock (Mutex) {
        for (auto index : indicies) {
            Y_VERIFY(index < Funcs.size());
            Y_VERIFY(Funcs[index]);

            Executor->Post(std::move(Funcs[index]));
        }
    }
}

size_t FunctionExecutorWrapper::GetFuncsCount() const
{
    with_lock (Mutex) {
        return Funcs.size();
    }
}

TIntrusivePtr<FunctionExecutorWrapper> CreateThreadPoolExecutorWrapper(size_t threads)
{
    return MakeIntrusive<FunctionExecutorWrapper>(NYdb::NPersQueue::CreateThreadPoolExecutor(threads));
}

TIntrusivePtr<FunctionExecutorWrapper> CreateSyncExecutorWrapper()
{
    return MakeIntrusive<FunctionExecutorWrapper>(NYdb::NPersQueue::CreateSyncExecutor());
}

}
