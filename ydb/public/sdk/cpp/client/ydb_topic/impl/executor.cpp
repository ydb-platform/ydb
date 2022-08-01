#include "executor.h"

namespace NYdb::NTopic {

void IAsyncExecutor::Post(TFunction&& f) {
    PostImpl(std::move(f));
}

IAsyncExecutor::TPtr CreateDefaultExecutor() {
    return CreateThreadPoolExecutor(1);
}

void TThreadPoolExecutor::PostImpl(TVector<TFunction>&& fs) {
    for (auto& f : fs) {
        ThreadPool->SafeAddFunc(std::move(f));
    }
}

void TThreadPoolExecutor::PostImpl(TFunction&& f) {
    ThreadPool->SafeAddFunc(std::move(f));
}

TSerialExecutor::TSerialExecutor(IAsyncExecutor::TPtr executor)
    : Executor(executor)
{
    Y_VERIFY(executor);
}

void TSerialExecutor::PostImpl(TVector<TFunction>&& fs) {
    for (auto& f : fs) {
        PostImpl(std::move(f));
    }
}

void TSerialExecutor::PostImpl(TFunction&& f) {
    with_lock(Mutex) {
        ExecutionQueue.push(std::move(f));
        if (Busy) {
            return;
        }
        PostNext();
    }
}

void TSerialExecutor::PostNext() {
    Y_VERIFY(!Busy);

    if (ExecutionQueue.empty()) {
        return;
    }

    auto weakThis = weak_from_this();
    Executor->Post([weakThis, f = std::move(ExecutionQueue.front())]() {
        if (auto sharedThis = weakThis.lock()) {
            f();
            with_lock(sharedThis->Mutex) {
                sharedThis->Busy = false;
                sharedThis->PostNext();
            }
        }
    });
    ExecutionQueue.pop();
    Busy = true;
}

IExecutor::TPtr CreateThreadPoolExecutor(size_t threads) {
    return MakeIntrusive<TThreadPoolExecutor>(threads);
}

IExecutor::TPtr CreateGenericExecutor() {
    return CreateThreadPoolExecutor(1);
}

IExecutor::TPtr CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool) {
    return MakeIntrusive<TThreadPoolExecutor>(std::move(threadPool));
}

TThreadPoolExecutor::TThreadPoolExecutor(std::shared_ptr<IThreadPool> threadPool)
    : ThreadPool(std::move(threadPool))
{
    IsFakeThreadPool = dynamic_cast<TFakeThreadPool*>(ThreadPool.get()) != nullptr;
}

TThreadPoolExecutor::TThreadPoolExecutor(size_t threadsCount)
    : TThreadPoolExecutor(CreateThreadPool(threadsCount))
{
    Y_VERIFY(threadsCount > 0);
    ThreadsCount = threadsCount;
}

}
