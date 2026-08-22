#include "executor_impl.h"

#include <ydb/public/sdk/cpp/src/client/impl/internal/thread_pool/pool.h>

namespace NYdb::inline Dev::NTopic {

void IAsyncExecutor::Post(TFunction&& f) {
    PostImpl(std::move(f));
}

TSerialExecutor::TSerialExecutor(IAsyncExecutor::TPtr executor)
    : Executor(executor)
{
    Y_ABORT_UNLESS(executor);
}

void TSerialExecutor::PostImpl(std::vector<TFunction>&& fs) {
    for (auto& f : fs) {
        PostImpl(std::move(f));
    }
}

void TSerialExecutor::PostImpl(TFunction&& f) {
    {
        std::lock_guard guard(Mutex);
        ExecutionQueue.push(std::move(f));
        if (Busy) {
            return;
        }
        PostNext();
    }
}

void TSerialExecutor::PostNext() {
    Y_ABORT_UNLESS(!Busy);

    if (ExecutionQueue.empty()) {
        return;
    }

    auto weakThis = weak_from_this();
    Executor->Post([weakThis, f = std::move(ExecutionQueue.front())]() {
        if (auto sharedThis = weakThis.lock()) {
            f();
            {
                std::lock_guard guard(sharedThis->Mutex);
                sharedThis->Busy = false;
                sharedThis->PostNext();
            }
        }
    });
    ExecutionQueue.pop();
    Busy = true;
}

IExecutor::TPtr CreateSyncExecutor()
{
    return std::make_shared<TSyncExecutor>();
}

}  // namespace NYdb::NTopic