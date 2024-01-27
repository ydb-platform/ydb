#include "invoker_pool.h"

#include "future.h"
#include "invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TDiagnosableInvokerPool::TInvokerStatistics TDiagnosableInvokerPool::GetInvokerStatistics(int index) const
{
    return DoGetInvokerStatistics(index);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> SuspendInvokerPool(const ISuspendableInvokerPoolPtr& invokerPool)
{
    const auto size = invokerPool->GetSize();

    std::vector<TFuture<void>> futures;
    futures.reserve(size);
    for (int i = 0; i < size; ++i) {
        futures.push_back(invokerPool->GetInvoker(i)->Suspend());
    }

    return AllSucceeded(std::move(futures));
}

void ResumeInvokerPool(const ISuspendableInvokerPoolPtr& invokerPool)
{
    for (int i = 0, size = invokerPool->GetSize(); i < size; ++i) {
        invokerPool->GetInvoker(i)->Resume();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
