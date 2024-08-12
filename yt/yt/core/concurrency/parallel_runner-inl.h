#ifndef PARALLEL_RUNNER_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_runner.h"
// For the sake of sane code completion.
#include "parallel_runner.h"
#endif
#undef PARALLEL_RUNNER_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParallelRunner<T>::TParallelRunner(
    IInvokerPtr invoker,
    i64 batchSize)
    : Invoker_(std::move(invoker))
    , BatchSize_(batchSize)
{ }

template <class T>
TParallelRunner<T> TParallelRunner<T>::CreateAsync(
    IInvokerPtr invoker,
    i64 batchSize)
{
    return TParallelRunner(std::move(invoker), batchSize);
}

template <class T>
TParallelRunner<T> TParallelRunner<T>::CreateSync()
{
    return TParallelRunner(
        /*invoker*/ nullptr,
        /*batchSize*/ std::numeric_limits<i64>::max());
}

template <class T>
void TParallelRunner<T>::Add(T item)
{
    CurrentBatch_.push_back(std::move(item));
    if (std::ssize(CurrentBatch_) >= BatchSize_) {
        Batches_.push_back(std::exchange(CurrentBatch_, {}));
    }
}

template <class T>
template <class F>
TFuture<void> TParallelRunner<T>::Run(F func)
{
    if (Invoker_) {
        if (!CurrentBatch_.empty()) {
            Batches_.push_back(std::move(CurrentBatch_));
        }

        std::vector<TFuture<void>> futures;
        futures.reserve(Batches_.size());
        for (auto& batch : Batches_) {
            futures.push_back(
                BIND([func, batch = std::move(batch)] () mutable {
                    for (auto& item : batch) {
                        func(item);
                    }
                })
                .AsyncVia(Invoker_)
                .Run());
        }

        return AllSucceeded(std::move(futures));
    } else {
        for (auto& item : CurrentBatch_) {
            func(item);
        }
        return VoidFuture;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
