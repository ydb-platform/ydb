#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Groups given items into batches (of a given size) and invokes
//! a function for each batch in parallel.
template <class T>
class TParallelRunner
{
public:
    static TParallelRunner CreateAsync(
        IInvokerPtr invoker,
        i64 batchSize);
    static TParallelRunner CreateSync();

    //! Registers an item to be processed.
    void Add(T item);

    //! Runs #func for each registered item.
    /*!
    *  For a runner created via #CreateAsync, function invocations happen asynchronously
    *  in a given invoker.
    *
    *  For a runner created via #CreateSync, these invocations happen synchronously
    *  in the current thread.
    */
    template <class F>
    TFuture<void> Run(F func);

private:
    TParallelRunner(
        IInvokerPtr invoker,
        i64 batchSize);

    const IInvokerPtr Invoker_;
    const i64 BatchSize_;

    std::vector<std::vector<T>> Batches_;
    std::vector<T> CurrentBatch_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define PARALLEL_RUNNER_INL_H_
#include "parallel_runner-inl.h"
#undef PARALLEL_RUNNER_INL_H_
