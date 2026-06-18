#pragma once

#include "public.h"

#include "executor_pool.h"

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

inline constexpr auto ExecutorTestWaitTimeout = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

// Runs `func` on the executor thread and returns a future with its result.
// `func` may itself return a future (i.e. the outer future stays unresolved as
// long as `func` is suspended inside Executor->WaitFor), which is exactly what
// the session-blocking tests rely on.
template <typename TFunc>
auto InvokeOnExecutor(
    const TExecutorPtr& executor,
    TFunc func) -> NThreading::TFuture<decltype(func())>
{
    auto promise = NThreading::NewPromise<decltype(func())>();
    auto future = promise.GetFuture();
    executor->ExecuteSimple(
        [promise = std::move(promise), func = std::move(func)]() mutable
        { promise.SetValue(func()); });
    return future;
}

// Hops onto the executor thread, runs func there and brings its result back.
// Methods touching executor-guarded internals assert they run on the executor
// thread, so reads/writes of those internals must be marshalled through here.
template <typename TFunc>
auto RunOnExecutor(const TExecutorPtr& executor, TFunc func) -> decltype(func())
{
    using TResult = decltype(func());
    auto promise = NThreading::NewPromise<TResult>();
    auto future = promise.GetFuture();
    executor->ExecuteSimple(
        [promise = std::move(promise), func = std::move(func)]() mutable
        { promise.SetValue(func()); });
    return future.GetValue(ExecutorTestWaitTimeout);
}

// Drains the executor: schedules an empty task and waits for it to finish. By
// the time it returns, every task queued before it has already run.
inline void DrainExecutor(const TExecutorPtr& executor)
{
    auto promise = NThreading::NewPromise<void>();
    auto future = promise.GetFuture();
    executor->ExecuteSimple([promise = std::move(promise)]() mutable
                            { promise.SetValue(); });
    future.GetValue(ExecutorTestWaitTimeout);
}

}   // namespace NYdb::NBS
