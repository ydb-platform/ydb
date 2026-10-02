#pragma once

#include "public.h"

#include "executor_pool.h"

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>

namespace NYdb::NBS {

// Runs `func` on the executor thread and returns a future with its result.
template <typename TFunc>
auto RunOnExecutor(
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

// Drains the executor: schedules an empty task and waits for it to finish. By
// the time it returns, every task queued before it has already run.
inline void DrainExecutor(
    const TExecutorPtr& executor,
    const TDuration& timeout = TDuration::Seconds(10))
{
    RunOnExecutor(executor, []() { return true; }).GetValue(timeout);
}

}   // namespace NYdb::NBS
