#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>

#include <library/cpp/threading/future/future.h>

#include <util/thread/pool.h>

#include <chrono>
#include <exception>
#include <functional>
#include <type_traits>
#include <utility>

namespace NYdb::inline Dev {

// Process-wide services. Tasks and timers are independent of driver lifetimes.
class TRuntime final {
public:
    using TTask = std::function<void()>;

    // Submit to the shared background pool. Tasks may run concurrently and are
    // never invoked inline. The pool is independent of driver response executors.
    void Post(TTask task) const;

    // Submit to the background pool once the deadline expires.
    void Schedule(TDeadline deadline, TTask task) const;
    template <class Rep, class Period>
    void Schedule(std::chrono::duration<Rep, Period> delay, TTask task) const {
        Schedule(TDeadline::AfterDuration(delay), std::move(task));
    }
    void Schedule(TDuration delay, TTask task) const;

    // Run the callable on a background worker after the deadline. Arguments are
    // stored by value and moved into the call; use std::ref for references.
    // Returned SDK futures are unwrapped without blocking; their values are
    // extracted and transferred to the result future.
    template <class F, class... TArgs>
    auto ScheduleFuture(TDeadline deadline, F&& function, TArgs&&... args) const {
        auto callable = std::bind_front(std::forward<F>(function), std::forward<TArgs>(args)...);
        using TCallResult = decltype(std::move(callable)());
        using TResult = NThreading::TFutureType<std::remove_cvref_t<TCallResult>>;
        auto promise = NThreading::NewPromise<TResult>();
        auto task = [promise, callable = std::move(callable)]() mutable {
            try {
                if constexpr (std::is_void_v<TCallResult>) {
                    std::move(callable)();
                    promise.SetValue();
                } else {
                    SetFutureResult(promise, std::move(callable)());
                }
            } catch (...) {
                if (!promise.TrySetException(std::current_exception())) {
                    throw;
                }
            }
        };
        ScheduleTask(deadline, THolder<IObjectInQueue>(MakeThrFuncObj(std::move(task))),
            [promise](std::exception_ptr error) mutable {
                try {
                    promise.SetException(std::move(error));
                } catch (...) { // NOLINT(bugprone-empty-catch): the error is stored; subscriber exceptions must not unwind the network thread.
                }
            });
        return promise.GetFuture();
    }

    NThreading::TFuture<void> ScheduleFuture(TDeadline deadline) const {
        return ScheduleFuture(deadline, [] {});
    }

    template <class Rep, class Period, class... TArgs>
    auto ScheduleFuture(std::chrono::duration<Rep, Period> delay, TArgs&&... args) const {
        return ScheduleFuture(TDeadline::AfterDuration(delay), std::forward<TArgs>(args)...);
    }

    template <class... TArgs>
    auto ScheduleFuture(TDuration delay, TArgs&&... args) const {
        return ScheduleFuture(TDeadline::AfterDuration(delay), std::forward<TArgs>(args)...);
    }

private:
    template <class T, class V>
    static void SetFutureResult(NThreading::TPromise<T>& promise, V&& value) {
        using TValue = std::remove_cvref_t<V>;
        if constexpr (std::is_same_v<TValue, NThreading::TFutureType<TValue>>) {
            promise.SetValue(std::forward<V>(value));
        } else {
            value.Subscribe([promise](const TValue& ready) mutable {
                try {
                    using TInner = typename TValue::value_type;
                    if constexpr (std::is_void_v<TInner>) {
                        ready.GetValue();
                        promise.SetValue();
                    } else {
                        auto future = ready;
                        SetFutureResult(promise, future.ExtractValue());
                    }
                } catch (...) {
                    if (!promise.TrySetException(std::current_exception())) {
                        throw;
                    }
                }
            });
        }
    }

    void ScheduleTask(TDeadline deadline, THolder<IObjectInQueue> task,
        std::function<void(std::exception_ptr)> onError = {}) const;

    TRuntime() = default;
    TRuntime(const TRuntime&) = delete;
    TRuntime& operator=(const TRuntime&) = delete;

    friend TRuntime& GetRuntime();
};

// The runtime remains available until process exit, including before the first
// driver is constructed. It does not use the driver response executor.
TRuntime& GetRuntime();

} // namespace NYdb
