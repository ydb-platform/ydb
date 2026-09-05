#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>

#include <cstddef>
#include <exception>
#include <functional>
#include <type_traits>
#include <utility>

namespace NYdb::inline Dev {

    class TDriver;
    class TGRpcConnectionsImpl;

    //! Process-wide runtime used to execute asynchronous SDK work.
    class TSdkRuntime final {
    public:
        //! Select the process-wide executor. The first runtime operation or driver construction
        //! initializes the runtime; initialization without an executor uses
        //! the default executor. The selected executor is retained for the process lifetime
        //! and is never stopped by the SDK; the caller must not stop it. Selecting a different
        //! executor afterward throws TContractViolation.
        //! IExecutor::DoStart() must not use the SDK runtime or wait for another thread that does.
        void SetExecutor(IExecutor::TPtr executor);

        //! Execute a callable on the SDK executor and return its result as a future.
        //! This initializes the runtime with the default executor if necessary.
        //! A callable returning TFuture<T> is flattened to TFuture<T>.
        //! The stored callable and result value must be copy and move constructible.
        template <class TFunction>
        auto Post(TFunction&& function) {
            using TTask = std::decay_t<TFunction>;
            using TCallResult = std::remove_cvref_t<std::invoke_result_t<TTask&>>;
            using TResult = decltype(UnwrapFuture(static_cast<TCallResult*>(nullptr)));
            static_assert(std::is_copy_constructible_v<TTask> && std::is_move_constructible_v<TTask>);
            static_assert(std::is_void_v<TResult> ||
                          (std::is_copy_constructible_v<TResult> && std::is_move_constructible_v<TResult>));

            auto promise = NThreading::NewPromise<TResult>();
            try {
                PostImpl([promise, function = std::forward<TFunction>(function)]() mutable {
                    if constexpr (std::is_same_v<TCallResult, NThreading::TFuture<TResult>>) {
                        ForwardFuture(promise, function);
                    } else {
                        NThreading::NImpl::SetValue(promise, function);
                    }
                });
            } catch (...) {
                if (!promise.TrySetException(std::current_exception())) {
                    throw;
                }
            }
            return promise.GetFuture();
        }

        //! Return a future that becomes ready on the SDK executor after delay.
        NThreading::TFuture<void> AsyncSleep(TDuration delay);

        //! Run a task on the SDK executor after every interval, without overlapping calls.
        //! The first call happens after interval. Returning false stops the task and completes
        //! the returned future; exceptions complete it exceptionally. The task may return
        //! either bool or TFuture<bool>.
        template <class TFunction>
        NThreading::TFuture<void> AddPeriodicTask(TFunction&& function, TDuration interval) {
            using TTask = std::decay_t<TFunction>;
            using TCallResult = std::remove_cvref_t<std::invoke_result_t<TTask&>>;
            using TResult = decltype(UnwrapFuture(static_cast<TCallResult*>(nullptr)));
            static_assert(std::is_same_v<TResult, bool>);
            static_assert(std::is_copy_constructible_v<TTask> && std::is_move_constructible_v<TTask>);

            return AddPeriodicTaskImpl(
                [function = std::forward<TFunction>(function)]() mutable {
                    if constexpr (std::is_same_v<TCallResult, NThreading::TFuture<bool>>) {
                        return function();
                    } else {
                        return NThreading::MakeFuture(function());
                    }
                },
                interval);
        }

    private:
        constexpr TSdkRuntime() noexcept = default;
        ~TSdkRuntime() = default;
        TSdkRuntime(const TSdkRuntime&) = delete;
        TSdkRuntime& operator=(const TSdkRuntime&) = delete;

        template <class T>
        static T UnwrapFuture(T*);

        template <class T>
        static T UnwrapFuture(NThreading::TFuture<T>*);

        template <class T, class TFunction>
        static void ForwardFuture(NThreading::TPromise<T> promise,
                                  TFunction& function) {
            try {
                function().Subscribe([promise](const NThreading::TFuture<T>& ready) mutable {
                    if constexpr (std::is_void_v<T>) {
                        NThreading::NImpl::SetValue(promise, [&] {
                            ready.GetValue();
                        });
                    } else {
                        NThreading::NImpl::SetValue(promise, [&]() -> const T& { return ready.GetValue(); });
                    }
                });
            } catch (...) {
                if (!promise.TrySetException(std::current_exception())) {
                    throw;
                }
            }
        }

        NThreading::TFuture<void> AddPeriodicTaskImpl(
            std::function<NThreading::TFuture<bool>()>&& function,
            TDuration interval);
        IExecutor& GetOrInitialize(IExecutor::TPtr executor = {}, std::size_t threadCount = 0, std::size_t maxQueueSize = 0);
        void PostImpl(IExecutor::TFunction&& function);

        friend class TDriver;
        friend class TGRpcConnectionsImpl;
        friend TSdkRuntime& GetSdkRuntime() noexcept;
    };

    TSdkRuntime& GetSdkRuntime() noexcept;

} // namespace NYdb::inline Dev
