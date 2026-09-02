#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#include <library/cpp/threading/future/future.h>

#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace NYdb::inline Dev {

class TDriver;
class TGRpcConnectionsImpl;

//! Process-wide runtime used to execute asynchronous SDK work.
class TSdkRuntime final {
public:
    //! Select the process-wide executor. The first SetExecutor(), Post(), or driver
    //! construction initializes the runtime; initialization without an executor uses
    //! the default executor. The selected executor is retained for the process lifetime
    //! and is never stopped by the SDK. Selecting a different executor afterward throws
    //! TContractViolation.
    void SetExecutor(IExecutor::TPtr executor);

    //! Execute a callable on the SDK executor and return its result as a future.
    //! This initializes the runtime with the default executor if necessary.
    //! A callable returning TFuture<T> is flattened to TFuture<T>.
    template <class TFunction>
    auto Post(TFunction&& function)
        -> NThreading::TFuture<NThreading::TFutureType<std::invoke_result_t<std::decay_t<TFunction>&>>> {
        using TTask = std::decay_t<TFunction>;
        using TResult = NThreading::TFutureType<std::invoke_result_t<TTask&>>;

        auto promise = NThreading::NewPromise<TResult>();
        auto future = promise.GetFuture();
        try {
            auto task = std::make_shared<TTask>(std::forward<TFunction>(function));
            PostImpl([promise, task = std::move(task)]() mutable {
                NThreading::NImpl::SetValue(promise, [&task]() mutable {
                    return std::invoke(*task);
                });
            });
        } catch (...) {
            promise.TrySetException(std::current_exception());
        }
        return future;
    }

private:
    struct TImpl;

    constexpr TSdkRuntime() noexcept = default;
    TSdkRuntime(const TSdkRuntime&) = delete;
    TSdkRuntime& operator=(const TSdkRuntime&) = delete;

    TImpl* GetOrInitialize(IExecutor::TPtr executor = {}, std::size_t threadCount = 0, std::size_t maxQueueSize = 0);
    void Configure(IExecutor::TPtr executor, std::size_t threadCount, std::size_t maxQueueSize);
    void PostImpl(IExecutor::TFunction&& function);

    friend class TDriver;
    friend class TGRpcConnectionsImpl;
    friend TSdkRuntime& GetSdkRuntime();

    std::atomic<TImpl*> Impl_ = nullptr;
};

TSdkRuntime& GetSdkRuntime();

} // namespace NYdb::inline Dev
