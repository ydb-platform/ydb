#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/runtime/runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <util/generic/scope.h>

namespace NYdb::inline Dev {

namespace {

thread_local unsigned int SdkRuntimeCallbackDepth = 0;
thread_local TSdkRuntime* InitializingRuntime = nullptr;
alignas(std::max_align_t) char RuntimeInitializingMarker;

} // anonymous namespace

struct TSdkRuntime::TImpl {
    explicit TImpl(IExecutor::TPtr executor)
        : Executor(std::move(executor))
    {
    }

    IExecutor::TPtr Executor;
};

void TSdkRuntime::SetExecutor(IExecutor::TPtr executor) {
    GetOrInitialize(std::move(executor));
}

TSdkRuntime::TImpl* TSdkRuntime::GetOrInitialize(
    IExecutor::TPtr executor,
    std::size_t threadCount,
    std::size_t maxQueueSize)
{
    auto* const initializing = reinterpret_cast<TImpl*>(&RuntimeInitializingMarker);

    while (true) {
        auto* impl = Impl_.load(std::memory_order_acquire);
        if (impl == initializing) {
            if (InitializingRuntime == this) {
                throw TContractViolation(
                    "The YDB SDK runtime cannot be used while its executor is starting");
            }
            Impl_.wait(impl, std::memory_order_acquire);
            continue;
        }
        if (impl) {
            if (executor && impl->Executor != executor) {
                throw TContractViolation(
                    "The process-wide YDB SDK executor has already been configured with another instance");
            }
            return impl;
        }

        if (!Impl_.compare_exchange_weak(
                impl,
                initializing,
                std::memory_order_acq_rel,
                std::memory_order_acquire))
        {
            continue;
        }

        try {
            auto holder = std::make_unique<TImpl>(
                executor ? std::move(executor) : CreateThreadPoolExecutor(threadCount, maxQueueSize));
            InitializingRuntime = this;
            try {
                holder->Executor->Start();
            } catch (...) {
                InitializingRuntime = nullptr;
                throw;
            }
            InitializingRuntime = nullptr;
            impl = holder.release();
            Impl_.store(impl, std::memory_order_release);
            Impl_.notify_all();
            return impl;
        } catch (...) {
            Impl_.store(nullptr, std::memory_order_release);
            Impl_.notify_all();
            throw;
        }
    }
}

void TSdkRuntime::Configure(
    IExecutor::TPtr executor,
    std::size_t threadCount,
    std::size_t maxQueueSize)
{
    GetOrInitialize(std::move(executor), threadCount, maxQueueSize);
}

void TSdkRuntime::PostImpl(IExecutor::TFunction&& function) {
    GetOrInitialize()->Executor->Post([function = std::move(function)]() mutable {
        ++SdkRuntimeCallbackDepth;
        Y_SCOPE_EXIT() {
            --SdkRuntimeCallbackDepth;
        };
        function();
    });
}

bool IsSdkRuntimeCallback() noexcept {
    return SdkRuntimeCallbackDepth != 0;
}

TSdkRuntime& GetSdkRuntime() {
    constinit static TSdkRuntime runtime;
    return runtime;
}

} // namespace NYdb::inline Dev
