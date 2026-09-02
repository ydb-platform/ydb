#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/runtime/runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <util/generic/scope.h>

#include <atomic>
#include <memory>
#include <utility>

namespace NYdb::inline Dev {

    namespace {

        struct TSdkRuntimeImpl {
            explicit TSdkRuntimeImpl(IExecutor::TPtr executor)
                : Executor(std::move(executor)) {
            }

            IExecutor::TPtr Executor;
            TTaskScheduler Scheduler;
        };

        class TPeriodicTask final : public std::enable_shared_from_this<TPeriodicTask> {
        public:
            TPeriodicTask(
                TSdkRuntime& runtime,
                std::function<NThreading::TFuture<bool>()>&& function,
                TDuration interval)
                : Runtime_(runtime)
                , Function_(std::move(function))
                , Interval_(interval)
                , Promise_(NThreading::NewPromise<void>()) {
            }

            NThreading::TFuture<void> Start() {
                auto future = Promise_.GetFuture();
                Sleep();
                return future;
            }

        private:
            void Sleep() {
                try {
                    Runtime_.AsyncSleep(Interval_).Subscribe(
                        [self = shared_from_this()](const NThreading::TFuture<void>& future) {
                            self->Wake(future);
                        });
                } catch (...) {
                    Promise_.TrySetException(std::current_exception());
                }
            }

            void Wake(const NThreading::TFuture<void>& sleep) {
                try {
                    sleep.GetValue();
                    Function_().Subscribe(
                        [self = shared_from_this()](const NThreading::TFuture<bool>& future) {
                            self->CompleteIteration(future);
                        });
                } catch (...) {
                    Promise_.TrySetException(std::current_exception());
                }
            }

            void CompleteIteration(const NThreading::TFuture<bool>& result) {
                try {
                    if (result.GetValue()) {
                        Sleep();
                    } else {
                        Promise_.TrySetValue();
                    }
                } catch (...) {
                    Promise_.TrySetException(std::current_exception());
                }
            }

            TSdkRuntime& Runtime_;
            std::function<NThreading::TFuture<bool>()> Function_;
            const TDuration Interval_;
            NThreading::TPromise<void> Promise_;
        };

        thread_local unsigned int SdkRuntimeCallbackDepth = 0;
        thread_local bool InitializingRuntime = false;
        constinit std::atomic<TSdkRuntimeImpl*> RuntimeImpl = nullptr;
        alignas(TSdkRuntimeImpl) char RuntimeInitializingMarker;

    } // anonymous namespace

    void TSdkRuntime::SetExecutor(IExecutor::TPtr executor) {
        GetOrInitialize(std::move(executor));
    }

    NThreading::TFuture<void> TSdkRuntime::AsyncSleep(TDuration delay) {
        auto promise = NThreading::NewPromise<void>();
        auto future = promise.GetFuture();

        try {
            GetOrInitialize();
            auto* impl = RuntimeImpl.load(std::memory_order_acquire);
            impl->Scheduler.SafeAddFunc(
                [this, promise]() mutable {
                    try {
                        PostImpl([promise]() mutable {
                            promise.TrySetValue();
                        });
                    } catch (...) {
                        promise.TrySetException(std::current_exception());
                    }
                    return TInstant::Max();
                },
                TInstant::Now() + delay);
        } catch (...) {
            promise.TrySetException(std::current_exception());
        }

        return future;
    }

    NThreading::TFuture<void> TSdkRuntime::AddPeriodicTaskImpl(
        std::function<NThreading::TFuture<bool>()>&& function,
        TDuration interval) {
        return std::make_shared<TPeriodicTask>(*this, std::move(function), interval)->Start();
    }

    IExecutor& TSdkRuntime::GetOrInitialize(
        IExecutor::TPtr executor,
        std::size_t threadCount,
        std::size_t maxQueueSize) {
        auto* const initializing = reinterpret_cast<TSdkRuntimeImpl*>(&RuntimeInitializingMarker);

        while (true) {
            auto* impl = RuntimeImpl.load(std::memory_order_acquire);
            if (impl == initializing) {
                if (InitializingRuntime) {
                    throw TContractViolation(
                        "The YDB SDK runtime cannot be used while its executor is starting");
                }
                RuntimeImpl.wait(impl, std::memory_order_acquire);
                continue;
            }
            if (impl) {
                if (executor && impl->Executor != executor) {
                    throw TContractViolation(
                        "The process-wide YDB SDK executor has already been configured with another instance");
                }
                return *impl->Executor;
            }

            if (!RuntimeImpl.compare_exchange_weak(
                    impl,
                    initializing,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                continue;
            }

            try {
                auto holder = std::make_unique<TSdkRuntimeImpl>(
                    executor ? std::move(executor) : CreateThreadPoolExecutor(threadCount, maxQueueSize));
                InitializingRuntime = true;
                Y_SCOPE_EXIT() {
                    InitializingRuntime = false;
                };
                holder->Scheduler.Start();
                holder->Executor->Start();
                impl = holder.release();
                RuntimeImpl.store(impl, std::memory_order_release);
                RuntimeImpl.notify_all();
                return *impl->Executor;
            } catch (...) {
                RuntimeImpl.store(nullptr, std::memory_order_release);
                RuntimeImpl.notify_all();
                throw;
            }
        }
    }

    void TSdkRuntime::PostImpl(IExecutor::TFunction&& function) {
        GetOrInitialize().Post([function = std::move(function)]() mutable {
            ++SdkRuntimeCallbackDepth;
            Y_SCOPE_EXIT(&function) {
                function = nullptr;
                --SdkRuntimeCallbackDepth;
            };
            function();
        });
    }

    bool IsSdkRuntimeCallback() noexcept {
        return SdkRuntimeCallbackDepth != 0;
    }

    TSdkRuntime& GetSdkRuntime() noexcept {
        constinit static std::atomic<TSdkRuntime*> instance = nullptr;
        auto* runtime = instance.load(std::memory_order_acquire);
        if (!runtime) {
            auto* candidate = new TSdkRuntime;
            if (instance.compare_exchange_strong(
                    runtime, candidate, std::memory_order_acq_rel, std::memory_order_acquire)) {
                runtime = candidate;
            } else {
                delete candidate;
            }
        }
        return *runtime;
    }

} // namespace NYdb::inline Dev
