#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/runtime/runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

using namespace NYdb;

namespace {

    class TTestExecutor final: public IExecutor {
    public:
        enum class EMode { Manual,
                           Concurrent };

        void Stop() override {
        }

        void Post(TFunction&& callback) override {
            std::lock_guard lock(Mutex_);
            if (Mode.load() == EMode::Manual) {
                Queue_.push_back(std::move(callback));
            } else {
                Threads_.emplace_back(std::move(callback));
            }
        }

        bool IsAsync() const override {
            return true;
        }

        void RunQueued() {
            std::deque<TFunction> queue;
            {
                std::lock_guard lock(Mutex_);
                queue.swap(Queue_);
            }
            for (auto& callback : queue) {
                callback();
            }
        }

        void Join() {
            std::vector<std::thread> threads;
            {
                std::lock_guard lock(Mutex_);
                threads.swap(Threads_);
            }
            for (auto& thread : threads) {
                thread.join();
            }
        }

        void SetMode(EMode mode) {
            Mode.store(mode);
        }

        std::atomic_size_t StartCount = 0;

    private:
        void DoStart() override {
            ++StartCount;
        }

        std::mutex Mutex_;
        std::deque<TFunction> Queue_;
        std::vector<std::thread> Threads_;
        std::atomic<EMode> Mode = EMode::Manual;
    };

    const auto TestExecutor = std::make_shared<TTestExecutor>();

} // anonymous namespace

Y_UNIT_TEST_SUITE(SdkRuntimeTest) {
    Y_UNIT_TEST(ParallelInitializationStartsOneExecutor) {
        constexpr size_t ThreadCount = 8;
        auto executor = TestExecutor;
        std::array<TSdkRuntime*, ThreadCount> runtimes{};
        std::array<std::thread, ThreadCount> threads;
        for (size_t i = 0; i < ThreadCount; ++i) {
            threads[i] = std::thread([&, i] {
                runtimes[i] = &GetSdkRuntime();
                runtimes[i]->SetExecutor(executor);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        for (auto* runtime : runtimes) {
            UNIT_ASSERT_VALUES_EQUAL(runtime, &GetSdkRuntime());
        }
        UNIT_ASSERT_VALUES_EQUAL(executor->StartCount.load(), 1);

        auto otherExecutor = std::make_shared<TTestExecutor>();
        UNIT_ASSERT_EXCEPTION(GetSdkRuntime().SetExecutor(otherExecutor), TContractViolation);
        UNIT_ASSERT_VALUES_EQUAL(otherExecutor->StartCount.load(), 0);
    }

    Y_UNIT_TEST(PublicPostReturnsFlattenedFuture) {
        auto executor = TestExecutor;
        GetSdkRuntime().SetExecutor(executor);
        executor->SetMode(TTestExecutor::EMode::Manual);

        auto value = std::make_unique<int>(42);
        auto valueFuture = GetSdkRuntime().Post([value = std::move(value)] {
            return *value;
        });
        bool called = false;
        auto voidFuture = GetSdkRuntime().Post([&called] { called = true; });
        auto nestedPromise = NThreading::NewPromise<int>();
        auto deferredNestedFuture = GetSdkRuntime().Post([future = nestedPromise.GetFuture()] {
            return future;
        });
        auto errorFuture = GetSdkRuntime().Post([]() -> int {
            throw yexception() << "runtime error";
        });

        UNIT_ASSERT(!valueFuture.Wait(TDuration::Zero()));
        executor->RunQueued();
        UNIT_ASSERT_VALUES_EQUAL(valueFuture.GetValue(), 42);
        UNIT_ASSERT(voidFuture.HasValue());
        UNIT_ASSERT(called);
        UNIT_ASSERT(!deferredNestedFuture.IsReady());
        nestedPromise.SetValue(23);
        UNIT_ASSERT_VALUES_EQUAL(deferredNestedFuture.GetValue(), 23);
        UNIT_ASSERT_EXCEPTION(errorFuture.GetValue(), yexception);
    }
} // Y_UNIT_TEST_SUITE(SdkRuntimeTest)
