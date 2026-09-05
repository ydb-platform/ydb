#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/runtime/runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

using namespace NYdb;

namespace {

    class TTestExecutor final: public IExecutor {
    public:
        void Stop() override {
        }

        void Post(TFunction&& callback) override {
            {
                std::lock_guard lock(Mutex_);
                Queue_.push_back(std::move(callback));
            }
            Queued_.notify_one();
        }

        bool IsAsync() const override {
            return true;
        }

        void RunQueued() {
            while (true) {
                TFunction callback;
                {
                    std::lock_guard lock(Mutex_);
                    if (Queue_.empty()) {
                        return;
                    }
                    callback = std::move(Queue_.front());
                    Queue_.pop_front();
                }
                callback();
            }
        }

        bool WaitForQueued() {
            std::unique_lock lock(Mutex_);
            return Queued_.wait_for(lock, std::chrono::seconds(10), [this] {
                return !Queue_.empty();
            });
        }

        bool HasQueued() {
            std::lock_guard lock(Mutex_);
            return !Queue_.empty();
        }

        std::atomic_size_t StartCount = 0;

    private:
        void DoStart() override {
            ++StartCount;
        }

        std::mutex Mutex_;
        std::condition_variable Queued_;
        std::deque<TFunction> Queue_;
    };

    const auto TestExecutor = std::make_shared<TTestExecutor>();

    struct TThrowingCopy {
        TThrowingCopy() = default;
        TThrowingCopy(const TThrowingCopy&) {
            throw yexception() << "copy failed";
        }
        TThrowingCopy(TThrowingCopy&&) noexcept = default;
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(SdkRuntimeTest) {
    Y_UNIT_TEST(Runtime) {
        constexpr size_t ThreadCount = 8;
        std::array<std::thread, ThreadCount> threads;
        std::barrier start(ThreadCount);
        for (auto& thread : threads) {
            thread = std::thread([&start] {
                start.arrive_and_wait();
                GetSdkRuntime().SetExecutor(TestExecutor);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(TestExecutor->StartCount.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TestExecutor.use_count(), 2);

        auto otherExecutor = std::make_shared<TTestExecutor>();
        UNIT_ASSERT_EXCEPTION(GetSdkRuntime().SetExecutor(otherExecutor), TContractViolation);
        UNIT_ASSERT_VALUES_EQUAL(otherExecutor->StartCount.load(), 0);

        auto valueFuture = GetSdkRuntime().Post([] { return 42; });
        bool called = false;
        auto voidFuture = GetSdkRuntime().Post([&called] { called = true; });
        auto nestedPromise = NThreading::NewPromise<int>();
        auto deferredNestedFuture = GetSdkRuntime().Post([future = nestedPromise.GetFuture()] {
            return future;
        });
        auto nestedVoidPromise = NThreading::NewPromise<void>();
        auto nestedVoidFuture = GetSdkRuntime().Post([future = nestedVoidPromise.GetFuture()] {
            return future;
        });
        auto outerPromise = NThreading::NewPromise<NThreading::TFuture<int>>();
        auto twiceNestedFuture = GetSdkRuntime().Post([future = outerPromise.GetFuture()] {
            return future;
        });
        static_assert(std::is_same_v<decltype(twiceNestedFuture),
            NThreading::TFuture<NThreading::TFuture<int>>>);
        auto throwingPromise = NThreading::NewPromise<TThrowingCopy>();
        auto throwingCopyFuture = GetSdkRuntime().Post([future = throwingPromise.GetFuture()] {
            return future;
        });
        auto errorFuture = GetSdkRuntime().Post([]() -> int {
            throw yexception() << "runtime error";
        });

        auto nestedErrorFuture = GetSdkRuntime().Post([]() -> NThreading::TFuture<int> {
            throw yexception() << "nested runtime error";
        });
        auto uninitializedFuture = GetSdkRuntime().Post([] {
            return NThreading::TFuture<int>{};
        });

        UNIT_ASSERT(!valueFuture.Wait(TDuration::Zero()));
        TestExecutor->RunQueued();
        UNIT_ASSERT_VALUES_EQUAL(valueFuture.GetValue(), 42);
        UNIT_ASSERT(voidFuture.HasValue());
        UNIT_ASSERT(called);
        UNIT_ASSERT(!deferredNestedFuture.IsReady());
        nestedPromise.SetValue(23);
        UNIT_ASSERT_VALUES_EQUAL(deferredNestedFuture.GetValue(), 23);
        nestedVoidPromise.SetValue();
        UNIT_ASSERT(nestedVoidFuture.HasValue());
        outerPromise.SetValue(NThreading::MakeFuture(31));
        UNIT_ASSERT_VALUES_EQUAL(twiceNestedFuture.GetValue().GetValue(), 31);
        throwingPromise.SetValue(TThrowingCopy{});
        UNIT_ASSERT(throwingCopyFuture.HasException());
        UNIT_ASSERT_EXCEPTION(throwingCopyFuture.GetValue(), yexception);
        UNIT_ASSERT(errorFuture.HasException());
        UNIT_ASSERT_EXCEPTION(errorFuture.GetValue(), yexception);
        UNIT_ASSERT(nestedErrorFuture.HasException());
        UNIT_ASSERT_EXCEPTION(nestedErrorFuture.GetValue(), yexception);
        UNIT_ASSERT(uninitializedFuture.HasException());

        auto sleepFuture = GetSdkRuntime().AsyncSleep(TDuration::Zero());
        UNIT_ASSERT(!sleepFuture.IsReady());
        UNIT_ASSERT(TestExecutor->WaitForQueued());
        TestExecutor->RunQueued();
        UNIT_ASSERT(sleepFuture.HasValue());

        auto delayedSleepFuture = GetSdkRuntime().AsyncSleep(TDuration::MilliSeconds(20));
        UNIT_ASSERT(!delayedSleepFuture.Wait(TDuration::MilliSeconds(1)));
        UNIT_ASSERT(TestExecutor->WaitForQueued());
        TestExecutor->RunQueued();
        UNIT_ASSERT(delayedSleepFuture.HasValue());

        std::size_t periodicCalls = 0;
        auto periodicFuture = GetSdkRuntime().AddPeriodicTask([iteration = std::size_t{0}, &periodicCalls]() mutable {
            periodicCalls = ++iteration;
            return iteration < 2;
        },
                                                              TDuration::Zero());
        while (!periodicFuture.IsReady()) {
            UNIT_ASSERT(TestExecutor->WaitForQueued());
            TestExecutor->RunQueued();
        }
        UNIT_ASSERT(periodicFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(periodicCalls, 2);

        auto iteration = NThreading::NewPromise<bool>();
        std::size_t asyncPeriodicCalls = 0;
        auto asyncPeriodicFuture = GetSdkRuntime().AddPeriodicTask([&] {
            if (++asyncPeriodicCalls == 1) {
                return iteration.GetFuture();
            }
            return NThreading::MakeFuture(false);
        },
                                                                   TDuration::Zero());
        UNIT_ASSERT(TestExecutor->WaitForQueued());
        TestExecutor->RunQueued();
        UNIT_ASSERT_VALUES_EQUAL(asyncPeriodicCalls, 1);
        UNIT_ASSERT(!TestExecutor->HasQueued());
        UNIT_ASSERT(!asyncPeriodicFuture.IsReady());
        iteration.SetValue(true);
        UNIT_ASSERT(TestExecutor->WaitForQueued());
        TestExecutor->RunQueued();
        UNIT_ASSERT_VALUES_EQUAL(asyncPeriodicCalls, 2);
        UNIT_ASSERT(asyncPeriodicFuture.HasValue());

        auto failingPeriodicFuture = GetSdkRuntime().AddPeriodicTask([]() -> bool {
            throw yexception() << "periodic error";
        },
                                                                     TDuration::Zero());
        UNIT_ASSERT(TestExecutor->WaitForQueued());
        TestExecutor->RunQueued();
        UNIT_ASSERT(failingPeriodicFuture.HasException());
        UNIT_ASSERT_EXCEPTION(failingPeriodicFuture.GetValue(), yexception);
    }
} // Y_UNIT_TEST_SUITE(SdkRuntimeTest)
