#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/runtime/runtime.h>
#include <ydb/public/sdk/cpp/src/library/runtime/runtime_impl.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <vector>

using namespace NYdb;

Y_UNIT_TEST_SUITE(RuntimeTests) {
    Y_UNIT_TEST(ScheduledTaskRunsAfterDeadlineOnAnotherThread) {
        auto done = std::make_shared<std::promise<std::pair<bool, bool>>>();
        auto completed = done->get_future();
        const auto submittingThread = std::this_thread::get_id();
        const auto deadline = TDeadline::AfterDuration(std::chrono::milliseconds(20));
        GetRuntime().Schedule(deadline, [done, deadline, submittingThread] {
            done->set_value({
                deadline <= TDeadline::Now(),
                std::this_thread::get_id() != submittingThread});
        });
        UNIT_ASSERT(completed.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        const auto [afterDeadline, otherThread] = completed.get();
        UNIT_ASSERT(afterDeadline);
        UNIT_ASSERT(otherThread);
    }

    Y_UNIT_TEST(ScheduledFutureCompletesAfterDeadline) {
        const auto deadline = TDeadline::AfterDuration(std::chrono::milliseconds(20));
        auto completed = GetRuntime().ScheduleFuture(deadline);
        UNIT_ASSERT(completed.Wait(TDuration::Seconds(5)));
        completed.GetValue();
        UNIT_ASSERT(deadline <= TDeadline::Now());
    }

    Y_UNIT_TEST(FutureDurationOverloadsComplete) {
        for (auto completed : {
                GetRuntime().ScheduleFuture(TDuration::Zero()),
                GetRuntime().ScheduleFuture(std::chrono::milliseconds::zero()),
                GetRuntime().ScheduleFuture(std::chrono::milliseconds(-1))}) {
            UNIT_ASSERT(completed.Wait(TDuration::Seconds(5)));
            completed.GetValue();
        }
        const auto earliest = TDeadline::AfterDuration(std::chrono::milliseconds(20));
        auto delayed = GetRuntime().ScheduleFuture(TDuration::MilliSeconds(20));
        UNIT_ASSERT(delayed.Wait(TDuration::Seconds(5)));
        delayed.GetValue();
        UNIT_ASSERT(earliest <= TDeadline::Now());
    }

    Y_UNIT_TEST(ScheduledFutureSupportsValueAndErrorContinuations) {
        const auto completed = GetRuntime().ScheduleFuture(std::chrono::milliseconds(20));
        auto value = completed.Apply([](const NThreading::TFuture<void>& ready) {
            ready.GetValue();
            return 42;
        });
        auto failure = completed.Apply([](const NThreading::TFuture<void>& ready) {
            ready.GetValue();
            throw std::runtime_error("Scheduled continuation failed");
        });
        UNIT_ASSERT(value.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(value.GetValue(), 42);
        UNIT_ASSERT(failure.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_EXCEPTION_CONTAINS(failure.GetValue(), std::runtime_error, "Scheduled continuation failed");

        auto subsequent = GetRuntime().ScheduleFuture(TDuration::Zero());
        UNIT_ASSERT(subsequent.Wait(TDuration::Seconds(5)));
        subsequent.GetValue();
    }

    Y_UNIT_TEST(CallableReceivesArgumentsAfterDeadline) {
        const auto deadline = TDeadline::AfterDuration(std::chrono::milliseconds(20));
        const auto submittingThread = std::this_thread::get_id();
        auto completed = GetRuntime().ScheduleFuture(deadline, [deadline, submittingThread](int left, int right) {
            return std::tuple{
                left + right,
                deadline <= TDeadline::Now(),
                std::this_thread::get_id() != submittingThread};
        }, 40, 2);
        UNIT_ASSERT(completed.Wait(TDuration::Seconds(5)));
        const auto [value, afterDeadline, otherThread] = completed.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(value, 42);
        UNIT_ASSERT(afterDeadline);
        UNIT_ASSERT(otherThread);
    }

    Y_UNIT_TEST(ImmediateCallablesNeverRunInline) {
        const auto submittingThread = std::this_thread::get_id();
        for (const auto delay : {std::chrono::milliseconds::zero(), std::chrono::milliseconds(-1)}) {
            auto completed = GetRuntime().ScheduleFuture(delay, [submittingThread] {
                return std::this_thread::get_id() != submittingThread;
            });
            UNIT_ASSERT(completed.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT(completed.GetValue());
        }
    }

    Y_UNIT_TEST(MoveOnlyCallableArgumentsAndResult) {
        auto total = std::make_shared<int>(1);
        auto completed = GetRuntime().ScheduleFuture(TDuration::Zero(),
            [offset = std::make_unique<int>(20), lifetime = total](std::unique_ptr<int> value, int& sum) {
                sum += *offset + *value;
                return std::make_unique<int>(*lifetime);
            }, std::make_unique<int>(21), std::ref(*total));
        UNIT_ASSERT(completed.Wait(TDuration::Seconds(5)));
        auto value = completed.ExtractValue();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(*value, 42);
        UNIT_ASSERT_VALUES_EQUAL(*total, 42);
    }

    Y_UNIT_TEST(VoidCallablesAndReturnedVoidFuturesComplete) {
        NThreading::TFuture<void> direct = GetRuntime().ScheduleFuture(TDuration::Zero(), [] {});
        NThreading::TFuture<void> nested = GetRuntime().ScheduleFuture(std::chrono::milliseconds::zero(), [] {
            return NThreading::MakeFuture();
        });
        UNIT_ASSERT(direct.Wait(TDuration::Seconds(5)));
        direct.GetValue();
        UNIT_ASSERT(nested.Wait(TDuration::Seconds(5)));
        nested.GetValue();
    }

    Y_UNIT_TEST(CallableAndReturnedFutureExceptionsReachResult) {
        auto direct = GetRuntime().ScheduleFuture(TDuration::Zero(), []() -> int {
            throw std::runtime_error("Scheduled callable failed");
        });
        auto nested = GetRuntime().ScheduleFuture(TDuration::Zero(), [] {
            return NThreading::MakeErrorFuture<int>(std::make_exception_ptr(
                std::runtime_error("Returned future failed")));
        });
        UNIT_ASSERT(direct.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_EXCEPTION_CONTAINS(direct.GetValue(), std::runtime_error, "Scheduled callable failed");
        UNIT_ASSERT(nested.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_EXCEPTION_CONTAINS(nested.GetValue(), std::runtime_error, "Returned future failed");
        auto subsequent = GetRuntime().ScheduleFuture(TDuration::Zero(), [] { return 42; });
        UNIT_ASSERT(subsequent.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(subsequent.GetValue(), 42);
    }

    Y_UNIT_TEST(PendingReturnedFutureDoesNotBlockRuntime) {
        auto inner = NThreading::NewPromise<int>();
        Y_SCOPE_EXIT(inner) {
            inner.TrySetValue(42);
        };
        auto entered = NThreading::NewPromise();
        auto returned = NThreading::NewPromise();
        auto marker = std::shared_ptr<void>(nullptr, [returned](void*) mutable {
            returned.SetValue();
        });
        NThreading::TFuture<int> completed = GetRuntime().ScheduleFuture(TDuration::Zero(),
            [future = inner.GetFuture(), entered, marker = std::move(marker)]() mutable {
                Y_UNUSED(marker);
                entered.SetValue();
                return NThreading::MakeFuture(future);
            });
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(returned.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!completed.IsReady());
        auto subsequent = GetRuntime().ScheduleFuture(TDuration::Zero(), [] { return 43; });
        UNIT_ASSERT(subsequent.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(subsequent.GetValue(), 43);
        inner.SetValue(42);
        UNIT_ASSERT(completed.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(completed.GetValue(), 42);
    }

    Y_UNIT_TEST(ReturnedMoveOnlyFutureTransfersResult) {
        using TResult = std::vector<std::unique_ptr<int>>;
        auto inner = NThreading::NewPromise<TResult>();
        Y_SCOPE_EXIT(inner) {
            inner.TrySetValue(TResult{});
        };
        NThreading::TFuture<TResult> completed = GetRuntime().ScheduleFuture(TDuration::Zero(),
            [future = inner.GetFuture()] { return future; });
        TResult values;
        values.push_back(std::make_unique<int>(42));
        inner.SetValue(std::move(values));
        UNIT_ASSERT(completed.Wait(TDuration::Seconds(5)));
        auto value = completed.ExtractValue();
        UNIT_ASSERT_VALUES_EQUAL(value.size(), 1);
        UNIT_ASSERT(value.front());
        UNIT_ASSERT_VALUES_EQUAL(*value.front(), 42);
    }

    Y_UNIT_TEST(ScopedRuntimeExecutorsFinishAcceptedTasks) {
        for (const std::size_t threadCount : {1U, 0U}) {
            std::atomic<unsigned> calls = 0;
            auto executor = NRuntime::CreateExecutor(threadCount);
            executor->Start();
            executor->Stop();
            executor->Post([&calls] { ++calls; });
            executor.reset();
            // Destruction of this private, scoped pool finishes its accepted work.
            UNIT_ASSERT_VALUES_EQUAL(calls.load(), 1);
        }
    }

    Y_UNIT_TEST(ThrowingSubscriberDoesNotOverwriteCompletedResult) {
        auto release = NThreading::NewPromise();
        Y_SCOPE_EXIT(release) {
            release.TrySetValue();
        };
        auto retired = NThreading::NewPromise();
        auto marker = std::shared_ptr<void>(nullptr, [retired](void*) mutable {
            retired.SetValue();
        });
        auto completed = GetRuntime().ScheduleFuture(TDuration::Zero(),
            [ready = release.GetFuture(), marker = std::move(marker)] {
                Y_UNUSED(marker);
                ready.GetValueSync();
                return NThreading::MakeFuture(42);
            });
        completed.Subscribe([](const NThreading::TFuture<int>&) {
            throw std::runtime_error("Subscriber failed after completion");
        });
        release.SetValue();
        // Capture retirement proves both propagation catches have unwound.
        UNIT_ASSERT(retired.GetFuture().Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!completed.HasException());
        UNIT_ASSERT_VALUES_EQUAL(completed.GetValue(), 42);
        auto subsequent = GetRuntime().ScheduleFuture(TDuration::Zero(), [] { return 43; });
        UNIT_ASSERT(subsequent.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(subsequent.GetValue(), 43);
    }
}
