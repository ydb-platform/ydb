#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/extension_common/extension.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections/grpc_connections.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/sdk_runtime/runtime.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <future>
#include <mutex>
#include <thread>

using namespace NYdb;
using namespace std::chrono_literals;

namespace {

    class TManualExecutor final: public IExecutor {
    public:
        void Stop() override {
            UNIT_FAIL("A driver must not stop the shared executor");
        }

        void Post(TFunction&& callback) override {
            if (OnPost) {
                OnPost();
            }
            if (Reject.load()) {
                ythrow yexception() << "Rejected test submission";
            }
            if (RunInline) {
                callback();
                return;
            }
            {
                std::lock_guard lock(Mutex_);
                Queue_.push_back(std::move(callback));
            }
        }

        bool IsAsync() const override {
            return !RunInline;
        }

        TFunction Take() {
            std::lock_guard lock(Mutex_);
            if (Queue_.empty()) {
                return {};
            }
            auto callback = std::move(Queue_.front());
            Queue_.pop_front();
            return callback;
        }

        void RunAll() {
            while (auto callback = Take()) {
                callback();
            }
        }

        std::atomic_uint Starts = 0;
        std::atomic_bool Reject = false;
        bool RunInline = false;
        std::function<void()> OnStart;
        std::function<void()> OnPost;

    private:
        void DoStart() override {
            ++Starts;
            if (OnStart) {
                OnStart();
            }
        }

        std::mutex Mutex_;
        std::deque<TFunction> Queue_;
    };

    class TDestructionProbe final: public IExtension {
    public:
        struct IApi: IExtensionApi {
            static IApi* Create(TDriver) {
                return nullptr;
            }
        };
        using TParams = std::shared_ptr<std::promise<void>>;

        TDestructionProbe(TParams destroyed, IApi*)
            : Destroyed_(std::move(destroyed))
        {
        }

        ~TDestructionProbe() override {
            Destroyed_->set_value();
        }

    private:
        std::shared_ptr<std::promise<void>> Destroyed_;
    };

    TDriverConfig DriverConfig(IExecutor::TPtr executor = {}) {
        auto config = TDriverConfig()
                          .SetEndpoint("localhost:1")
                          .SetDiscoveryMode(EDiscoveryMode::Off)
                          .SetSocketIdleTimeout(TDuration::Max());
        if (executor) {
            config.SetExecutor(std::move(executor));
        }
        return config;
    }

} // namespace

// Fork explicitly: each test must initialize the process-wide executor independently.
Y_UNIT_TEST_SUITE(SharedResponseExecutorTest) {
    SIMPLE_UNIT_FORKED_TEST(InitializesOnceAndRejectsDifferentExecutor) {
        auto executor = std::make_shared<TManualExecutor>();
        constexpr std::size_t ThreadCount = 8;
        std::array<std::thread, ThreadCount> threads;
        for (std::size_t i = 0; i < ThreadCount; ++i) {
            threads[i] = std::thread([&, i] {
                GetSdkRuntime().CreateResponseQueue(executor, i + 1, i + 1);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(executor->Starts.load(), 1);
        auto implicit = GetSdkRuntime().CreateResponseQueue();
        std::size_t calls = 0;
        implicit->Post([&] { ++calls; });
        UNIT_ASSERT_VALUES_EQUAL(calls, 0);
        executor->RunAll();
        implicit->Stop();
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        auto driver = TDriver(DriverConfig());
        auto same = TDriver(DriverConfig(executor));
        auto other = std::make_shared<TManualExecutor>();
        UNIT_ASSERT_EXCEPTION(TDriver(DriverConfig(other)), TContractViolation);
        UNIT_ASSERT_VALUES_EQUAL(other->Starts.load(), 0);
    }

    SIMPLE_UNIT_FORKED_TEST(RetriesFailedInitialization) {
        auto executor = std::make_shared<TManualExecutor>();
        executor->OnStart = [] { ythrow yexception() << "Startup failed"; };
        UNIT_ASSERT_EXCEPTION(GetSdkRuntime().CreateResponseQueue(executor), yexception);
        executor->OnStart = {};
        auto queue = GetSdkRuntime().CreateResponseQueue(executor);
        UNIT_ASSERT_VALUES_EQUAL(executor->Starts.load(), 2);
        queue->Stop();
    }

    SIMPLE_UNIT_FORKED_TEST(DrainsOnlyItsOwnQueuedTasksAndCaptures) {
        auto executor = std::make_shared<TManualExecutor>();
        auto queueA = GetSdkRuntime().CreateResponseQueue(executor);
        auto queueB = GetSdkRuntime().CreateResponseQueue();
        auto capture = std::make_shared<int>(42);
        std::weak_ptr<int> weak = capture;
        queueA->Post([capture] { UNIT_ASSERT_VALUES_EQUAL(*capture, 42); });
        capture.reset();
        auto taskA = executor->Take();
        auto retainedCopy = taskA;
        bool calledB = false;
        queueB->Post([&] { calledB = true; });
        auto taskB = executor->Take();
        std::array<std::future<bool>, 2> stopped;
        for (auto& stop : stopped) {
            stop = std::async(std::launch::async, [queueA, weak] {
                queueA->Stop();
                return weak.expired();
            });
        }
        Y_SCOPE_EXIT(&taskA, &retainedCopy, &executor, &stopped) {
            taskA = {};
            retainedCopy = {};
            executor->RunAll();
            for (auto& stop : stopped) {
                if (stop.valid()) {
                    stop.wait();
                }
            }
        };
        for (auto& stop : stopped) {
            UNIT_ASSERT(stop.wait_for(0s) != std::future_status::ready);
        }
        taskA();
        UNIT_ASSERT(weak.expired());
        executor->RunAll();
        for (auto& stop : stopped) {
            UNIT_ASSERT(stop.wait_for(10s) == std::future_status::ready);
            UNIT_ASSERT(stop.get());
        }
        // An idle executor may keep its last completed callback indefinitely.
        taskA = {};
        retainedCopy = {};
        UNIT_ASSERT(!calledB);
        taskB();
        taskB = {};
        queueB->Stop();
        queueA->Stop();
        UNIT_ASSERT(calledB);
        bool calledAfterStop = false;
        queueA->Post([&] { calledAfterStop = true; });
        UNIT_ASSERT(!calledAfterStop);
        executor->RunAll();
        queueA->Stop();
        UNIT_ASSERT(calledAfterStop);
    }

    SIMPLE_UNIT_FORKED_TEST(ExceptionsReleaseAccountingAndCaptures) {
        auto executor = std::make_shared<TManualExecutor>();
        for (bool runInline : {false, true}) {
            executor->RunInline = runInline;
            executor->Reject = !runInline;
            auto queue = GetSdkRuntime().CreateResponseQueue(executor);
            queue->Stop();
            UNIT_ASSERT_VALUES_EQUAL(queue->IsAsync(), !runInline);
            auto capture = std::make_shared<int>(42);
            std::weak_ptr<int> weak = capture;
            unsigned calls = 0;
            UNIT_ASSERT_EXCEPTION_CONTAINS(queue->Post([capture = std::move(capture), &calls] {
                ++calls;
                ythrow yexception() << "Callback failed";
            }), yexception, runInline ? "Callback failed" : "Rejected test submission");
            UNIT_ASSERT_VALUES_EQUAL(calls, runInline ? 1 : 0);
            UNIT_ASSERT(weak.expired());
            queue->Stop();
        }
    }

    SIMPLE_UNIT_FORKED_TEST(StopWaitsForBlockedSubmission) {
        auto executor = std::make_shared<TManualExecutor>();
        auto queue = GetSdkRuntime().CreateResponseQueue(executor);
        std::promise<void> entered;
        std::promise<void> release;
        auto released = release.get_future().share();
        std::atomic_bool blockNext = true;
        executor->OnPost = [&] {
            if (blockNext.exchange(false)) {
                entered.set_value();
                released.wait();
            }
        };
        std::atomic_bool called = false;
        auto posted = std::async(std::launch::async, [queue, &called] {
            queue->Post([&called] { called.store(true); });
        });
        std::future<bool> stopped;
        {
            Y_SCOPE_EXIT(&release, &posted, &executor) {
                release.set_value();
                posted.wait();
                executor->OnPost = {};
                executor->RunAll();
            };
            UNIT_ASSERT(entered.get_future().wait_for(10s) == std::future_status::ready);
            stopped = std::async(std::launch::async, [queue, &called] {
                queue->Stop();
                return called.load();
            });
            UNIT_ASSERT(stopped.wait_for(0s) != std::future_status::ready);
        }
        UNIT_ASSERT(stopped.wait_for(10s) == std::future_status::ready);
        UNIT_ASSERT(stopped.get());
    }

    SIMPLE_UNIT_FORKED_TEST(SubmissionRacingWithStopIsDelegated) {
        auto executor = std::make_shared<TManualExecutor>();
        for (std::size_t iteration = 0; iteration < 32; ++iteration) {
            auto queue = GetSdkRuntime().CreateResponseQueue(executor);
            std::promise<void> start;
            auto ready = start.get_future().share();
            constexpr std::size_t PosterCount = 8;
            std::array<bool, PosterCount> called = {};
            std::array<std::thread, PosterCount> posters;
            for (std::size_t i = 0; i < PosterCount; ++i) {
                posters[i] = std::thread([&, i] {
                    ready.wait();
                    queue->Post([&, i] { called[i] = true; });
                });
            }
            auto stopped = std::async(std::launch::async, [queue, ready] {
                ready.wait();
                queue->Stop();
            });
            start.set_value();
            for (auto& poster : posters) {
                poster.join();
            }
            executor->RunAll();
            UNIT_ASSERT(stopped.wait_for(10s) == std::future_status::ready);
            queue->Stop();
            for (std::size_t i = 0; i < PosterCount; ++i) {
                UNIT_ASSERT(called[i]);
            }
        }
    }

    SIMPLE_UNIT_FORKED_TEST(StopCancelsOneDriverWithoutStoppingAnother) {
        auto executor = std::make_shared<TManualExecutor>();
        auto driverA = TDriver(DriverConfig(executor));
        auto driverB = TDriver(DriverConfig());
        auto connectionsA = CreateInternalInterface(driverA);
        auto connectionsB = CreateInternalInterface(driverB);
        auto contextA = connectionsA->CreateContext();
        auto contextB = connectionsB->CreateContext();
        driverA.Stop(false);
        UNIT_ASSERT(contextA->IsCancelled());
        UNIT_ASSERT(!contextB->IsCancelled());
        UNIT_ASSERT(!connectionsA->CreateContext());
        contextA.reset();
        bool calledB = false;
        connectionsB->PostToResponseQueue([&] { calledB = true; });
        auto taskB = executor->Take();
        // The executor can accept more work after a completed Stop(true).
        for (unsigned i = 0; i < 2; ++i) {
            bool calledA = false;
            auto capture = std::make_shared<int>(42);
            std::weak_ptr<int> weak = capture;
            connectionsA->PostToResponseQueue([&calledA, capture] { calledA = true; });
            capture.reset();
            auto taskA = executor->Take();
            std::promise<void> stopping;
            auto stopped = std::async(std::launch::async, [&] {
                stopping.set_value();
                driverA.Stop(true);
                return weak.expired();
            });
            stopping.get_future().wait();
            taskA();
            taskA = {};
            UNIT_ASSERT(stopped.wait_for(10s) == std::future_status::ready);
            UNIT_ASSERT(calledA);
            UNIT_ASSERT(stopped.get());
        }
        UNIT_ASSERT(!calledB);
        taskB();
        taskB = {};
        UNIT_ASSERT(calledB);
        driverA.Stop(true);
        contextB.reset();
        driverB.Stop(true);
        UNIT_ASSERT_VALUES_EQUAL(executor->Starts.load(), 1);
    }

    SIMPLE_UNIT_FORKED_TEST(CallbackCanRemoveItsExecutorWrapper) {
        auto executor = std::make_shared<TManualExecutor>();
        auto queue = GetSdkRuntime().CreateResponseQueue(executor);
        IExecutor::TFunction running;
        bool called = false;
        queue->Post([&] {
            running = {};
            called = true;
        });
        running = executor->Take();
        running();
        queue->Stop();
        UNIT_ASSERT(called);
    }

    SIMPLE_UNIT_FORKED_TEST(UnexecutedCaptureCanStopItsDriver) {
        auto executor = std::make_shared<TManualExecutor>();
        for (bool reject : {false, true}) {
            auto driver = std::make_shared<TDriver>(DriverConfig(executor));
            auto destroyed = std::make_shared<std::promise<void>>();
            auto destruction = destroyed->get_future();
            driver->AddExtension<TDestructionProbe>(destroyed);
            auto connections = CreateInternalInterface(*driver);
            bool destroyedInCallback = false;
            auto capture = std::shared_ptr<void>(nullptr, [driver = std::move(driver), &destroyedInCallback](void*) {
                destroyedInCallback = TDriverScope::IsCurrentThreadInCallback();
                driver->Stop(true);
            });
            std::weak_ptr<void> weak = capture;
            // Avoid libc++ retaining a source copy in std::function inline storage.
            auto callback = [capture = std::move(capture), padding = std::array<char, 64>{}] {
                Y_UNUSED(capture);
                Y_UNUSED(padding);
            };
            executor->Reject = reject;
            if (reject) {
                UNIT_ASSERT_EXCEPTION(connections->PostToResponseQueue(std::move(callback)), yexception);
            } else {
                connections->PostToResponseQueue(std::move(callback));
                auto discarded = executor->Take();
                UNIT_ASSERT(discarded);
                auto retainedCopy = discarded;
                discarded = {};
                UNIT_ASSERT(!weak.expired());
                retainedCopy = {};
            }
            UNIT_ASSERT(weak.expired());
            UNIT_ASSERT(destroyedInCallback);
            connections.reset();
            UNIT_ASSERT(destruction.wait_for(10s) == std::future_status::ready);
        }
    }

    SIMPLE_UNIT_FORKED_TEST(DefaultExecutorSurvivesStopAndLastOwnerReleaseInsideCallback) {
        auto driver = std::make_shared<TDriver>(DriverConfig().SetClientThreadsNum(1));
        UNIT_ASSERT_EXCEPTION(
            GetSdkRuntime().CreateResponseQueue(std::make_shared<TManualExecutor>()), TContractViolation);
        auto reused = GetSdkRuntime().CreateResponseQueue({}, 8, 100);
        reused->Stop();
        auto destroyed = std::make_shared<std::promise<void>>();
        auto destruction = destroyed->get_future();
        driver->AddExtension<TDestructionProbe>(destroyed);
        auto connections = CreateInternalInterface(*driver);
        auto completed = std::make_shared<std::promise<void>>();
        auto completion = completed->get_future();
        connections->PostToResponseQueue([driver = std::move(driver), completed] {
            driver->Stop(true);
            completed->set_value();
        });
        connections.reset();
        UNIT_ASSERT(completion.wait_for(10s) == std::future_status::ready);
        UNIT_ASSERT(destruction.wait_for(10s) == std::future_status::ready);
    }
} // Y_UNIT_TEST_SUITE(SharedResponseExecutorTest)
