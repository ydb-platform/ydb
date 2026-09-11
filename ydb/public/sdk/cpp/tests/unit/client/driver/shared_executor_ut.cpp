#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/extension_common/extension.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections/grpc_connections.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/sdk_runtime/runtime.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/testing/unittest/registar.h>

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
                          .SetNetworkThreadsNum(1)
                          .SetDiscoveryMode(EDiscoveryMode::Off)
                          .SetSocketIdleTimeout(TDuration::Max());
        if (executor) {
            config.SetExecutor(std::move(executor));
        }
        return config;
    }

} // namespace

// Each test configures its own process-wide executor.
Y_UNIT_TEST_SUITE(SharedResponseExecutorTest) {
    SIMPLE_UNIT_FORKED_TEST(InitializesOnceAndRejectsDifferentExecutor) {
        auto executor = std::make_shared<TManualExecutor>();
        constexpr std::size_t ThreadCount = 8;
        std::array<std::thread, ThreadCount> threads;
        for (std::size_t i = 0; i < ThreadCount; ++i) {
            threads[i] = std::thread([&, i] {
                UNIT_ASSERT(GetSdkRuntime().GetExecutor(executor, i + 1, i + 1) == executor);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(executor->Starts.load(), 1);
        UNIT_ASSERT(GetSdkRuntime().GetExecutor() == executor);
        TDriver implicit(DriverConfig());
        TDriver same(DriverConfig(executor));
        auto other = std::make_shared<TManualExecutor>();
        UNIT_ASSERT_EXCEPTION(TDriver(DriverConfig(other)), TContractViolation);
        UNIT_ASSERT_VALUES_EQUAL(other->Starts.load(), 0);
    }

    SIMPLE_UNIT_FORKED_TEST(RetriesFailedInitialization) {
        auto executor = std::make_shared<TManualExecutor>();
        executor->OnStart = [] { ythrow yexception() << "Startup failed"; };
        UNIT_ASSERT_EXCEPTION(GetSdkRuntime().GetExecutor(executor), yexception);
        executor->OnStart = {};
        UNIT_ASSERT(GetSdkRuntime().GetExecutor(executor) == executor);
        UNIT_ASSERT_VALUES_EQUAL(executor->Starts.load(), 2);
    }

    SIMPLE_UNIT_FORKED_TEST(StopDoesNotCancelContextsOrDrainTasks) {
        auto executor = std::make_shared<TManualExecutor>();
        TDriver driverA(DriverConfig(executor));
        TDriver driverB(DriverConfig());
        auto connectionsA = CreateInternalInterface(driverA);
        auto connectionsB = CreateInternalInterface(driverB);
        auto contextA = connectionsA->CreateContext();
        auto contextB = connectionsB->CreateContext();
        bool called = false;
        connectionsA->PostToResponseQueue([&] { called = true; });
        driverA.Stop(false);
        driverA.Stop(true);
        UNIT_ASSERT(!called);
        UNIT_ASSERT(!contextA->IsCancelled());
        UNIT_ASSERT(!contextB->IsCancelled());
        UNIT_ASSERT(connectionsA->CreateContext());
        executor->RunAll();
        UNIT_ASSERT(called);
    }

    SIMPLE_UNIT_FORKED_TEST(PostsDelegateWithoutWrappingTasks) {
        auto executor = std::make_shared<TManualExecutor>();
        TDriver driver(DriverConfig(executor));
        auto connections = CreateInternalInterface(driver);
        bool called = false;
        auto callback = [&] { called = true; };
        connections->PostToResponseQueue(callback);
        auto posted = executor->Take();
        UNIT_ASSERT(posted.target<decltype(callback)>());
        posted();
        UNIT_ASSERT(called);
        executor->Reject.store(true);
        driver.Stop(true);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            connections->PostToResponseQueue([] {}), yexception, "Rejected test submission");
    }

    SIMPLE_UNIT_FORKED_TEST(CallbacksAndContextsOutliveDriverHandles) {
        auto executor = std::make_shared<TManualExecutor>();
        std::weak_ptr<TGRpcConnectionsImpl> weak;
        NYdbGrpc::IQueueClientContextPtr context;
        bool called = false;
        {
            TDriver driver(DriverConfig(executor).SetDatabase("/Root"));
            auto connections = CreateInternalInterface(driver);
            weak = connections;
            context = connections->CreateContext();
            connections->PostToResponseQueue([&] { called = true; });
        }
        UNIT_ASSERT(weak.expired());
        UNIT_ASSERT(!context->IsCancelled());
        executor->RunAll();
        UNIT_ASSERT(called);
    }

    SIMPLE_UNIT_FORKED_TEST(DatabaseStateOwnsItsConfiguration) {
        auto executor = std::make_shared<TManualExecutor>();
        std::weak_ptr<TGRpcConnectionsImpl> weak;
        TDbDriverStatePtr state;
        {
            TDriver driver(DriverConfig(executor).SetDatabase("/Root"));
            auto connections = CreateInternalInterface(driver);
            weak = connections;
            state = connections->GetDriverState({}, {}, {}, {}, {});
        }
        UNIT_ASSERT(!weak.expired());
        bool called = false;
        state->PostToResponseQueue([&] { called = true; });
        executor->RunAll();
        UNIT_ASSERT(called);
        state.reset();
        UNIT_ASSERT(weak.expired());
    }

    SIMPLE_UNIT_FORKED_TEST(LastDriverCanBeReleasedInsideCallback) {
        auto executor = std::make_shared<TManualExecutor>();
        auto driver = std::make_shared<TDriver>(DriverConfig(executor).SetDatabase("/Root"));
        auto destroyed = std::make_shared<std::promise<void>>();
        auto destroyedFuture = destroyed->get_future();
        driver->AddExtension<TDestructionProbe>(destroyed);
        std::weak_ptr<TGRpcConnectionsImpl> weak = CreateInternalInterface(*driver);
        CreateInternalInterface(*driver)->PostToResponseQueue([driver = std::move(driver)]() mutable {
            driver->Stop(true);
            driver.reset();
        });
        executor->RunAll();
        UNIT_ASSERT(destroyedFuture.wait_for(0s) == std::future_status::ready);
        UNIT_ASSERT(weak.expired());
    }

    SIMPLE_UNIT_FORKED_TEST(ExplicitCancellationSurvivesDriverDestruction) {
        auto executor = std::make_shared<TManualExecutor>();
        NYdbGrpc::IQueueClientContextPtr context;
        NThreading::TFuture<bool> scheduled;
        std::weak_ptr<TGRpcConnectionsImpl> weak;
        {
            TDriver driver(DriverConfig(executor));
            auto connections = CreateInternalInterface(driver);
            weak = connections;
            context = connections->CreateContext();
            scheduled = connections->ScheduleFuture(TDuration::Max(), context);
            driver.Stop(true);
            UNIT_ASSERT(!scheduled.IsReady());
        }
        UNIT_ASSERT(weak.expired());
        context->Cancel();
        UNIT_ASSERT(scheduled.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(!scheduled.GetValue());
        auto ready = GetSdkRuntime().ScheduleFuture(TDuration::Zero());
        UNIT_ASSERT(ready.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(ready.GetValue());
    }
}
