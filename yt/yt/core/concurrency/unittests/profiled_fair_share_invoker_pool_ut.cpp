#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/fair_share_invoker_pool.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <util/datetime/base.h>

#include <algorithm>
#include <array>
#include <ranges>
#include <utility>

namespace NYT::NConcurrency {
namespace {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

constexpr auto Margin = TDuration::MilliSeconds(1);
constexpr auto Quantum = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TMockFairShareCallbackQueue
    : public IFairShareCallbackQueue
{
public:
    explicit TMockFairShareCallbackQueue(int bucketCount)
        : UnderlyingCallbackQueue_(CreateFairShareCallbackQueue(bucketCount))
        , TotalCpuTime_(bucketCount)
    { }

    void Enqueue(TClosure callback, int bucketIndex) override
    {
        UnderlyingCallbackQueue_->Enqueue(std::move(callback), bucketIndex);
    }

    bool TryDequeue(TClosure* resultCallback, int* resultBucketIndex) override
    {
        return UnderlyingCallbackQueue_->TryDequeue(resultCallback, resultBucketIndex);
    }

    void AccountCpuTime(int bucketIndex, NProfiling::TCpuDuration cpuTime) override
    {
        YT_VERIFY(IsValidBucketIndex(bucketIndex));
        TotalCpuTime_[bucketIndex] += cpuTime;
        UnderlyingCallbackQueue_->AccountCpuTime(bucketIndex, cpuTime);
    }

    NProfiling::TCpuDuration GetTotalCpuTime(int bucketIndex) const
    {
        YT_VERIFY(IsValidBucketIndex(bucketIndex));
        return TotalCpuTime_[bucketIndex];
    }

private:
    const IFairShareCallbackQueuePtr UnderlyingCallbackQueue_;
    std::vector<std::atomic<NProfiling::TCpuDuration>> TotalCpuTime_;

    bool IsValidBucketIndex(int bucketIndex) const
    {
        return 0 <= bucketIndex && bucketIndex < std::ssize(TotalCpuTime_);
    }
};

using TMockFairShareCallbackQueuePtr = TIntrusivePtr<TMockFairShareCallbackQueue>;

////////////////////////////////////////////////////////////////////////////////

class TProfiledFairShareInvokerPoolTest
    : public ::testing::Test
{
protected:
    std::array<TLazyIntrusivePtr<TActionQueue>, 2> Queues_;

    THashMap<IInvoker*, int> InvokerToIndex_;

    TMockFairShareCallbackQueuePtr MockCallbackQueue;

    struct TInvocationOrder
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
        std::vector<int> InvokerIndexes_;
    } InvocationOrder_;

    void TearDown() override
    {
        for (int i = 0; i < std::ssize(Queues_); ++i) {
            if (Queues_[i]) {
                Queues_[i]->Shutdown();
            }
        }
    }

    template <typename TInvokerPoolPtr>
    void InitializeInvokerToIndexMapping(const TInvokerPoolPtr& invokerPool, int invokerCount)
    {
        YT_VERIFY(invokerCount > 0);
        InvokerToIndex_.clear();
        for (int i = 0; i < invokerCount; ++i) {
            auto invoker = invokerPool->GetInvoker(i);
            InvokerToIndex_[invoker.Get()] = i;
        }
    }

    int GetInvokerIndex(IInvoker* invokerAddress) const
    {
        return GetOrCrash(InvokerToIndex_, invokerAddress);
    }

    int GetCurrentInvokerIndex() const
    {
        return GetInvokerIndex(GetCurrentInvoker());
    }

    void ClearInvocationOrder()
    {
        auto guard = Guard(InvocationOrder_.Lock_);
        InvocationOrder_.InvokerIndexes_.clear();
    }

    void PushInvokerIndexToInvocationOrder()
    {
        auto currentInvokerIndex = GetCurrentInvokerIndex();
        auto guard = Guard(InvocationOrder_.Lock_);
        InvocationOrder_.InvokerIndexes_.push_back(currentInvokerIndex);
    }

    std::vector<int> GetInvocationOrder()
    {
        auto guard = Guard(InvocationOrder_.Lock_);
        return InvocationOrder_.InvokerIndexes_;
    }

    TDiagnosableInvokerPoolPtr CreateInvokerPool(IInvokerPtr underlyingInvoker, int invokerCount, TSolomonRegistryPtr registry = nullptr)
    {
        std::vector<TString> bucketNames;

        for (int i = 0; i < invokerCount; ++i) {
            bucketNames.push_back("invoker_" + ToString(i));
        }
        auto result = CreateProfiledFairShareInvokerPool(
            std::move(underlyingInvoker),
            [this] (int bucketCount) {
                YT_VERIFY(bucketCount > 0);
                MockCallbackQueue = New<TMockFairShareCallbackQueue>(bucketCount);
                return MockCallbackQueue;
            },
            TAdjustedExponentialMovingAverage::DefaultHalflife,
            "TestInvokerPool",
            bucketNames,
            std::move(registry));
        InitializeInvokerToIndexMapping(result, invokerCount);
        return result;
    }

    void ExpectInvokerIndex(int invokerIndex)
    {
        EXPECT_EQ(invokerIndex, GetCurrentInvokerIndex());
    }

    void ExpectTotalCpuTime(int bucketIndex, TDuration expectedCpuTime, TDuration precision = Quantum / 2)
    {
        // Push dummy callback to the scheduler queue and synchronously wait for it
        // to ensure that all possible CPU time accounters were destroyed during fiber stack unwinding.
        for (int i = 0; i < std::ssize(Queues_); ++i) {
            if (Queues_[i]) {
                auto invoker = Queues_[i]->GetInvoker();
                BIND([] { }).AsyncVia(invoker).Run().Get().ThrowOnError();
            }
        }

        auto precisionValue = NProfiling::DurationToValue(precision);
        auto expectedValue = NProfiling::DurationToValue(expectedCpuTime);
        auto actualValue = NProfiling::CpuDurationToValue(MockCallbackQueue->GetTotalCpuTime(bucketIndex));
        EXPECT_GT(precisionValue, std::abs(expectedValue - actualValue));
    }

    void DoTestFairness(IInvokerPoolPtr invokerPool, int invokerCount)
    {
        YT_VERIFY(1 < invokerCount && invokerCount < 5);

        // Each invoker executes some number of callbacks of the same duration |Quantum * (2 ^ #invokerIndex)|.
        // Individual duration of callback and number of callbacks chosen
        // such that total duration is same for all invokers.
        auto getWeight = [] (int invokerIndex) {
            return (1 << invokerIndex);
        };
        auto getSpinDuration = [getWeight] (int invokerIndex) {
            return Quantum * getWeight(invokerIndex);
        };
        auto getCallbackCount = [getWeight, invokerCount] (int invokerIndex) {
            // Weights are supposed to be in the ascending order.
            return 4 * getWeight(invokerCount - 1) / getWeight(invokerIndex);
        };

        std::vector<TFuture<void>> futures;
        for (int i = 0; i < invokerCount; ++i) {
            for (int j = 0, callbackCount = getCallbackCount(i); j < callbackCount; ++j) {
                futures.push_back(
                    BIND([this, spinDuration = getSpinDuration(i)] {
                        PushInvokerIndexToInvocationOrder();
                        Spin(spinDuration);
                    }).AsyncVia(invokerPool->GetInvoker(i)).Run());
            }
        }

        AllSucceeded(futures).Get().ThrowOnError();

        auto invocationOrder = GetInvocationOrder();

        // Test is considered successful if at any moment of the execution
        // deviation of the weighted count of executed callbacks per invoker
        // is not greater than the threshold (see in the code below).
        std::vector<int> invocationCount(invokerCount);
        for (auto invokerIndex : invocationOrder) {
            YT_VERIFY(0 <= invokerIndex && invokerIndex < invokerCount);

            ++invocationCount[invokerIndex];

            auto getWeightedInvocationCount = [getWeight, &invocationCount] (int invokerIndex) {
                return invocationCount[invokerIndex] * getWeight(invokerIndex);
            };

            auto minWeightedInvocationCount = getWeightedInvocationCount(0);
            auto maxWeightedInvocationCount = minWeightedInvocationCount;
            for (int i = 0; i < invokerCount; ++i) {
                auto weightedInvocationCount = getWeightedInvocationCount(i);
                minWeightedInvocationCount = std::min(minWeightedInvocationCount, weightedInvocationCount);
                maxWeightedInvocationCount = std::max(maxWeightedInvocationCount, weightedInvocationCount);
            }

            // Compare threshold and deviation.
            EXPECT_GE(getWeight(invokerCount - 1), maxWeightedInvocationCount - minWeightedInvocationCount);
        }

        for (int i = 0; i < invokerCount; ++i) {
            EXPECT_EQ(getCallbackCount(i), invocationCount[i]);
        }
    }

    void DoTestFairness(int invokerCount)
    {
        DoTestFairness(
            CreateInvokerPool(Queues_[0]->GetInvoker(), invokerCount),
            invokerCount);
    }

    void DoTestSwitchTo(int switchToCount)
    {
        YT_VERIFY(switchToCount > 0);

        auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), switchToCount + 1);

        auto callback = BIND([this, invokerPool, switchToCount] () {
            for (int i = 1; i <= switchToCount; ++i) {
                ExpectInvokerIndex(i - 1);
                Spin(Quantum * i);
                SwitchTo(invokerPool->GetInvoker(i));
            }
            ExpectInvokerIndex(switchToCount);
            Spin(Quantum * (switchToCount + 1));
        }).AsyncVia(invokerPool->GetInvoker(0));

        callback.Run().Get().ThrowOnError();

        for (int i = 0; i <= switchToCount; ++i) {
            ExpectTotalCpuTime(i, Quantum * (i + 1));
        }
    }

    void DoTestWaitFor(int waitForCount)
    {
        YT_VERIFY(waitForCount > 0);

        auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 2);

        auto callback = BIND([waitForCount] {
            Spin(Quantum);
            for (int i = 0; i < waitForCount; ++i) {
                TDelayedExecutor::WaitForDuration(Quantum);
                Spin(Quantum);
            }
        }).AsyncVia(invokerPool->GetInvoker(0));

        callback.Run().Get().ThrowOnError();

        ExpectTotalCpuTime(0, Quantum * (waitForCount + 1));
        ExpectTotalCpuTime(1, TDuration::Zero());
    }

    static void Spin(TDuration duration)
    {
        NProfiling::TFiberWallTimer timer;
        while (timer.GetElapsedTime() < duration) {
        }
    }
};

TEST_F(TProfiledFairShareInvokerPoolTest, Fairness2)
{
    DoTestFairness(2);
}

TEST_F(TProfiledFairShareInvokerPoolTest, Fairness3)
{
    DoTestFairness(3);
}

TEST_F(TProfiledFairShareInvokerPoolTest, Fairness4)
{
    DoTestFairness(4);
}

TEST_F(TProfiledFairShareInvokerPoolTest, SwitchTo12)
{
    DoTestSwitchTo(1);
}

TEST_F(TProfiledFairShareInvokerPoolTest, SwitchTo123)
{
    DoTestSwitchTo(2);
}

TEST_F(TProfiledFairShareInvokerPoolTest, SwitchTo1234)
{
    DoTestSwitchTo(3);
}

TEST_F(TProfiledFairShareInvokerPoolTest, SwitchTo121)
{
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 2);

    auto callback = BIND([this, invokerPool] {
        SwitchTo(invokerPool->GetInvoker(0));
        ExpectInvokerIndex(0);
        Spin(Quantum);

        SwitchTo(invokerPool->GetInvoker(1));
        ExpectInvokerIndex(1);
        Spin(Quantum * 3);

        SwitchTo(invokerPool->GetInvoker(0));
        ExpectInvokerIndex(0);
        Spin(Quantum);
    }).AsyncVia(invokerPool->GetInvoker(0));

    callback.Run().Get().ThrowOnError();

    ExpectTotalCpuTime(0, Quantum * 2);
    ExpectTotalCpuTime(1, Quantum * 3);
}

TEST_F(TProfiledFairShareInvokerPoolTest, SwitchTo111AndSwitchTo222)
{
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 2);

    std::vector<TFuture<void>> futures;

    futures.push_back(
        BIND([this] {
            ExpectInvokerIndex(0);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(0);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(0);
            Spin(Quantum);
        }).AsyncVia(invokerPool->GetInvoker(0)).Run());

    futures.push_back(
        BIND([this] {
            ExpectInvokerIndex(1);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(1);
            Spin(Quantum);
            SwitchTo(GetCurrentInvoker());

            ExpectInvokerIndex(1);
            Spin(Quantum);
        }).AsyncVia(invokerPool->GetInvoker(1)).Run());

    AllSucceeded(futures).Get().ThrowOnError();

    ExpectTotalCpuTime(0, Quantum * 3);
    ExpectTotalCpuTime(1, Quantum * 3);
}

TEST_F(TProfiledFairShareInvokerPoolTest, WaitFor1)
{
    DoTestWaitFor(1);
}

TEST_F(TProfiledFairShareInvokerPoolTest, WaitFor2)
{
    DoTestWaitFor(2);
}

TEST_F(TProfiledFairShareInvokerPoolTest, WaitFor3)
{
    DoTestWaitFor(3);
}

TEST_F(TProfiledFairShareInvokerPoolTest, CpuTimeAccountingBetweenContextSwitchesIsNotSupportedYet)
{
    auto threadPool = CreateThreadPool(2, "ThreadPool");
    auto invokerPool = CreateInvokerPool(threadPool->GetInvoker(), 2);

    NThreading::TEvent started;

    // Start busy loop in the first thread via first fair share invoker.
    auto future = BIND([this, &started] {
        Spin(Quantum * 10);

        auto invocationOrder = GetInvocationOrder();
        EXPECT_TRUE(invocationOrder.empty());

        started.NotifyOne();

        Spin(Quantum * 50);

        invocationOrder = GetInvocationOrder();
        EXPECT_TRUE(!invocationOrder.empty());
    }).AsyncVia(invokerPool->GetInvoker(0)).Run();

    YT_VERIFY(started.Wait(Quantum * 100));

    // After 10 quantums of time (see notification of the #started variable) we start Fairness test in the second thread.
    // In case of better implementation we expect to have non-fair CPU time distribution between first and second invokers,
    // because first invoker is given more CPU time in the first thread (at least within margin of 10 quantums).
    // But CPU accounting is not supported for running callbacks, therefore we expect Fairness test to pass.
    DoTestFairness(invokerPool, 2);

    future.Get().ThrowOnError();
}

TEST_F(TProfiledFairShareInvokerPoolTest, GetTotalWaitTimeEstimateEmptyPool)
{
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 1);

    EXPECT_EQ(TDuration::Zero(), invokerPool->GetInvokerStatistics(0).TotalTimeEstimate);

    WaitFor(BIND([] {
        Spin(Quantum);
    }).AsyncVia(invokerPool->GetInvoker(0)).Run()).ThrowOnError();

    EXPECT_LE(invokerPool->GetInvokerStatistics(0).TotalTimeEstimate, Quantum + Margin);
}

TEST_F(TProfiledFairShareInvokerPoolTest, GetTotalWaitTimeEstimateStuckAction)
{
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 1);
    NThreading::TEvent event;

    auto action = BIND([&event]{
        event.Wait(TDuration::Seconds(100));
    })
    .AsyncVia(invokerPool->GetInvoker(0))
    .Run();

    TDelayedExecutor::WaitForDuration(Quantum);

    auto totalTimeEstimate = invokerPool->GetInvokerStatistics(0).TotalTimeEstimate;

    EXPECT_LE(totalTimeEstimate, Quantum + Margin);
    EXPECT_GE(totalTimeEstimate, Quantum - Margin);

    event.NotifyAll();
    WaitFor(std::move(action)).ThrowOnError();
}

TEST_F(TProfiledFairShareInvokerPoolTest, GetTotalWaitTimeEstimateRelevancyDecay)
{
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 1);
    // Make aggregator very forgetful.
    invokerPool->UpdateActionTimeRelevancyHalflife(TDuration::Zero());
    NThreading::TEvent event;

    auto action = BIND([&event]{
        event.Wait(100 * Quantum);
    })
    .AsyncVia(invokerPool->GetInvoker(0))
    .Run();

    TDelayedExecutor::WaitForDuration(Quantum);

    auto totalTimeEstimate = invokerPool->GetInvokerStatistics(0).TotalTimeEstimate;

    EXPECT_LE(totalTimeEstimate, Quantum + Margin);
    EXPECT_GE(totalTimeEstimate, Quantum - Margin);

    event.NotifyAll();
    WaitFor(std::move(action)).ThrowOnError();

    TDelayedExecutor::WaitForDuration(Quantum);

    EXPECT_LE(invokerPool->GetInvokerStatistics(0).TotalTimeEstimate, Margin);
}

TEST_F(TProfiledFairShareInvokerPoolTest, GetTotalWaitTimeEstimateSeveralActions)
{
    static constexpr int ActionCount = 3;

    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 1);
    // Make aggregator never forget a sample.
    invokerPool->UpdateActionTimeRelevancyHalflife(TDuration::Days(100000000000000000));

    std::vector<NThreading::TEvent> leashes(ActionCount);
    std::vector<TFuture<void>> actions;

    for (int idx = 0; idx < ActionCount; ++idx) {
        actions.emplace_back(BIND([&leashes, idx] {
            leashes[idx].Wait(100 * Quantum);
        })
        .AsyncVia(invokerPool->GetInvoker(0))
        .Run());
    }

    auto expectedTotalTimeEstimate = TDuration::Zero();
    auto start = GetInstant();

    for (int idx = 0; idx < ActionCount; ++idx) {
        TDelayedExecutor::WaitForDuration(Quantum);

        auto statistics = invokerPool->GetInvokerStatistics(0);
        auto expectedTotalTime = GetInstant() - start;

        if (idx == 0) {
            expectedTotalTimeEstimate = expectedTotalTime;
        } else {
            expectedTotalTimeEstimate = (expectedTotalTimeEstimate * idx + expectedTotalTime) / (idx + 1.0);
        }

        EXPECT_EQ(statistics.WaitingActionCount, ActionCount - idx);

        EXPECT_LE(statistics.TotalTimeEstimate, expectedTotalTimeEstimate + Margin)
            << TError("Index: %v.", idx).GetMessage();
        EXPECT_GE(statistics.TotalTimeEstimate, expectedTotalTimeEstimate - Margin)
            << TError("Index: %v.", idx).GetMessage();

        leashes[idx].NotifyOne();
        WaitFor(std::move(actions[idx])).ThrowOnError();
    }
}

TEST_F(TProfiledFairShareInvokerPoolTest, GetTotalWaitEstimateUncorrelatedWithOtherInvokers)
{
    auto executionOrderEnforcer = [] (int suggestedStep) {
        static int realStep = 0;
        EXPECT_EQ(realStep, suggestedStep);
        ++realStep;
    };
    auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), 2);
    // Make aggregator never forget a sample.
    invokerPool->UpdateActionTimeRelevancyHalflife(TDuration::Days(100000000000000000));

    std::vector<NThreading::TEvent> leashes(2);
    std::vector<TFuture<void>> actions;

    for (int idx = 0; idx < 2; ++idx) {
        actions.emplace_back(BIND([&executionOrderEnforcer, &leashes, idx] {
            if (idx == 0) {
                executionOrderEnforcer(0);
            } else {
                executionOrderEnforcer(2);
            }
            leashes[idx].Wait(100 * Quantum);
        })
        .AsyncVia(invokerPool->GetInvoker(0))
        .Run());
    }

    NThreading::TEvent secondaryLeash;
    auto secondaryAction = BIND([&executionOrderEnforcer, &secondaryLeash] {
        executionOrderEnforcer(1);
        secondaryLeash.Wait(100 * Quantum);
    }).AsyncVia(invokerPool->GetInvoker(1)).Run();

    auto start = GetInstant();

    TDelayedExecutor::WaitForDuration(Quantum);

    auto statistics = invokerPool->GetInvokerStatistics(0);
    auto expectedTotalTimeEstimate = GetInstant() - start;

    EXPECT_EQ(statistics.WaitingActionCount, 2);
    EXPECT_LE(statistics.TotalTimeEstimate, expectedTotalTimeEstimate + Margin);
    EXPECT_GE(statistics.TotalTimeEstimate, expectedTotalTimeEstimate - Margin);

    leashes[0].NotifyOne();
    WaitFor(std::move(actions[0])).ThrowOnError();

    // Second action will not be executed until the secondary action is released.

    leashes[1].NotifyOne();
    TDelayedExecutor::WaitForDuration(10 * Quantum);
    EXPECT_FALSE(actions[1].IsSet());

    // Release Secondary action.

    auto secondaryStatistics = invokerPool->GetInvokerStatistics(1);
    auto secondaryWaitTime = GetInstant() - start;

    EXPECT_EQ(secondaryStatistics.WaitingActionCount, 1);
    EXPECT_LE(secondaryStatistics.TotalTimeEstimate, secondaryWaitTime + Margin);
    EXPECT_GE(secondaryStatistics.TotalTimeEstimate, secondaryWaitTime - Margin);

    secondaryLeash.NotifyOne();
    WaitFor(std::move(secondaryAction)).ThrowOnError();
    WaitFor(std::move(actions[1])).ThrowOnError();

    statistics = invokerPool->GetInvokerStatistics(0);
    expectedTotalTimeEstimate = (expectedTotalTimeEstimate + (GetInstant() - start)) / 3.0;

    EXPECT_EQ(statistics.WaitingActionCount, 0);
    EXPECT_LE(statistics.TotalTimeEstimate, expectedTotalTimeEstimate + Margin);
    EXPECT_GE(statistics.TotalTimeEstimate, expectedTotalTimeEstimate - Margin);
}

////////////////////////////////////////////////////////////////////////////////

// TL;DR:
// 1) We make solomon exporter and profiled queue with custom registry so we can read counters.
// 2) We enqueue a bunch of actions into each invoker and wait for them to complete.
// 3) We read json from exporter, check that every sensor and tagset is present.
// 4) We convert json to yson to list of sensor.
// 5) We check that:
// 5.1) There is exactly one tagless enqueue/dequeue sensor containing total number of actions in "value";
// 5.2) There is exactly one enqueue/dequeue sensor with tag "thread":threadName
//      containing total number of actions in "value";
// 5.3) There is exactly one enqueue/dequeue sensor for each tagset {"thread":threadName, "bucket":"invoker_i"}
//      containing i number of actions in "value" for i from 0 to totalInvokerCount.
class TProfiledFairShareInvokerPoolProfilingTest
    : public TProfiledFairShareInvokerPoolTest
{
public:
    TSolomonExporterConfigPtr CreateExporterConfig()
    {
        auto config = New<TSolomonExporterConfig>();
        config->GridStep = TDuration::Seconds(1);
        config->EnableCoreProfilingCompatibility = true;
        config->EnableSelfProfiling = false;

        return config;
    }

    template <class F>
    void SyncRunCallback(IInvokerPtr invoker, F&& func)
    {
        WaitFor(BIND(std::forward<F>(func)).AsyncVia(invoker).Run()).ThrowOnError();
    }

    auto GetSensors(TString json)
    {
        for (auto& c : json) {
            if (c == ':') {
                c = '=';
            } else if (c == ',') {
                c = ';';
            }
        }

        auto yson = NYson::TYsonString(json);

        auto list = NYTree::ConvertToNode(yson)->AsMap()->FindChild("sensors");

        EXPECT_TRUE(list);

        return list->AsList()->GetChildren();
    }

    // Implementation detail is that if sensor present
    // for one tagset, it is present for all of them
    // thus I can't be bothered verifying it here.
    void VerifyCountersArePresent(int invokerCount, TString json)
    {
        TString sensorPrefix = "\"sensor\":\"yt.fair_share_invoker_pool";
        TString bucketPrefix = "\"bucket\":\"invoker_";
        TString threadName = "\"thread\":\"TestInvokerPool\"";

        auto checker = [&] (const TString& pattern) {
            EXPECT_TRUE(json.Contains(pattern))
                << TError("Pattern %v is missing", pattern).GetMessage();
        };

        checker(sensorPrefix + ".size\"");
        checker(sensorPrefix + ".dequeued\"");
        checker(sensorPrefix + ".enqueued\"");
        checker(sensorPrefix + ".time.wait.max\"");
        checker(sensorPrefix + ".time.exec.max\"");
        checker(sensorPrefix + ".time.cumulative\"");
        checker(sensorPrefix + ".time.total.max\"");
        checker(threadName);

        for (int i = 0; i < invokerCount; ++i) {
            checker(bucketPrefix + ToString(i) + "\"");
        }
    }

    void VerifyJson(int invokerCount, TString json)
    {
        VerifyCountersArePresent(invokerCount, json);

        THashMap<TString, int> invokerNameToEnqueued;
        bool taglessEnqueuedPresent = false;
        bool taglessDequeuedPresent = false;
        bool threadOnlyTagEnqueuedPresent = false;
        bool threadOnlyTagDequeuedPresent = false;

        int totalActions = 0;

        for (int i = 0; i < invokerCount; ++i) {
            invokerNameToEnqueued.emplace("invoker_" + ToString(i), i);
            totalActions += i;
        }

        THashMap<TString, int> invokerNameToDequeued = invokerNameToEnqueued;

        for (const auto& entry : GetSensors(json)) {
            auto mapEntry = entry->AsMap();
            auto labels = mapEntry->FindChild("labels")->AsMap();

            auto sensor = labels->FindChildValue<TString>("sensor");

            if (!sensor ||
                !(sensor == "yt.fair_share_invoker_pool.dequeued" ||
                sensor == "yt.fair_share_invoker_pool.enqueued"))
            {
                continue;
            }

            auto value = mapEntry->FindChildValue<int>("value");
            EXPECT_TRUE(value);

            auto& invokerNameToValue =
                sensor == "yt.fair_share_invoker_pool.enqueued" ?
                invokerNameToEnqueued :
                invokerNameToDequeued;

            if (auto threadName = labels->FindChildValue<TString>("thread")) {
                EXPECT_EQ(threadName, "TestInvokerPool");

                if (auto bucketName = labels->FindChildValue<TString>("bucket")) {
                    EXPECT_TRUE(bucketName->StartsWith("invoker_"));

                    EXPECT_TRUE(invokerNameToValue.contains(*bucketName));
                    EXPECT_EQ(invokerNameToValue[*bucketName], *value);
                    invokerNameToValue.erase(*bucketName);

                    continue;
                }

                if (sensor == "yt.fair_share_invoker_pool.enqueued") {
                    EXPECT_FALSE(std::exchange(threadOnlyTagEnqueuedPresent, true));
                } else {
                    EXPECT_FALSE(std::exchange(threadOnlyTagDequeuedPresent, true));
                }
            } else {
                if (sensor == "yt.fair_share_invoker_pool.enqueued") {
                    EXPECT_FALSE(std::exchange(taglessEnqueuedPresent, true));
                } else {
                    EXPECT_FALSE(std::exchange(taglessDequeuedPresent, true));
                }
            }

            EXPECT_EQ(value, totalActions);
        }

        EXPECT_TRUE(threadOnlyTagEnqueuedPresent);
        EXPECT_TRUE(threadOnlyTagDequeuedPresent);
        EXPECT_TRUE(taglessEnqueuedPresent);
        EXPECT_TRUE(taglessDequeuedPresent);

        EXPECT_TRUE(invokerNameToEnqueued.empty());
        EXPECT_TRUE(invokerNameToDequeued.empty());
    }

    void TestProfiler(int invokerCount)
    {
        auto registry = New<TSolomonRegistry>();
        auto invokerPool = CreateInvokerPool(Queues_[0]->GetInvoker(), invokerCount, registry);

        auto config = CreateExporterConfig();
        auto exporter = New<TSolomonExporter>(config, registry);

        exporter->Start();

        for (int invokerIdx = 0; invokerIdx < invokerCount; ++invokerIdx) {
            for (int actionIndex = 0; actionIndex < invokerIdx; ++actionIndex) {
                SyncRunCallback(invokerPool->GetInvoker(invokerIdx), []{});
            }
        }

        Sleep(TDuration::Seconds(5));

        auto json = exporter->ReadJson();
        ASSERT_TRUE(json);

        exporter->Stop();

        VerifyJson(invokerCount, std::move(*json));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TProfiledFairShareInvokerPoolProfilingTest, TestProfilerGenericFairShareInvokerPool1)
{
    TestProfiler(1);
}

TEST_F(TProfiledFairShareInvokerPoolProfilingTest, TestProfilerGenericFairShareInvokerPool2)
{
    TestProfiler(2);
}

TEST_F(TProfiledFairShareInvokerPoolProfilingTest, TestProfilerGenericFairShareInvokerPool4)
{
    TestProfiler(4);
}

TEST_F(TProfiledFairShareInvokerPoolProfilingTest, TestProfilerGenericFairShareInvokerPool10)
{
    TestProfiler(10);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EInvokerBuckets,
    ((Apple)                (1))
    ((Pear)                 (2))
    ((Orange)               (3))
    ((Watermelon)           (4))
);

TEST_F(TProfiledFairShareInvokerPoolProfilingTest, TestEnumIndexedProfilerGenericFairShareInvokerPool)
{
    auto registry = New<TSolomonRegistry>();
    auto invokerPool = CreateEnumIndexedProfiledFairShareInvokerPool<EInvokerBuckets>(
        Queues_[0]->GetInvoker(),
        [this] (int bucketCount) {
            YT_VERIFY(bucketCount > 0);
            MockCallbackQueue = New<TMockFairShareCallbackQueue>(bucketCount);
            return MockCallbackQueue;
        },
        TAdjustedExponentialMovingAverage::DefaultHalflife,
        "TestInvokerPool",
        registry);

    auto config = CreateExporterConfig();
    auto exporter = New<TSolomonExporter>(config, registry);

    exporter->Start();

    for (int invokerIdx = 0; invokerIdx < TEnumTraits<EInvokerBuckets>::GetDomainSize(); ++invokerIdx) {
        SyncRunCallback(invokerPool->GetInvoker(invokerIdx), []{});
    }

    Sleep(TDuration::Seconds(5));

    auto json = exporter->ReadJson();
    ASSERT_TRUE(json);

    exporter->Stop();

    TEnumIndexedArray<EInvokerBuckets, bool> mentions;
    std::ranges::fill(mentions, false);

    for (const auto& sensor : GetSensors(*json)) {
        auto labels = sensor->AsMap()->FindChild("labels")->AsMap();

        if (auto bucketName = labels->FindChildValue<TString>("bucket")) {
            mentions[TEnumTraits<EInvokerBuckets>::FromString(*bucketName)] = true;

            if (auto sensorName = labels->FindChildValue<TString>("sensor");
                sensorName &&
                sensorName == "yt.fair_share_invoker_pool.dequeued")
            {
                auto value = sensor->AsMap()->FindChildValue<int>("value");

                EXPECT_TRUE(value);
                EXPECT_EQ(*value, 1);
            }
        }
    }

    EXPECT_TRUE(std::ranges::all_of(mentions, std::identity{}));
}

} // namespace
} // namespace NYT::NConcurrency
