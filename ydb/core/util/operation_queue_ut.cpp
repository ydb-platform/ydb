#include "operation_queue.h"

#include "circular_queue.h"

#include <ydb/library/actors/core/monotonic_provider.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NOperationQueue {

namespace {

TDuration Timeout = TDuration::Minutes(10);

class TSimpleTimeProvider : public NActors::IMonotonicTimeProvider {
public:
    TMonotonic Now() override {
        return Now_;
    }

    void Move(TDuration delta) {
        Now_ += delta;
    }

    void Move(TMonotonic now) {
        Now_ = now;
    }

private:
    TMonotonic Now_;
};

using TQueue = TOperationQueue<int, TFifoQueue<int>>;

struct TOperationStarter : public TQueue::IStarter, public NOperationQueue::ITimer {
    TSimpleTimeProvider TimeProvider;

    TVector<int> StartHistory;
    TVector<TMonotonic> WakeupHistory;

    NOperationQueue::EStartStatus StartResult = NOperationQueue::EStartStatus::EOperationRunning;

    NOperationQueue::EStartStatus StartOperation(const int& itemId) override
    {
        StartHistory.push_back(itemId);
        return StartResult;
    }

    void SetWakeupTimer(TDuration delta) override
    {
        WakeupHistory.push_back(this->Now() + delta);
    }

    void OnTimeout(const int&) override
    {}

    TMonotonic Now() override
    {
        return TimeProvider.Now();
    }
};

void CheckQueue(
    const TQueue& queue,
    const TOperationStarter& starter,
    TVector<TQueue::TItemWithTs> runningGold,
    TVector<int> inQueueGold,
    TVector<int> startHistory,
    TVector<TMonotonic> wakeupHistory)
{
    auto running = queue.GetRunning();
    auto inQueue = queue.GetQueue();
    UNIT_ASSERT_VALUES_EQUAL(running.size(), runningGold.size());
    UNIT_ASSERT_VALUES_EQUAL(inQueue.size(), inQueueGold.size());
    UNIT_ASSERT_VALUES_EQUAL(running, runningGold);
    UNIT_ASSERT_VALUES_EQUAL(inQueue, inQueueGold);
    UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory, startHistory);
    UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory, wakeupHistory);
}

void TestStartInflightBeforeStart(int inflight, int pushN = 10) {
    TQueue::TConfig config;
    config.InflightLimit = inflight;
    config.Timeout = Timeout;
    TOperationStarter starter;

    auto now = starter.TimeProvider.Now();

    TQueue queue(config, starter, starter);

    for (int i: xrange(pushN)) {
        queue.Enqueue(i);
    }

    queue.Start();
    auto running = queue.GetRunning();
    auto startN = Min(inflight, pushN);
    UNIT_ASSERT_VALUES_EQUAL(running.size(), startN);

    int firstStarted = 0;
    int firstNotStarted = firstStarted + startN;

    TVector<TQueue::TItemWithTs> runningGold;
    for (int i: xrange(firstNotStarted)) {
        runningGold.push_back({i, now});
    }

    TVector<TMonotonic> wakeupsGold =
        { starter.TimeProvider.Now() + config.Timeout };

    CheckQueue(
        queue,
        starter,
        runningGold,
        xrange(firstNotStarted, pushN),
        xrange(firstNotStarted),
        wakeupsGold);
}

void TestInflightWithEnqueue(int inflight, int pushN = 10) {
    TQueue::TConfig config;
    config.InflightLimit = inflight;
    config.Timeout = Timeout;
    TOperationStarter starter;

    TQueue queue(config, starter, starter);
    queue.Start();

    TVector<TQueue::TItemWithTs> runningGold;
    TVector<int> queuedGold;

    TVector<TMonotonic> wakeupsGold =
        { starter.TimeProvider.Now() + TDuration::Seconds(1) + config.Timeout };

    int lastStarted = 0;

    for (int lastPushed: xrange(pushN)) {
        starter.TimeProvider.Move(TDuration::Seconds(1));
        auto now = starter.TimeProvider.Now();

        queue.Enqueue(lastPushed);

        if (runningGold.size() < (size_t)inflight) {
            lastStarted = lastPushed;
            runningGold.push_back({lastStarted, now});
        } else {
            queuedGold.push_back(lastPushed);
        }

        CheckQueue(
            queue,
            starter,
            runningGold,
            queuedGold,
            xrange(lastStarted + 1),
            wakeupsGold);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TCircularOperationQueueTest) {
    Y_UNIT_TEST(ShouldStartEmpty) {
        TQueue::TConfig config;
        config.IsCircular = true;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT(starter.StartHistory.empty());
        UNIT_ASSERT(starter.WakeupHistory.empty());
    }

    Y_UNIT_TEST(ShouldNotStartUntilStart) {
        TQueue::TConfig config;
        config.IsCircular = true;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT(starter.StartHistory.empty());
        UNIT_ASSERT(starter.WakeupHistory.empty());
    }

    Y_UNIT_TEST(ShouldStartInflight1) {
        TestStartInflightBeforeStart(1);
    }

    Y_UNIT_TEST(ShouldStartInflight2) {
        TestStartInflightBeforeStart(2);
    }

    Y_UNIT_TEST(ShouldStartInflight3) {
        TestStartInflightBeforeStart(3);
    }

    Y_UNIT_TEST(ShouldStartInflight10) {
        TestStartInflightBeforeStart(10, 10);
    }

    Y_UNIT_TEST(ShouldStartInflight100) {
        TestStartInflightBeforeStart(100);
    }

    Y_UNIT_TEST(ShouldStartInflightEnqueue1) {
        TestInflightWithEnqueue(1);
    }

    Y_UNIT_TEST(ShouldStartInflightEnqueue2) {
        TestInflightWithEnqueue(2);
    }

    Y_UNIT_TEST(ShouldStartInflightEnqueue3) {
        TestInflightWithEnqueue(3);
    }

    Y_UNIT_TEST(ShouldStartInflightEnqueue10) {
        TestInflightWithEnqueue(10, 10);
    }

    Y_UNIT_TEST(ShouldStartInflightEnqueue100) {
        TestInflightWithEnqueue(100);
    }

    Y_UNIT_TEST(CheckOnDoneInflight1) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);

        queue.OnDone(1);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({3, 4, 1}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 2);
    }

    Y_UNIT_TEST(CheckOnDoneInflight2) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 2;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        queue.OnDone(2);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({4, 5, 2}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[1].Item, 3);

        queue.OnDone(1);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({5, 2, 1}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 3);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[1].Item, 4);
    }

    Y_UNIT_TEST(CheckOnDoneNotExisting) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);

        queue.OnDone(5);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({2, 3, 4}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 1);
    }

    Y_UNIT_TEST(CheckRemoveNotRunning) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        UNIT_ASSERT(queue.Remove(3));

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({2, 4, 5}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 1);

        UNIT_ASSERT(queue.Remove(2));

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({4, 5}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 1);
    }

    Y_UNIT_TEST(CheckRemoveRunning) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);

        UNIT_ASSERT(!queue.Remove(1));

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({3, 4}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 2);
    }

    Y_UNIT_TEST(CheckRemoveWaiting) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        config.MinOperationRepeatDelay = TDuration::Minutes(10);
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);

        queue.OnDone(1);

        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 1UL);
        UNIT_ASSERT(queue.Remove(1));
        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 0UL);
    }

    Y_UNIT_TEST(CheckRemoveNotExisting) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);

        UNIT_ASSERT(!queue.Remove(5));

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({2, 3, 4}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 1);
    }

    Y_UNIT_TEST(CheckTimeout) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        starter.TimeProvider.Move(TDuration::Hours(1)); // 01:00:00
        auto t1 = starter.TimeProvider.Now();
        auto d1 = t1 + Timeout; // 01:10:00
        queue.Enqueue(1);

        starter.TimeProvider.Move(TDuration::Seconds(1));
        queue.Enqueue(2);
        queue.Enqueue(3);

        starter.TimeProvider.Move(TDuration::Seconds(10));
        queue.Enqueue(4);
        queue.Enqueue(5);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[0], d1);

        starter.TimeProvider.Move(d1);
        auto t2 = starter.TimeProvider.Now();
        auto d2 = t2 + Timeout;
        queue.Wakeup();

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({3, 4, 5, 1}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 2);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[1], d2);

        starter.TimeProvider.Move(d2);
        auto t3 = starter.TimeProvider.Now();
        auto d3 = t3 + Timeout;
        queue.Wakeup();

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({4, 5, 1, 2}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 3);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[2], d3);
    }

    Y_UNIT_TEST(CheckTimeoutWhenFirstItemRemoved) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        starter.TimeProvider.Move(TDuration::Hours(1)); // 01:00:00
        auto t1 = starter.TimeProvider.Now();
        auto d1 = t1 + Timeout; // 01:10:00
        queue.Enqueue(1);

        starter.TimeProvider.Move(TDuration::Seconds(1));
        queue.Enqueue(2);
        queue.Enqueue(3);

        starter.TimeProvider.Move(TDuration::Seconds(10));
        queue.Enqueue(4);
        queue.Enqueue(5);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[0], d1);

        starter.TimeProvider.Move(TDuration::Seconds(5));
        auto t2 = starter.TimeProvider.Now();
        auto d2 = t2 + Timeout;

        queue.Remove(1);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({3, 4, 5}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 2);

        // must still wait for the first wakeup
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[0], d1);

        starter.TimeProvider.Move(d1);
        queue.Wakeup();

        // check no changes in queue sinse last time except new wakeup scheduled

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({3, 4, 5}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRunning()[0].Item, 2);

        // must still wait for the first wakeup
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[1], d2);
    }

    Y_UNIT_TEST(ShouldScheduleWakeupWhenNothingStarted) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;

        TOperationStarter starter;
        starter.StartResult = NOperationQueue::EStartStatus::EOperationRetry;

        TQueue queue(config, starter, starter);
        queue.Start();

        starter.TimeProvider.Move(TDuration::Hours(1)); // 01:00:00
        auto t1 = starter.TimeProvider.Now();
        auto d1 = t1 + config.WakeupInterval;

        queue.Enqueue(1);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({1}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[0], d1);
    }

    Y_UNIT_TEST(ShouldScheduleWakeupWhenHasWaitingAndStart) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = TDuration::Zero(); // disable timeout
        config.MinOperationRepeatDelay = TDuration::Minutes(10);

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        starter.TimeProvider.Move(TDuration::Hours(1)); // 01:00:00

        queue.Enqueue(1);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);

        starter.TimeProvider.Move(TDuration::Minutes(1)); // 01:01:00
        queue.OnDone(1);
        auto t1 = starter.TimeProvider.Now();
        auto d1 = t1 + config.MinOperationRepeatDelay;

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 1UL);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[0], d1);

        // some spurious wakeup
        starter.TimeProvider.Move(TDuration::Minutes(1));
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);

        starter.TimeProvider.Move(d1);
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
    }

    Y_UNIT_TEST(UseMinOperationRepeatDelayWhenTimeout) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;
        config.MinOperationRepeatDelay = TDuration::Minutes(10);

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        starter.TimeProvider.Move(TDuration::Hours(1));

        queue.Enqueue(1);
        starter.TimeProvider.Move(Timeout + TDuration::Minutes(1));
        queue.Wakeup();

        auto t1 = starter.TimeProvider.Now();
        auto d1 = t1 + config.MinOperationRepeatDelay;

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 1UL);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.back(), d1);
    }

    Y_UNIT_TEST(ShouldReturnExecTime) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        starter.TimeProvider.Move(TDuration::Hours(1)); // 01:00:00

        queue.Enqueue(1);
        starter.TimeProvider.Move(TDuration::Seconds(5)); // 01:00:05
        auto duration = queue.OnDone(1);

        UNIT_ASSERT_VALUES_EQUAL(duration, TDuration::Seconds(5));
    }

    Y_UNIT_TEST(ShouldTryToStartAnotherOneWhenStartFails) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.Timeout = Timeout;

        TOperationStarter starter;
        starter.StartResult = NOperationQueue::EStartStatus::EOperationRetry;

        TQueue queue(config, starter, starter);
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        queue.Start();

        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 5UL);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);
        UNIT_ASSERT_VALUES_EQUAL(
            queue.GetQueue(),
            TVector<int>({1, 2, 3, 4, 5}));

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);
    }

    Y_UNIT_TEST(ShouldShuffle) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.ShuffleOnStart = true;
        config.InflightLimit = 5;

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        queue.Start();

        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 5UL);

        UNIT_ASSERT(starter.StartHistory != TVector<int>({1, 2, 3, 4, 5}));

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 5UL);
    }

    Y_UNIT_TEST(RemoveExistingWhenShuffle) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.ShuffleOnStart = true;
        config.InflightLimit = 5;

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);
        UNIT_ASSERT(queue.Remove(3));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);

        const auto& queueVector = queue.GetQueue();
        UNIT_ASSERT(Find(queueVector.begin(), queueVector.end(), 3) == queueVector.end());
    }

    Y_UNIT_TEST(RemoveNonExistingWhenShuffle) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.ShuffleOnStart = true;
        config.InflightLimit = 5;

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);
        UNIT_ASSERT(!queue.Remove(10));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);
    }

    Y_UNIT_TEST(BasicRPSCheck) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 2;
        config.MaxRate = 1.0;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        // initially buckets were full, thus 2 running
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 2UL);

        queue.OnDone(1);

        // should start another one because RPS smoothing
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 2UL);

        // some spurious wakeup
        starter.TimeProvider.Move(TDuration::MilliSeconds(100));
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 2UL);

        queue.OnDone(2);

        // some spurious wakeup2, note that RPS forbids
        // starting more operations
        starter.TimeProvider.Move(TDuration::MilliSeconds(100));
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);

        // 1 second left, we can start 1 more
        starter.TimeProvider.Move(TDuration::MilliSeconds(900));
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 2UL);

        starter.TimeProvider.Move(TDuration::Seconds(1));
        queue.OnDone(2);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 2UL);
    }

    Y_UNIT_TEST(BasicRPSCheckWithRound) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.MaxRate = 10.0;
        config.RoundInterval = TDuration::Seconds(100);
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        // no items yet, thus rate by MaxRate
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRate(), config.MaxRate);

        queue.Enqueue(1);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRate(), 1.0 / 100);

        queue.Enqueue(2);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRate(), 2.0 / 100);

        queue.Enqueue(3);
        queue.Enqueue(4);
        queue.Enqueue(5);

        UNIT_ASSERT_VALUES_EQUAL(queue.GetRate(), 5.0 / 100);

        // Note that remove should affect rate as well
        queue.Remove(5);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRate(), 4.0 / 100);

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);

        // OnDone should not affect the rate
        queue.OnDone(1);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetRate(), 4.0 / 100);

        // should start another one because RPS smoothing
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);

        queue.OnDone(2);

        // Queue should start items every 25 seconds,
        // thus now no items should be running
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);

        // some spurious wakeup1
        starter.TimeProvider.Move(TDuration::Seconds(10));
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);

        // some spurious wakeup1
        starter.TimeProvider.Move(TDuration::Seconds(10));
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);

        // wakeup3
        starter.TimeProvider.Move(TDuration::Seconds(6));
        queue.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
    }

    Y_UNIT_TEST(CheckWakeupAfterStop) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.ShuffleOnStart = true;
        config.InflightLimit = 3;
        config.Timeout = Timeout;

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        auto now = starter.TimeProvider.Now();
        for (auto i: xrange(1, 6)) {
            queue.Enqueue(i);
            starter.TimeProvider.Move(TDuration::Seconds(1));
        }

        queue.Stop();

        auto currentWakeup = now + Timeout;
        for (auto i: xrange(1, 4)) {
            UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), i);
            UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory[i-1], currentWakeup);
            starter.TimeProvider.Move(currentWakeup);
            queue.Wakeup();
            currentWakeup += TDuration::Seconds(1);
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);
    }

    Y_UNIT_TEST(CheckWakeupWhenRPSExhausted) {
        // regression case for the following case:
        // Config: Inglight = 1, RPS = 1/s, MinOperationRepeatDelay = 60s
        // 1. Enqueue multiple operations, 1 is running
        // 3. OnDone1, OnDone2 within same second, RPS is now exhausted.
        // 4. Wakeup should be +1 second, not +MinOperationRepeatDelay=60s as caused by bug

        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.MaxRate = 1.0;
        config.Timeout = Timeout;
        config.MinOperationRepeatDelay = TDuration::Seconds(60);
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);
        queue.Enqueue(3);

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        queue.OnDone(1);
        queue.OnDone(2);

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);

        auto goldWakeup = starter.TimeProvider.Now() + TDuration::Seconds(1);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.back(), goldWakeup);
    }

    Y_UNIT_TEST(CheckWakeupWhenRPSExhausted2) {
        // regression case for the following case:
        // 1. Enqueue operation 1.
        // 2. Done operation 1.
        // 3. Enqueue 2 and 3 - they should not add extra wakeups

        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.MaxRate = 0.5;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue(1);
        queue.Enqueue(2);

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL); // only timeout for 1

        queue.OnDone(1);

        // 2 is running now because token bucket allows
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL); // only first timeout

        queue.Enqueue(3);
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL); // only first timeout

        queue.OnDone(2);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL); // blocked by RPS
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 2UL); // start new one, when RPS allows

        queue.Enqueue(4);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL); // no change
        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 2UL); // no change
    }

    Y_UNIT_TEST(CheckStartAfterStop) {
        TQueue::TConfig config;
        config.IsCircular = true;
        config.ShuffleOnStart = true;
        config.InflightLimit = 3;
        config.Timeout = Timeout;

        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        for (auto i: xrange(1, 6)) {
            queue.Enqueue(i);
        }

        queue.Stop();
        for (auto i: xrange(1, 4)) {
            queue.OnDone(i);
        }

        starter.TimeProvider.Move(Timeout);
        queue.Wakeup();

        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);

        starter.TimeProvider.Move(Timeout);

        queue.Start();
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT(starter.WakeupHistory.back() > starter.TimeProvider.Now());
    }

    Y_UNIT_TEST(ShouldTolerateInaccurateTimer) {
        // should properly work when wokeup earlier than requested (i.e. properly set new timer)
        // regression test: woke up earlier and didn't set new wakeup

        TQueue::TConfig config;
        config.IsCircular = true;
        config.InflightLimit = 1;
        config.MaxRate = 0.0;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TQueue queue(config, starter, starter);
        queue.Start();

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 0UL);

        queue.Enqueue(1);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 1UL);

        // expect to wakeup on Timeout, but wakeup earlier
        starter.TimeProvider.Move(Timeout - TDuration::Seconds(1));
        queue.Wakeup();

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);

        UNIT_ASSERT_VALUES_EQUAL(starter.WakeupHistory.size(), 2UL);
    }
};

} // NOperationQueue
} // NKikimr

using TSomeQueue = NKikimr::NOperationQueue::TOperationQueue<int, NKikimr::TFifoQueue<int>>;
template<>
void Out<TSomeQueue::TItemWithTs>(IOutputStream& o, const TSomeQueue::TItemWithTs& item) {
    o << "{" << item.Item << "," << item.Timestamp << "}";
}
