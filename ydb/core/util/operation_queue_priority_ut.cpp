#include "operation_queue.h"

#include "circular_queue.h"

#include <ydb/library/actors/core/monotonic_provider.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/generic/xrange.h>


namespace NKikimr {
namespace NOperationQueue {

struct TPriorityItem {
    int Id = 0;
    int Priority = 0;

    explicit TPriorityItem(int id)
        : Id(id)
    {}

    TPriorityItem(int id, int priority)
        : Id(id)
        , Priority(priority)
    {}

    TPriorityItem(const TPriorityItem&) = default;

    bool operator ==(const TPriorityItem& rhs) const {
        // note that only identity intentionally checked
        return Id == rhs.Id;
    }

    TPriorityItem& operator =(const TPriorityItem& rhs) = default;

    size_t Hash() const {
        return THash<int>()(Id);
    }

    explicit operator size_t() const {
        return Hash();
    }

    struct TLessByPriority {
        bool operator()(const TPriorityItem& lhs, const TPriorityItem& rhs) const {
            // note ">" is intentional
            return lhs.Priority > rhs.Priority;
        }
    };

    TString ToString() const {
        TStringStream ss;
        ss << "{" << Id << "," << Priority << "}";
        return ss.Str();
    }
};

using TPriorityQueue = TOperationQueue<
    TPriorityItem,
    TQueueWithPriority<
        TPriorityItem,
        TPriorityItem::TLessByPriority>>;

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


struct TOperationStarter : public TPriorityQueue::IStarter, public NOperationQueue::ITimer {
    TSimpleTimeProvider TimeProvider;

    TVector<TPriorityItem> StartHistory;
    TVector<TMonotonic> WakeupHistory;

    NOperationQueue::EStartStatus StartResult = NOperationQueue::EStartStatus::EOperationRunning;

    NOperationQueue::EStartStatus StartOperation(const TPriorityItem& itemId) override
    {
        StartHistory.push_back(itemId);
        return StartResult;
    }

    void SetWakeupTimer(TDuration delta) override
    {
        WakeupHistory.push_back(this->Now() + delta);
    }

    void OnTimeout(const TPriorityItem&) override
    {}

    TMonotonic Now() override
    {
        return TimeProvider.Now();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TPriorityQueueTest) {
    Y_UNIT_TEST(TestOrder) {
        TQueueWithPriority<TPriorityItem, TPriorityItem::TLessByPriority> queue;
        queue.Enqueue(TPriorityItem(2, 50));
        queue.Enqueue(TPriorityItem(3, 100));
        queue.Enqueue(TPriorityItem(6, 90));
        queue.Enqueue(TPriorityItem(8, 80));
        queue.Enqueue(TPriorityItem(1, 100));

        UNIT_ASSERT(queue.Remove(TPriorityItem(1)));
        UNIT_ASSERT(queue.Remove(TPriorityItem(2)));

        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), TPriorityItem(3, 100));
        queue.PopFront();

        UNIT_ASSERT(!queue.Remove(TPriorityItem(4)));
        UNIT_ASSERT(!queue.Remove(TPriorityItem(5)));

        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), TPriorityItem(6, 90));
        queue.PopFront();

        UNIT_ASSERT(!queue.Remove(TPriorityItem(7)));

        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), TPriorityItem(8, 80));
        queue.PopFront();

        UNIT_ASSERT(queue.Empty());
    }
};

Y_UNIT_TEST_SUITE(TPriorityOperationQueueTest) {
    Y_UNIT_TEST(ShouldStartEmpty) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Start();
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT(starter.StartHistory.empty());
        UNIT_ASSERT(starter.WakeupHistory.empty());
    }

    Y_UNIT_TEST(ShouldNotStartUntilStart) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Enqueue({1, 1});
        queue.Enqueue({2, 2});
        queue.Enqueue({3, 3});
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT(starter.StartHistory.empty());
        UNIT_ASSERT(starter.WakeupHistory.empty());
    }

    Y_UNIT_TEST(ShouldStartByPriority) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Enqueue({1, 1});
        queue.Enqueue({3, 3});
        queue.Enqueue({2, 2});

        queue.Start();

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);

        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(3, 3));

        queue.OnDone({3, 0});
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(2, 2));

        queue.OnDone({2, 0});
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(1, 1));

        // test that circular property works

        queue.OnDone({1, 0});
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(3, 1));

        queue.OnDone({3, 0});
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 5UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(2, 1));

        queue.OnDone({2, 0});
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 6UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(1, 1));
    }

    Y_UNIT_TEST(ShouldStartByPriorityWithRemove) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Enqueue({1, 1});
        queue.Enqueue({3, 3});
        queue.Enqueue({2, 2});

        queue.Start();

        UNIT_ASSERT(queue.Remove({2,2}));

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);

        queue.OnDone({3, 0});
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(1, 1));
    }

    Y_UNIT_TEST(ShouldUpdatePriorityReadyQueue) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Enqueue({1, 1});
        queue.Enqueue({3, 3});
        queue.Enqueue({2, 2});

        queue.Start();

        UNIT_ASSERT(queue.Update({1, 100}));
        queue.OnDone({3, 0});
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(1, 100));

        UNIT_ASSERT(queue.Update({3, 100}));
        queue.OnDone({1, 0});
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(3, 100));

        queue.OnDone({3, 0});
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(2, 2));
    }

    Y_UNIT_TEST(ShouldUpdatePriorityWaitingQueue) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        config.MinOperationRepeatDelay = TDuration::Minutes(10);
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Enqueue({1, 1});
        queue.Enqueue({3, 3});
        queue.Enqueue({2, 2});

        queue.Start();

        queue.OnDone({3, 0});
        queue.OnDone({2, 0});
        queue.OnDone({1, 0});

        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 0UL);

        UNIT_ASSERT(queue.Update({2, 3}));
        UNIT_ASSERT(queue.Update({3, 2}));
        UNIT_ASSERT(queue.Update({1, 1}));

        starter.TimeProvider.Move(TDuration::Hours(1));
        queue.Wakeup();

        UNIT_ASSERT_VALUES_EQUAL(queue.WaitingSize(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.RunningSize(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);

        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(2, 3));
        queue.OnDone({2, 0});

        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(3, 2));
        queue.OnDone({3, 0});

        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(1, 2));
    }

    Y_UNIT_TEST(ShouldReturnExecTimeWhenUpdateRunningPriority) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        config.MinOperationRepeatDelay = TDuration::Minutes(10);
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Start();

        starter.TimeProvider.Move(TDuration::Hours(1)); // 01:00:00

        queue.Enqueue({1, 1});
        queue.Enqueue({2, 2});

        starter.TimeProvider.Move(TDuration::Seconds(5)); // 01:00:05

        UNIT_ASSERT(queue.Update({1, 100}));

        starter.TimeProvider.Move(TDuration::Seconds(1)); // 01:00:06

        auto duration = queue.OnDone({1, 0});

        UNIT_ASSERT_VALUES_EQUAL(duration, TDuration::Seconds(6));

        UNIT_ASSERT_VALUES_EQUAL(starter.StartHistory.back(), TPriorityItem(2, 2));
    }

    Y_UNIT_TEST(UpdateNonExistingShouldReturnFalse) {
        TPriorityQueue::TConfig config;
        config.IsCircular = true;
        config.Timeout = Timeout;
        config.MinOperationRepeatDelay = TDuration::Minutes(10);
        TOperationStarter starter;

        TPriorityQueue queue(config, starter, starter);
        queue.Start();

        queue.Enqueue({1, 1});
        queue.Enqueue({2, 2});

        UNIT_ASSERT(!queue.Update({3, 100}));
    }
};

} // NOperationQueue
} // NKikimr

using TSomeQueue = NKikimr::NOperationQueue::TPriorityQueue;
template<>
void Out<TSomeQueue::TItemWithTs>(IOutputStream& o, const TSomeQueue::TItemWithTs& item) {
    o << "{" << item.Item << "," << item.Timestamp << "}";
}

template<>
void Out<NKikimr::NOperationQueue::TPriorityItem>(
    IOutputStream& o,
    const NKikimr::NOperationQueue::TPriorityItem& item)
{
    o << item.ToString();
}
