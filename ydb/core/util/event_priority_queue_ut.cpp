#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/testing/unittest/registar.h>

#include "event_priority_queue.h"

namespace NKikimr {

static constexpr ui32 EvPriorityTest = 0;

struct TEvPriorityTest : NActors::TEventLocal<TEvPriorityTest, EvPriorityTest> {
    ui32 Priority;

    TEvPriorityTest(ui32 priority) : Priority(priority) {
    }
};

class PriorityTester {
private:
    size_t EventsToProcess_ = 0;
    std::vector<ui32> ProcessedPriorities_;

public:
    ui32 GetEventPriority(NActors::IEventHandle* ev) {
        return ev->Get<TEvPriorityTest>()->Priority;
    }

    void PushProcessIncomingEvent() {
        ++EventsToProcess_;
    }

    void ProcessEvent(std::unique_ptr<NActors::IEventHandle> ev) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvPriorityTest, Handle);
            default:
                Y_ABORT("Unexpected event type");
        }
    }

    bool AllEventsDone() {
        return EventsToProcess_ == 0;
    }

    const std::vector<ui32>& GetProcessedPriorities() {
        return ProcessedPriorities_;
    }

    void Handle(TEvPriorityTest::TPtr& ev) {
        ProcessedPriorities_.push_back(ev->Get()->Priority);
        Y_ABORT_UNLESS(EventsToProcess_ > 0);
        --EventsToProcess_;
    }

    TEventPriorityQueue<PriorityTester> Queue{*this};
};

Y_UNIT_TEST_SUITE(TEventPriorityQueueTest) {
    Y_UNIT_TEST(TestPriority) {
        PriorityTester tester;
        static constexpr size_t NUM_EVENTS = 100;
        std::vector<ui32> priorities;
        priorities.reserve(NUM_EVENTS);
        for (size_t i = 0; i < NUM_EVENTS; i++) {
            ui32 priority = (i * i) % 5;
            auto ev = new TEvPriorityTest(priority);
            auto handle = TAutoPtr(new NActors::IEventHandle({}, {}, ev));
            tester.Queue.EnqueueIncomingEvent(handle);
            priorities.push_back(priority);
        }

        while(!tester.AllEventsDone()) {
            tester.Queue.ProcessIncomingEvent();
        }

        std::sort(priorities.begin(), priorities.end());
        UNIT_ASSERT_VALUES_EQUAL(tester.GetProcessedPriorities(), priorities);
    }
};

} // NKikimr
