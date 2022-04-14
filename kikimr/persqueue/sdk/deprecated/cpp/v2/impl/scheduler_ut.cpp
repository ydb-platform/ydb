#include "scheduler.h"
#include "persqueue_p.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>

namespace NPersQueue {
const void* Tag = (const void*)42;
Y_UNIT_TEST_SUITE(TSchedulerTest) {
    Y_UNIT_TEST(EmptySchedulerCanDestruct) {
        // Test that scheduler doesn't hang on till shutdown
        TScheduler scheduler(nullptr);
    }

    Y_UNIT_TEST(ExecutesInProperOrder) {
        TSystemEvent ev;
        bool callback1Executed = false;
        TIntrusivePtr<TPQLibPrivate> lib(new TPQLibPrivate({}));
        TScheduler scheduler(lib.Get());
        scheduler.Schedule(TDuration::MilliSeconds(100), Tag, [&callback1Executed, &ev] {
            UNIT_ASSERT(callback1Executed);
            ev.Signal();
        });
        scheduler.Schedule(TDuration::MilliSeconds(50), Tag, [&callback1Executed] {
            callback1Executed = true;
        });
        ev.Wait();
    }

    Y_UNIT_TEST(CancelsAndClearsData) {
        TIntrusivePtr<TPQLibPrivate> lib(new TPQLibPrivate({}));
        TScheduler scheduler(lib.Get());
        std::shared_ptr<TString> ptr(new TString());
        bool callbackExecuted = false;
        auto h = scheduler.Schedule(TDuration::Seconds(50), Tag, [&callbackExecuted, ptr] {
            callbackExecuted = true;
        });
        UNIT_ASSERT_VALUES_EQUAL(ptr.use_count(), 2);
        h->TryCancel();
        UNIT_ASSERT_VALUES_EQUAL(ptr.use_count(), 1);
        UNIT_ASSERT(!callbackExecuted);
        Sleep(TDuration::MilliSeconds(51));
        UNIT_ASSERT_VALUES_EQUAL(ptr.use_count(), 1);
        UNIT_ASSERT(!callbackExecuted);
    }

    Y_UNIT_TEST(ExitsThreadImmediately) {
        TIntrusivePtr<TPQLibPrivate> lib(new TPQLibPrivate({}));
        std::shared_ptr<TString> ptr(new TString());
        bool callback1Executed = false;
        bool callback2Executed = false;
        TSystemEvent ev, ev2;
        auto now = TInstant::Now();
        lib->GetScheduler().Schedule(TDuration::Seconds(500), Tag, [&callback1Executed, ptr] {
            callback1Executed = true;
        });
        lib->GetScheduler().Schedule(now, Tag, [&callback2Executed, &ev, &ev2, ptr] {
            callback2Executed = true;
            ev2.Wait();
            ev.Signal();
        });
        UNIT_ASSERT_VALUES_EQUAL(ptr.use_count(), 3);
        ev2.Signal();

        // kill scheduler
        ev.Wait();
        lib.Reset();

        UNIT_ASSERT_VALUES_EQUAL(ptr.use_count(), 1);
        UNIT_ASSERT(!callback1Executed);
        UNIT_ASSERT(callback2Executed);
    }
}
} // namespace NPersQueue
