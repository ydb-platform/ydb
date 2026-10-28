#include <library/cpp/testing/unittest/registar.h>
#include "event_history.h"

using namespace NKikimr;
using namespace NHive;

Y_UNIT_TEST_SUITE(TEventHistoryTest) {
    THiveEvent FakeEvent(ui32 i) {
        return THiveEvent(TInstant::Seconds(i), EHiveEventType::Starting, EHiveEventReason::BootQueue, ToString(i));
    }

    // history of the given capacity filled with events 1..count
    TEventHistory MakeHistory(ui64 capacity, ui32 count) {
        TEventHistory history;
        for (ui32 i = 1; i <= count; ++i) {
            history.Push(capacity, FakeEvent(i));
        }
        return history;
    }

    // Details of the stored events, oldest first
    TVector<TString> Contents(const TEventHistory& history) {
        TVector<TString> result;
        history.ForEachNewestFirst([&result](const THiveEvent& event) {
            result.push_back(event.Details);
        });
        std::reverse(result.begin(), result.end());
        return result;
    }

    // TSimpleRingBuffer reserves exactly its capacity on construction and never grows, so the capacity
    // is also the memory reserved by the history; overfilling confirms the ring really is that small
    size_t Capacity(TEventHistory& history) {
        const size_t capacity = history.GetCapacity();
        for (ui32 i = 0; i < 100; ++i) {
            history.Push(capacity, FakeEvent(1000 + i));
        }
        UNIT_ASSERT_VALUES_EQUAL(history.GetSize(), capacity);
        return capacity;
    }

    Y_UNIT_TEST(EmptyStaysEmptyOnResize) {
        TEventHistory history;
        history.Resize(10);
        UNIT_ASSERT(history.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(history.GetCapacity(), 0);
        UNIT_ASSERT(history.GetNewest() == nullptr);
        history.Resize(0);
        UNIT_ASSERT(history.IsEmpty());
    }

    Y_UNIT_TEST(ZeroDropsHistory) {
        TEventHistory history = MakeHistory(5, 5);
        history.Resize(0);
        UNIT_ASSERT(history.IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(history.GetCapacity(), 0);
        UNIT_ASSERT_VALUES_EQUAL(history.GetSize(), 0);
    }

    Y_UNIT_TEST(ShortenKeepsNewest) {
        TEventHistory history = MakeHistory(5, 7); // holds 3..7
        history.Resize(3);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"5", "6", "7"}));
        UNIT_ASSERT_VALUES_EQUAL(history.GetNewest()->Details, "7");
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 3);
    }

    Y_UNIT_TEST(ShortenPartiallyFilled) {
        TEventHistory history = MakeHistory(10, 4); // holds 1..4, room for 6 more
        history.Resize(2);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"3", "4"}));
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 2);
    }

    Y_UNIT_TEST(ResizeToCurrentCountKeepsAll) {
        TEventHistory history = MakeHistory(10, 3);
        history.Resize(3);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"1", "2", "3"}));
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 3);
    }

    Y_UNIT_TEST(EnlargeKeepsAllAndAddsRoom) {
        TEventHistory history = MakeHistory(3, 5); // holds 3..5
        history.Resize(6);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"3", "4", "5"}));
        for (ui32 i = 6; i <= 8; ++i) {
            history.Push(6, FakeEvent(i));
        }
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"3", "4", "5", "6", "7", "8"}));
        history.Push(6, FakeEvent(9));
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"4", "5", "6", "7", "8", "9"}));
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 6);
    }

    Y_UNIT_TEST(PushAppliesConfiguredSizeLazily) {
        TEventHistory history;
        history.Push(0, FakeEvent(1)); // disabled: nothing is created
        UNIT_ASSERT(history.IsEmpty());
        history.Push(3, FakeEvent(1)); // created on the first event with the configured size
        UNIT_ASSERT_VALUES_EQUAL(history.GetCapacity(), 3);
        history.Push(3, FakeEvent(2));
        history.Push(3, FakeEvent(3));
        history.Push(3, FakeEvent(4));
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"2", "3", "4"}));
        history.Push(2, FakeEvent(5)); // setting shrank: resized before the push
        UNIT_ASSERT_VALUES_EQUAL(history.GetCapacity(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"4", "5"}));
        history.Push(4, FakeEvent(6)); // setting grew: everything kept, room added
        UNIT_ASSERT_VALUES_EQUAL(history.GetCapacity(), 4);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"4", "5", "6"}));
        history.Push(0, FakeEvent(7)); // disabled again: history dropped, event not recorded
        UNIT_ASSERT(history.IsEmpty());
    }

    Y_UNIT_TEST(EventsSurviveIntact) {
        TEventHistory history = MakeHistory(4, 6); // holds 3..6
        history.Resize(2);
        TVector<const THiveEvent*> events;
        history.ForEachNewestFirst([&events](const THiveEvent& event) {
            events.push_back(&event);
        });
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 2);
        const THiveEvent& oldest = *events.back();
        UNIT_ASSERT_VALUES_EQUAL(oldest.GetTimestamp(), TInstant::Seconds(5));
        UNIT_ASSERT_EQUAL(oldest.Type, EHiveEventType::Starting);
        UNIT_ASSERT_EQUAL(oldest.Reason, EHiveEventReason::BootQueue);
        UNIT_ASSERT_VALUES_EQUAL(oldest.Details, "5");
    }
}
