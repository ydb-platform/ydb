#include <library/cpp/testing/unittest/registar.h>
#include "event_history.h"

using namespace NKikimr;
using namespace NHive;

Y_UNIT_TEST_SUITE(TEventHistoryResize) {
    using THistory = TMaybe<TSimpleRingBuffer<THiveEvent>>;

    THiveEvent FakeEvent(ui32 i) {
        return THiveEvent(TInstant::Seconds(i), EHiveEventType::Starting, EHiveEventReason::BootQueue, ToString(i));
    }

    // history of the given capacity filled with events 1..count
    THistory MakeHistory(ui64 capacity, ui32 count) {
        THistory history;
        history.ConstructInPlace(capacity);
        for (ui32 i = 1; i <= count; ++i) {
            history->PushBack(FakeEvent(i));
        }
        return history;
    }

    // Details of the stored events, oldest first
    TVector<TString> Contents(const THistory& history) {
        TVector<TString> result;
        for (size_t i = history->FirstIndex(); i < history->TotalSize(); ++i) {
            result.push_back((*history)[i].Details);
        }
        return result;
    }

    // TSimpleRingBuffer reserves exactly its capacity on construction and never grows, so the capacity,
    // observed as the AvailSize() bound after overfilling, is also the memory reserved by the history
    size_t Capacity(THistory& history) {
        for (ui32 i = 0; i < 100; ++i) {
            history->PushBack(FakeEvent(1000 + i));
        }
        return history->AvailSize();
    }

    Y_UNIT_TEST(UndefinedStaysUndefined) {
        THistory history;
        ResizeEventHistory(history, 10);
        UNIT_ASSERT(!history.Defined());
        ResizeEventHistory(history, 0);
        UNIT_ASSERT(!history.Defined());
    }

    Y_UNIT_TEST(ZeroDropsHistory) {
        THistory history = MakeHistory(5, 5);
        ResizeEventHistory(history, 0);
        UNIT_ASSERT(!history.Defined());
    }

    Y_UNIT_TEST(ShortenKeepsNewest) {
        THistory history = MakeHistory(5, 7); // holds 3..7
        ResizeEventHistory(history, 3);
        UNIT_ASSERT(history.Defined());
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"5", "6", "7"}));
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 3);
    }

    Y_UNIT_TEST(ShortenPartiallyFilled) {
        THistory history = MakeHistory(10, 4); // holds 1..4, room for 6 more
        ResizeEventHistory(history, 2);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"3", "4"}));
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 2);
    }

    Y_UNIT_TEST(ShortenToSameOrLargerCountKeepsAll) {
        THistory history = MakeHistory(10, 3);
        ResizeEventHistory(history, 3);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"1", "2", "3"}));
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 3);
    }

    Y_UNIT_TEST(EnlargeKeepsAllAndAddsRoom) {
        THistory history = MakeHistory(3, 5); // holds 3..5
        ResizeEventHistory(history, 6);
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"3", "4", "5"}));
        history->PushBack(FakeEvent(6));
        history->PushBack(FakeEvent(7));
        history->PushBack(FakeEvent(8));
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"3", "4", "5", "6", "7", "8"}));
        history->PushBack(FakeEvent(9));
        UNIT_ASSERT_VALUES_EQUAL(Contents(history), (TVector<TString>{"4", "5", "6", "7", "8", "9"}));
        UNIT_ASSERT_VALUES_EQUAL(Capacity(history), 6);
    }

    Y_UNIT_TEST(EventsSurviveIntact) {
        THistory history = MakeHistory(4, 6); // holds 3..6
        ResizeEventHistory(history, 2);
        const THiveEvent& oldest = (*history)[history->FirstIndex()];
        UNIT_ASSERT_VALUES_EQUAL(oldest.GetTimestamp(), TInstant::Seconds(5));
        UNIT_ASSERT_EQUAL(oldest.Type, EHiveEventType::Starting);
        UNIT_ASSERT_EQUAL(oldest.Reason, EHiveEventReason::BootQueue);
        UNIT_ASSERT_VALUES_EQUAL(oldest.Details, "5");
    }
}
