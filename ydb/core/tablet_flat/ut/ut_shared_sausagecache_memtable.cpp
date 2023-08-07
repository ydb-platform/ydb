#include <library/cpp/testing/unittest/registar.h>
#include "shared_sausagecache.cpp"

namespace NKikimr {
namespace NTabletFlatExecutor {
Y_UNIT_TEST_SUITE(TSharedPageCacheMemTableTracker) {

#define VERIFY_TOTAL(expected) \
do { \
    UNIT_ASSERT_VALUES_EQUAL(tracker->GetTotalConsumption(), expected); \
    UNIT_ASSERT_VALUES_EQUAL(counters->MemTableTotalBytes->Val(), i64(expected)); \
} while (false)

#define VERIFY_COMPACTING(expected) \
do { \
    UNIT_ASSERT_VALUES_EQUAL(tracker->GetTotalCompacting(), expected); \
    UNIT_ASSERT_VALUES_EQUAL(counters->MemTableCompactingBytes->Val(), i64(expected)); \
} while (false)

Y_UNIT_TEST(Empty) {
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
    auto tracker = std::make_shared<TSharedPageCacheMemTableTracker>(counters);

    VERIFY_TOTAL(0);
    VERIFY_COMPACTING(0);

    UNIT_ASSERT(tracker->SelectForCompaction(100).empty());

    VERIFY_TOTAL(0);
    VERIFY_COMPACTING(0);
}

Y_UNIT_TEST(Destruction) {
    std::weak_ptr<TSharedPageCacheMemTableTracker> wTracker;

    {
        auto counters = MakeIntrusive<TSharedPageCacheCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
        auto tracker = std::make_shared<TSharedPageCacheMemTableTracker>(counters);
        wTracker = tracker;

        auto registration = tracker->Register(TActorId{1, 10}, 100);
        registration->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);
    }

    UNIT_ASSERT(!wTracker.lock());
}

Y_UNIT_TEST(Register) {
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
    auto tracker = std::make_shared<TSharedPageCacheMemTableTracker>(counters);

    { // non-compacting registration
        auto registration = tracker->Register(TActorId{1, 10}, 100);
        registration->SetConsumption(1000);

        // weak pointer doesn't use
        UNIT_ASSERT_VALUES_EQUAL(tracker.use_count(), 1);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(registration->RefCount(), 3);
        tracker->Unregister(TActorId{1, 10}, 100);
        UNIT_ASSERT_VALUES_EQUAL(registration->RefCount(), 1);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        registration.Reset();
        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }

    { // compacting registration
        auto registration = tracker->Register(TActorId{1, 10}, 100);
        registration->SetConsumption(1000);
        UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1).size(), 1);

        // weak pointer doesn't use
        UNIT_ASSERT_VALUES_EQUAL(tracker.use_count(), 1);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(1000);

        UNIT_ASSERT_VALUES_EQUAL(registration->RefCount(), 3);
        tracker->Unregister(TActorId{1, 10}, 100);
        UNIT_ASSERT_VALUES_EQUAL(registration->RefCount(), 1);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        registration.Reset();
        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }

    { // duplicate registration
        auto registration1 = tracker->Register(TActorId{1, 10}, 100);
        registration1->SetConsumption(1000);

        auto registration2 = tracker->Register(TActorId{1, 10}, 100);
        UNIT_ASSERT_VALUES_EQUAL(registration1, registration2);

        // weak pointer doesn't use
        UNIT_ASSERT_VALUES_EQUAL(tracker.use_count(), 1);

        registration2->SetConsumption(2000);
        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(registration2->RefCount(), 4);
        tracker->Unregister(TActorId{1, 10}, 100);
        UNIT_ASSERT_VALUES_EQUAL(registration2->RefCount(), 2);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        registration1.Reset();
        registration2.Reset();
        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }
}

Y_UNIT_TEST(Unregister) {
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
    auto tracker = std::make_shared<TSharedPageCacheMemTableTracker>(counters);

    { // double unregister
        auto registration = tracker->Register(TActorId{1, 10}, 100);
        registration->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        tracker->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        tracker->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(registration->RefCount(), 1);
    }

    { // unregister and register back
        auto registration1 = tracker->Register(TActorId{1, 10}, 100);
        registration1->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        tracker->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        auto registration2 = tracker->Register(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        registration2->SetConsumption(2000);

        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(0);
    }

    { // someone else unregister
        auto registration = tracker->Register(TActorId{1, 10}, 100);
        registration->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        tracker->Unregister(TActorId{1, 10}, 200);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(registration->RefCount(), 3);

        tracker->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(registration->RefCount(), 1);
    }
}

Y_UNIT_TEST(SetConsumption) {
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
    auto tracker = std::make_shared<TSharedPageCacheMemTableTracker>(counters);

    { // non-compacting updates
        auto registration = tracker->Register(TActorId{1, 10}, 100);
        
        registration->SetConsumption(1000);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        registration->SetConsumption(2000);
        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(0);

        registration->SetConsumption(999);
        VERIFY_TOTAL(999);
        VERIFY_COMPACTING(0);

        tracker->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }

    { // compacting updates
        auto registration = tracker->Register(TActorId{1, 10}, 100);
        
        registration->SetConsumption(1000);
        UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1).size(), 1);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(1000);

        registration->SetConsumption(2000);
        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(1000);

        registration->SetConsumption(999);
        VERIFY_TOTAL(999);
        VERIFY_COMPACTING(1000);

        tracker->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }
}

Y_UNIT_TEST(CompactionComplete) {
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
    auto tracker = std::make_shared<TSharedPageCacheMemTableTracker>(counters);

    {
        auto registration = tracker->Register(TActorId{1, 10}, 100);
        
        registration->SetConsumption(1000);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1).size(), 1);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(1000);

        tracker->CompactionComplete(registration);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);
    }
}

Y_UNIT_TEST(SelectForCompaction) {
    auto counters = MakeIntrusive<TSharedPageCacheCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
    auto tracker = std::make_shared<TSharedPageCacheMemTableTracker>(counters);

    TVector<TIntrusivePtr<TSharedPageCacheMemTableRegistration>> registrations;
    for (size_t i = 0; i < 3; i++) {
        registrations.push_back(tracker->Register(TActorId{1, 10}, 100 + i));
    }

    auto reset = [&]() {
        registrations[0]->SetConsumption(3);
        registrations[1]->SetConsumption(100);
        registrations[2]->SetConsumption(20);

        for (size_t i = 0; i < 3; i++) {
            tracker->CompactionComplete(registrations[i]);
        }

        VERIFY_TOTAL(123);
        VERIFY_COMPACTING(0);
    };

    TVector<std::pair<TIntrusivePtr<TSharedPageCacheMemTableRegistration>, ui64>> expected;

    reset();
    expected = {{registrations[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{registrations[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(100), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{registrations[1], 100}, {registrations[2], 20}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(101), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(120);

    reset();
    expected = {{registrations[1], 100}, {registrations[2], 20}, {registrations[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(123), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(123);

    reset();
    expected = {{registrations[1], 100}, {registrations[2], 20}, {registrations[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(999), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(123);

    reset();
    expected = {{registrations[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1), expected);
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(100).size(), 0); // compacts max, not sum
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{registrations[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1), expected);
    expected = {{registrations[2], 20}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(101), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(120);

    reset();
    expected = {{registrations[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);
    tracker->CompactionComplete(registrations[1]);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(0);
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{registrations[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(1), expected);
    registrations[1]->SetConsumption(1000);
    VERIFY_TOTAL(1023);
    VERIFY_COMPACTING(100); // didn't update
    
    reset();
    registrations[2]->SetConsumption(0);
    expected = {{registrations[1], 100}, {registrations[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(999), expected);
    VERIFY_TOTAL(103);
    VERIFY_COMPACTING(103);

    reset();
    tracker->Unregister(TActorId{1, 10}, 101);
    expected = {{registrations[2], 20}, {registrations[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(tracker->SelectForCompaction(100), expected);
    VERIFY_TOTAL(23);
    VERIFY_COMPACTING(23);
    registrations[1] = tracker->Register(TActorId{1, 10}, 101);
}

}
}
}
