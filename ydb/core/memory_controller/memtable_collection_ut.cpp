#include <library/cpp/testing/unittest/registar.h>
#include "memtable_collection.h"

namespace NKikimr::NMemory {

namespace {

class TMemoryConsumer : public IMemoryConsumer {
public:
    ui64 GetConsumption() const {
        return Consumption;
    }

    void SetConsumption(ui64 value) override {
        Consumption = value;
    }

private:
    std::atomic<ui64> Consumption = 0;
};

}

Y_UNIT_TEST_SUITE(TMemTableMemoryConsumersCollection) {

#define VERIFY_TOTAL(expected) \
do { \
    UNIT_ASSERT_VALUES_EQUAL(collection->GetTotalConsumption(), expected); \
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("MemTable/TotalBytes")->Val(), i64(expected)); \
    UNIT_ASSERT_VALUES_EQUAL(collectionConsumer->GetConsumption(), expected); \
} while (false)

#define VERIFY_COMPACTING(expected) \
do { \
    UNIT_ASSERT_VALUES_EQUAL(collection->GetTotalCompacting(), expected); \
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("MemTable/CompactingBytes")->Val(), i64(expected)); \
} while (false)

Y_UNIT_TEST(Empty) {
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto collectionConsumer = MakeIntrusive<TMemoryConsumer>();
    auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);

    VERIFY_TOTAL(0);
    VERIFY_COMPACTING(0);

    UNIT_ASSERT(collection->SelectForCompaction(0).empty());

    VERIFY_TOTAL(0);
    VERIFY_COMPACTING(0);
}

Y_UNIT_TEST(Destruction) {
    std::weak_ptr<TMemTableMemoryConsumersCollection> wCollection;

    {
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        auto collectionConsumer = MakeIntrusive<TMemoryConsumer>();
        auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);
        wCollection = collection;

        auto consumer = collection->Register(TActorId{1, 10}, 100);
        consumer->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);
    }

    UNIT_ASSERT(!wCollection.lock());
}

Y_UNIT_TEST(Register) {
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto collectionConsumer = MakeIntrusive<TMemoryConsumer>();
    auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);

    { // non-compacting consumer
        auto consumer = collection->Register(TActorId{1, 10}, 100);
        consumer->SetConsumption(1000);

        // weak pointer doesn't use
        UNIT_ASSERT_VALUES_EQUAL(collection.use_count(), 1);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(consumer->RefCount(), 3);
        collection->Unregister(TActorId{1, 10}, 100);
        UNIT_ASSERT_VALUES_EQUAL(consumer->RefCount(), 1);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        consumer.Reset();
        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }

    { // compacting consumer
        auto consumer = collection->Register(TActorId{1, 10}, 100);
        consumer->SetConsumption(1000);
        UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(1).size(), 1);

        // weak pointer doesn't use
        UNIT_ASSERT_VALUES_EQUAL(collection.use_count(), 1);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(1000);

        UNIT_ASSERT_VALUES_EQUAL(consumer->RefCount(), 3);
        collection->Unregister(TActorId{1, 10}, 100);
        UNIT_ASSERT_VALUES_EQUAL(consumer->RefCount(), 1);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        consumer.Reset();
        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }

    { // duplicate consumer
        auto registration1 = collection->Register(TActorId{1, 10}, 100);
        registration1->SetConsumption(1000);

        auto registration2 = collection->Register(TActorId{1, 10}, 100);
        UNIT_ASSERT_VALUES_EQUAL(registration1, registration2);

        // weak pointer doesn't use
        UNIT_ASSERT_VALUES_EQUAL(collection.use_count(), 1);

        registration2->SetConsumption(2000);
        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(registration2->RefCount(), 4);
        collection->Unregister(TActorId{1, 10}, 100);
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
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto collectionConsumer = MakeIntrusive<TMemoryConsumer>();
    auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);

    { // double unregister
        auto consumer = collection->Register(TActorId{1, 10}, 100);
        consumer->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        collection->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        collection->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(consumer->RefCount(), 1);
    }

    { // unregister and register back
        auto registration1 = collection->Register(TActorId{1, 10}, 100);
        registration1->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        collection->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        auto registration2 = collection->Register(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        registration2->SetConsumption(2000);

        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(0);
    }

    { // someone else unregister
        auto consumer = collection->Register(TActorId{1, 10}, 100);
        consumer->SetConsumption(1000);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        collection->Unregister(TActorId{1, 10}, 200);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(consumer->RefCount(), 3);

        collection->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(consumer->RefCount(), 1);
    }
}

Y_UNIT_TEST(SetConsumption) {
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto collectionConsumer = MakeIntrusive<TMemoryConsumer>();
    auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);

    { // non-compacting updates
        auto consumer = collection->Register(TActorId{1, 10}, 100);
        
        consumer->SetConsumption(1000);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        consumer->SetConsumption(2000);
        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(0);

        consumer->SetConsumption(999);
        VERIFY_TOTAL(999);
        VERIFY_COMPACTING(0);

        collection->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }

    { // compacting updates
        auto consumer = collection->Register(TActorId{1, 10}, 100);
        
        consumer->SetConsumption(1000);
        UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(1).size(), 1);

        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(1000);

        consumer->SetConsumption(2000);
        VERIFY_TOTAL(2000);
        VERIFY_COMPACTING(1000);

        consumer->SetConsumption(999);
        VERIFY_TOTAL(999);
        VERIFY_COMPACTING(1000);

        collection->Unregister(TActorId{1, 10}, 100);

        VERIFY_TOTAL(0);
        VERIFY_COMPACTING(0);
    }
}

Y_UNIT_TEST(CompactionComplete) {
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto collectionConsumer = MakeIntrusive<TMemoryConsumer>();
    auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);

    {
        auto consumer = collection->Register(TActorId{1, 10}, 100);
        
        consumer->SetConsumption(1000);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);

        UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(1).size(), 1);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(1000);

        collection->CompactionComplete(consumer);
        VERIFY_TOTAL(1000);
        VERIFY_COMPACTING(0);
    }
}

Y_UNIT_TEST(SelectForCompaction) {
    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    auto collectionConsumer = MakeIntrusive<TMemoryConsumer>();
    auto collection = std::make_shared<TMemTableMemoryConsumersCollection>(counters, collectionConsumer);

    TVector<TIntrusivePtr<TMemTableMemoryConsumer>> consumers;
    for (size_t i = 0; i < 3; i++) {
        consumers.push_back(collection->Register(TActorId{1, 10}, 100 + i));
    }

    auto reset = [&]() {
        consumers[0]->SetConsumption(3);
        consumers[1]->SetConsumption(100);
        consumers[2]->SetConsumption(20);

        for (size_t i = 0; i < 3; i++) {
            collection->CompactionComplete(consumers[i]);
        }

        VERIFY_TOTAL(123);
        VERIFY_COMPACTING(0);
    };

    TVector<std::pair<TIntrusivePtr<TMemTableMemoryConsumer>, ui64>> expected;

    reset();
    expected = {{consumers[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(50), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{consumers[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(23), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{consumers[1], 100}, {consumers[2], 20}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(22), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(120);

    reset();
    expected = {{consumers[1], 100}, {consumers[2], 20}, {consumers[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(2), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(123);

    reset();
    expected = {{consumers[1], 100}, {consumers[2], 20}, {consumers[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(0), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(123);

    reset();
    expected = {{consumers[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(50), expected);
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(40).size(), 0); // compacts max, not sum
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{consumers[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(50), expected);
    expected = {{consumers[2], 20}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(5), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(120);

    reset();
    expected = {{consumers[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(50), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);
    collection->CompactionComplete(consumers[1]);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(0);
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(50), expected);
    VERIFY_TOTAL(123);
    VERIFY_COMPACTING(100);

    reset();
    expected = {{consumers[1], 100}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(50), expected);
    consumers[1]->SetConsumption(1000);
    VERIFY_TOTAL(1023);
    VERIFY_COMPACTING(100); // didn't update
    
    reset();
    consumers[2]->SetConsumption(0);
    expected = {{consumers[1], 100}, {consumers[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(0), expected);
    VERIFY_TOTAL(103);
    VERIFY_COMPACTING(103);

    reset();
    collection->Unregister(TActorId{1, 10}, 101);
    expected = {{consumers[2], 20}, {consumers[0], 3}};
    UNIT_ASSERT_VALUES_EQUAL(collection->SelectForCompaction(0), expected);
    VERIFY_TOTAL(23);
    VERIFY_COMPACTING(23);
    consumers[1] = collection->Register(TActorId{1, 10}, 101);
}

}

}
