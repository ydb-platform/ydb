#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/event_holder_pool.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

#include <atomic>
#include <iostream>

using namespace NActors;

template<typename T>
TEventHolderPool Setup(T&& callback) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->DestructorQueueSize = std::make_shared<std::atomic<TAtomicBase>>();
    common->MaxDestructorQueueSize = 1024 * 1024;
    return TEventHolderPool(common, callback);
}

Y_UNIT_TEST_SUITE(EventHolderPool) {

    Y_UNIT_TEST(Overflow) {
        TDeque<THolder<IEventBase>> freeQ;
        auto callback = [&](THolder<IEventBase> event) {
            freeQ.push_back(std::move(event));
        };
        auto pool = Setup(std::move(callback));

        std::list<TEventHolder> q;

        auto& ev1 = pool.Allocate(q);
        ev1.Buffer = MakeIntrusive<TEventSerializedData>(TString::Uninitialized(512 * 1024), TEventSerializationInfo{});

        auto& ev2 = pool.Allocate(q);
        ev2.Buffer = MakeIntrusive<TEventSerializedData>(TString::Uninitialized(512 * 1024), TEventSerializationInfo{});

        auto& ev3 = pool.Allocate(q);
        ev3.Buffer = MakeIntrusive<TEventSerializedData>(TString::Uninitialized(512 * 1024), TEventSerializationInfo{});

        auto& ev4 = pool.Allocate(q);
        ev4.Buffer = MakeIntrusive<TEventSerializedData>(TString::Uninitialized(512 * 1024), TEventSerializationInfo{});

        pool.Release(q, q.begin());
        pool.Release(q, q.begin());
        pool.Trim();
        UNIT_ASSERT_VALUES_EQUAL(freeQ.size(), 1);

        pool.Release(q, q.begin());
        UNIT_ASSERT_VALUES_EQUAL(freeQ.size(), 1);

        freeQ.clear();
        pool.Release(q, q.begin());
        pool.Trim();
        UNIT_ASSERT_VALUES_EQUAL(freeQ.size(), 1);

        freeQ.clear(); // if we don't this, we may probablty crash due to the order of object destruction
    }

    struct TMemProfiler {
        size_t UsedAtStart = 0;


        TMemProfiler()
            : UsedAtStart(0)
        {
            UsedAtStart = GetUsed();

            const auto &info = NMalloc::MallocInfo();
            bool tcmallocIsUsed = TStringBuf(info.Name).StartsWith("tc");
            UNIT_ASSERT(tcmallocIsUsed);
        }

        size_t GetUsed() {
            auto properties = tcmalloc::MallocExtension::GetProperties();
            auto x = properties["generic.bytes_in_use_by_app"];
            return x.value - UsedAtStart;
        }
    };

    void MemComsumption(size_t repeats, size_t buffSize) {
        TDeque<THolder<IEventBase>> freeQ;
        auto callback = [&](THolder<IEventBase> event) {
            freeQ.push_back(std::move(event));
        };
        auto pool = Setup(std::move(callback));

        std::list<TEventHolder> q;
        TMemProfiler prof;

        for (ui32 i = 0; i < repeats; i++) {
            TEventHolder& event = pool.Allocate(q);
            TString data = TString::Uninitialized(buffSize);
            auto holder = MakeHolder<IEventHandle>(TActorId{}, TActorId{},  new TEvents::TEvBlob(data));
            event.Fill(*holder);

            pool.Release(q, q.begin());
            UNIT_ASSERT_LT_C(prof.GetUsed(), TEventHolderPool::MaxBytesPerMessage * 2, prof.GetUsed());
        }

        for (ui32 i = 0; i < repeats; i++) {
            TEventHolder& event = pool.Allocate(q);
            TString data = TString::Uninitialized(buffSize);
            auto holder = MakeHolder<IEventHandle>(TActorId{}, TActorId{},  new TEvents::TEvBlob(data));
            event.Fill(*holder);
        }
        for (ui32 i = 0; i < repeats; i++) {
            pool.Release(q, q.begin());
        }
        UNIT_ASSERT_LT_C(prof.GetUsed(), TEventHolderPool::MaxBytesPerMessage * 2, prof.GetUsed());
    }

    Y_UNIT_TEST(MemConsumptionSmall) {
#ifdef _san_enabled_
        std::cout << "Skipping, because TCMalloc is unavailable\n";
        return;
#endif
        MemComsumption(100'000, 4);
    }

    Y_UNIT_TEST(MemConsumptionLarge) {
#ifdef _san_enabled_
        std::cout << "Skipping, because TCMalloc is unavailable\n";
        return;
#endif
        MemComsumption(10'000, 1024*1024);
    }
}
