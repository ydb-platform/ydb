#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/interconnect/event_holder_pool.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

#include <atomic>

using namespace NActors;

const size_t MaxDestructorQueueSize = 1024 * 1024;

template<typename T>
TEventHolderPool Setup(T&& callback) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->DestructorQueueSize = std::make_shared<std::atomic<TAtomicBase>>();
    common->MaxDestructorQueueSize = MaxDestructorQueueSize;
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

    struct TEvBlobLarge : TEvents::TEvBlob {
        char Data[4096];

        TEvBlobLarge(TString data)
            : TEvents::TEvBlob(std::move(data))
        {}
    };

    struct TMemProfiler {
        size_t UsedAtStart = 0;

        TMemProfiler()
            : UsedAtStart(0)
        {
            UsedAtStart = GetUsed();
        }

        size_t GetUsed() {
            auto properties = tcmalloc::MallocExtension::GetProperties();
            auto x = properties["generic.bytes_in_use_by_app"];
            return x.value - UsedAtStart;
        }
    };

    Y_UNIT_TEST(MemConsumptionSmall) {
        TDeque<THolder<IEventBase>> freeQ;
        auto callback = [&](THolder<IEventBase> event) {
            freeQ.push_back(std::move(event));
        };
        auto pool = Setup(std::move(callback));

        std::list<TEventHolder> q;
        TMemProfiler prof;

        const ui32 repeats = 500'000;

        for (ui32 i = 0; i < repeats; i++) {
            TEventHolder& event = pool.Allocate(q);
            TString data = TString::Uninitialized(10);
            auto holder = MakeHolder<IEventHandle>(TActorId{}, TActorId{},  new TEvBlobLarge(data));
            event.Fill(*holder);

            pool.Release(q, q.begin());

            if (i % (repeats / 100) == 0) {
                Cerr << prof.GetUsed() << Endl;
            }
        }
        Cerr << prof.GetUsed() << Endl;
        UNIT_ASSERT_LT(prof.GetUsed(), 2 * std::max(TEventHolderPool::MaxBytesPerMessage, MaxDestructorQueueSize));
        Cerr << "------------------------\n";

        for (ui32 i = 0; i < repeats; i++) {
            TEventHolder& event = pool.Allocate(q);
            TString data = TString::Uninitialized(10);
            auto holder = MakeHolder<IEventHandle>(TActorId{}, TActorId{},  new TEvents::TEvBlob(data));
            event.Fill(*holder);
        }
        for (ui32 i = 0; i < repeats; i++) {
            pool.Release(q, q.begin());
            if (i % (repeats / 50) == 0) {
                Cerr << prof.GetUsed() << Endl;
            }
        }
        Cerr << prof.GetUsed() << Endl;
        UNIT_ASSERT_LT(prof.GetUsed(), 2 * std::max(TEventHolderPool::MaxBytesPerMessage, MaxDestructorQueueSize));
    }

    Y_UNIT_TEST(MemConsumptionLarge) {
        TDeque<THolder<IEventBase>> freeQ;
        auto callback = [&](THolder<IEventBase> event) {
            freeQ.push_back(std::move(event));
        };
        auto pool = Setup(std::move(callback));

        std::list<TEventHolder> q;
        TMemProfiler prof;

        const ui32 repeats = 10'000;

        for (ui32 i = 0; i < repeats; i++) {
            TEventHolder& event = pool.Allocate(q);
            TString data = TString::Uninitialized(1024 * 1024);
            auto holder = MakeHolder<IEventHandle>(TActorId{}, TActorId{},  new TEvents::TEvBlob(data));
            event.Fill(*holder);

            pool.Release(q, q.begin());
            UNIT_ASSERT_LT(prof.GetUsed(), TEventHolderPool::MaxBytesPerMessage * 2);
        }
        Cerr << "------\n";

        for (ui32 i = 0; i < repeats; i++) {
            TEventHolder& event = pool.Allocate(q);
            TString data = TString::Uninitialized(1024 * 1024);
            auto holder = MakeHolder<IEventHandle>(TActorId{}, TActorId{},  new TEvents::TEvBlob(data));
            event.Fill(*holder);
        }
        for (ui32 i = 0; i < repeats; i++) {
            pool.Release(q, q.begin());
        }

        Cerr << prof.GetUsed() << Endl;
        UNIT_ASSERT_LT(prof.GetUsed(), 1_MB); // 1_MB for possible overhead
    }
}
