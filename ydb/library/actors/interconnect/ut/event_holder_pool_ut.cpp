#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/interconnect/event_holder_pool.h>

#include <atomic>

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

}
