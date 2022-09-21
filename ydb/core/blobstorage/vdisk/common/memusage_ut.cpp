#include "memusage.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(TTrackable) {
    Y_UNIT_TEST(TVector) {
        ::NMonitoring::TDynamicCounters::TCounterPtr cntr(new NMonitoring::TCounterForPtr);
        TMemoryConsumer cons(cntr);
        ::NMonitoring::TDynamicCounters::TCounterPtr cntr2(new NMonitoring::TCounterForPtr);
        TMemoryConsumer cons2(cntr2);
        {
            TTrackableVector<ui32> v{TMemoryConsumer(cons)};
            v.emplace_back(1);
            v.emplace_back(2);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr), sizeof(ui32) * v.capacity());
        }
        {
            TTrackableVector<ui32> v1{TMemoryConsumer(cons), std::initializer_list<ui32>{1, 2, 3, 4, 5}};
            TTrackableVector<ui32> v2{TMemoryConsumer(cons2)};
            UNIT_ASSERT_VALUES_EQUAL(v1.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(v2.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr), sizeof(ui32) * v1.capacity());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr2), sizeof(ui32) * v2.capacity());
            DoSwap(v1, v2);
            UNIT_ASSERT_VALUES_EQUAL(v1.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(v2.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr), sizeof(ui32) * v1.capacity());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr2), sizeof(ui32) * v2.capacity());
            v1.swap(v2);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr), sizeof(ui32) * v1.capacity());
            UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr2), sizeof(ui32) * v2.capacity());
            UNIT_ASSERT_VALUES_EQUAL(v1.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(v2.size(), 0);
        }
        UNIT_ASSERT_EQUAL(*cntr, 0);
    }

    Y_UNIT_TEST(TList) {
        ::NMonitoring::TDynamicCounters::TCounterPtr cntr(new NMonitoring::TCounterForPtr);
        TMemoryConsumer cons(cntr);
        TTrackableList<ui32> list{TMemoryConsumer(cons)};
        list.push_back(1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*cntr), 3 * sizeof(void *));
    }

    Y_UNIT_TEST(TString) {
        ::NMonitoring::TDynamicCounters::TCounterPtr cntr(new NMonitoring::TCounterForPtr);
        TMemoryConsumer cons(cntr);
    }

    Y_UNIT_TEST(TBuffer) {
        ::NMonitoring::TDynamicCounters::TCounterPtr cntr(new NMonitoring::TCounterForPtr);
        TMemoryConsumer cons(cntr);
    }
}
