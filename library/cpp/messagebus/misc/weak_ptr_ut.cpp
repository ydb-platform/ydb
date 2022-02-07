#include <library/cpp/testing/unittest/registar.h>

#include "weak_ptr.h"

Y_UNIT_TEST_SUITE(TWeakPtrTest) {
    struct TWeakPtrTester: public TWeakRefCounted<TWeakPtrTester> {
        int* const CounterPtr;

        TWeakPtrTester(int* counterPtr)
            : CounterPtr(counterPtr)
        {
        }
        ~TWeakPtrTester() {
            ++*CounterPtr;
        }
    };

    Y_UNIT_TEST(Simple) {
        int destroyCount = 0;

        TIntrusivePtr<TWeakPtrTester> p(new TWeakPtrTester(&destroyCount));

        UNIT_ASSERT(!!p);
        UNIT_ASSERT_VALUES_EQUAL(1u, p->RefCount());

        TWeakPtr<TWeakPtrTester> p2(p);

        UNIT_ASSERT_VALUES_EQUAL(1u, p->RefCount());

        {
            TIntrusivePtr<TWeakPtrTester> p3 = p2.Get();
            UNIT_ASSERT(!!p3);
            UNIT_ASSERT_VALUES_EQUAL(2u, p->RefCount());
        }

        p.Drop();
        UNIT_ASSERT_VALUES_EQUAL(1, destroyCount);

        {
            TIntrusivePtr<TWeakPtrTester> p3 = p2.Get();
            UNIT_ASSERT(!p3);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, destroyCount);
    }
}
