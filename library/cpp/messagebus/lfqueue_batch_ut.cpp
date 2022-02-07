#include <library/cpp/testing/unittest/registar.h>

#include "lfqueue_batch.h"

Y_UNIT_TEST_SUITE(TLockFreeQueueBatch) {
    Y_UNIT_TEST(Order1) {
        TLockFreeQueueBatch<unsigned> q;
        {
            TAutoPtr<TVector<unsigned>> v(new TVector<unsigned>);
            v->push_back(0);
            v->push_back(1);
            q.EnqueueAll(v);
        }

        TVector<unsigned> r;
        q.DequeueAllSingleConsumer(&r);

        UNIT_ASSERT_VALUES_EQUAL(2u, r.size());
        for (unsigned i = 0; i < 2; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, r[i]);
        }

        r.clear();
        q.DequeueAllSingleConsumer(&r);
        UNIT_ASSERT_VALUES_EQUAL(0u, r.size());
    }

    Y_UNIT_TEST(Order2) {
        TLockFreeQueueBatch<unsigned> q;
        {
            TAutoPtr<TVector<unsigned>> v(new TVector<unsigned>);
            v->push_back(0);
            v->push_back(1);
            q.EnqueueAll(v);
        }
        {
            TAutoPtr<TVector<unsigned>> v(new TVector<unsigned>);
            v->push_back(2);
            v->push_back(3);
            v->push_back(4);
            q.EnqueueAll(v);
        }

        TVector<unsigned> r;
        q.DequeueAllSingleConsumer(&r);

        UNIT_ASSERT_VALUES_EQUAL(5u, r.size());
        for (unsigned i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, r[i]);
        }

        r.clear();
        q.DequeueAllSingleConsumer(&r);
        UNIT_ASSERT_VALUES_EQUAL(0u, r.size());
    }
}
