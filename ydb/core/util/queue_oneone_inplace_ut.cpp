#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <library/cpp/threading/future/legacy_future.h>


#include "queue_oneone_inplace.h"

Y_UNIT_TEST_SUITE(TOneOneQueueTests) {

    Y_UNIT_TEST(TestSimpleEnqueueDequeue) {
        using TQueueType = TOneOneQueueInplace<ui64, 32>; // 3 values per chunk
        TQueueType queue;

        UNIT_ASSERT(queue.Pop() == 0);
        UNIT_ASSERT(queue.Head() == 0);

        queue.Push(10);
        queue.Push(11);
        queue.Push(12);
        queue.Push(13);
        queue.Push(14);

        UNIT_ASSERT_EQUAL(queue.Head(), 10);
        UNIT_ASSERT_EQUAL(queue.Pop(), 10);
        UNIT_ASSERT_EQUAL(queue.Head(), 11);
        UNIT_ASSERT_EQUAL(queue.Pop(), 11);
        UNIT_ASSERT_EQUAL(queue.Head(), 12);
        UNIT_ASSERT_EQUAL(queue.Pop(), 12);
        UNIT_ASSERT_EQUAL(queue.Head(), 13);
        UNIT_ASSERT_EQUAL(queue.Pop(), 13);
        UNIT_ASSERT_EQUAL(queue.Head(), 14);
        UNIT_ASSERT_EQUAL(queue.Pop(), 14);

        UNIT_ASSERT(queue.Pop() == 0);
    }

    Y_UNIT_TEST(CleanInDestructor) {
        using TQueueType = TOneOneQueueInplace<std::shared_ptr<bool> *, 32>;


        std::shared_ptr<bool> p(new bool(true));
        UNIT_ASSERT_VALUES_EQUAL(1u, p.use_count());

        {
            TAutoPtr<TQueueType, TQueueType::TPtrCleanDestructor> queue(new TQueueType());
            queue->Push(new std::shared_ptr<bool>(p));
            queue->Push(new std::shared_ptr<bool>(p));
            queue->Push(new std::shared_ptr<bool>(p));
            queue->Push(new std::shared_ptr<bool>(p));

            UNIT_ASSERT_VALUES_EQUAL(5u, p.use_count());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, p.use_count());
    }

    Y_UNIT_TEST(ReadIterator) {
        using TQueueType = TOneOneQueueInplace<int, 32>;
        TQueueType queue;

        queue.Push(10);
        queue.Push(11);
        queue.Push(12);
        queue.Push(13);
        queue.Push(14);

        auto it = queue.Iterator();
        while (it.Next());

        UNIT_ASSERT_EQUAL(queue.Head(), 10);
        UNIT_ASSERT_EQUAL(queue.Pop(), 10);
        UNIT_ASSERT_EQUAL(queue.Head(), 11);
        UNIT_ASSERT_EQUAL(queue.Pop(), 11);
        UNIT_ASSERT_EQUAL(queue.Head(), 12);
        UNIT_ASSERT_EQUAL(queue.Pop(), 12);
        UNIT_ASSERT_EQUAL(queue.Head(), 13);
        UNIT_ASSERT_EQUAL(queue.Pop(), 13);
        UNIT_ASSERT_EQUAL(queue.Head(), 14);
        UNIT_ASSERT_EQUAL(queue.Pop(), 14);

        UNIT_ASSERT(queue.Pop() == 0);
    }
}
