#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <library/cpp/threading/future/legacy_future.h>


#include "queue_inplace.h"

Y_UNIT_TEST_SUITE(TQueueInplaceTests) {

    struct TStruct {
        ui32 X;
        ui32 Y;
        TStruct(ui32 i = 0)
            : X(i)
            , Y(i)
        {}

        TStruct &operator = (const TStruct &s) {
            X = s.X;
            Y = s.Y;
            return *this;
        }

        bool operator == (ui32 i) const {
            return X == i && Y == i;
        }
    };

    Y_UNIT_TEST(TestSimpleInplace) {
        using TQueueType = TQueueInplace<TStruct, 32>; // 3 values per chunk
        TQueueType queue;

        UNIT_ASSERT(queue.Head() == nullptr);

        queue.Push(10);
        queue.Push(11);
        queue.Push(12);
        queue.Push(13);
        queue.Push(14);

        UNIT_ASSERT(*queue.Head() == 10);
        queue.Pop();
        UNIT_ASSERT(*queue.Head() == 11);
        queue.Pop();
        UNIT_ASSERT(*queue.Head() == 12);
        queue.Pop();
        UNIT_ASSERT(*queue.Head() == 13);
        queue.Pop();
        UNIT_ASSERT(*queue.Head() == 14);
        queue.Pop();

        UNIT_ASSERT(queue.Head() == nullptr);
    }

    Y_UNIT_TEST(CleanInDestructor) {
        using TQueueType = TQueueInplace<std::shared_ptr<bool> *, 32>;

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

}
