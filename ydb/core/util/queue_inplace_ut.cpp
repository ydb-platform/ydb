#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <library/cpp/threading/future/legacy_future.h>


#include "queue_inplace.h"

Y_UNIT_TEST_SUITE(TQueueInplaceTests) {

    struct TStruct {
        ui32 X;
        ui32 Y;
        TStruct(ui32 i)
            : X(i)
            , Y(i)
        {}
        TStruct(TStruct&& s)
            : X(s.X)
            , Y(s.Y)
        {}
        TStruct(const TStruct&) = delete;

        TStruct& operator=(TStruct&& s) = delete;
        TStruct& operator=(const TStruct&) = delete;

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

    Y_UNIT_TEST(DestroyInDestructor) {
        using TQueueType = TQueueInplace<std::shared_ptr<bool>, 32>;

        std::shared_ptr<bool> p(new bool(true));
        UNIT_ASSERT_VALUES_EQUAL(1u, p.use_count());

        {
            TQueueType queue;
            queue.Push(p);
            queue.Push(p);
            queue.Push(p);
            queue.Push(p);

            UNIT_ASSERT_VALUES_EQUAL(5u, p.use_count());

            queue.Pop();

            UNIT_ASSERT_VALUES_EQUAL(4u, p.use_count());
        }

        UNIT_ASSERT_VALUES_EQUAL(1u, p.use_count());
    }

    Y_UNIT_TEST(EmplacePopDefault) {
        using TQueueType = TQueueInplace<std::unique_ptr<int>, 32>;

        TQueueType queue;
        queue.Push(std::make_unique<int>(10));
        queue.Emplace(new int(11));
        queue.Emplace(new int(12));
        queue.Emplace(std::make_unique<int>(13));

        auto a = queue.PopDefault();
        UNIT_ASSERT(a && *a == 10);
        auto b = queue.PopDefault();
        UNIT_ASSERT(b && *b == 11);
        auto c = queue.PopDefault();
        UNIT_ASSERT(c && *c == 12);
        auto d = queue.PopDefault();
        UNIT_ASSERT(d && *d == 13);
        auto e = queue.PopDefault();
        UNIT_ASSERT(!e);
    }

    Y_UNIT_TEST(PopTooManyTimes) {
        using TQueueType = TQueueInplace<std::unique_ptr<int>, 32>;

        TQueueType queue;
        queue.Push(std::make_unique<int>(10));
        queue.Push(std::make_unique<int>(11));
        queue.Push(std::make_unique<int>(12));
        queue.Push(std::make_unique<int>(13));
        UNIT_ASSERT(queue.GetSize() == 4);

        queue.Pop();
        queue.Pop();
        queue.Pop();
        queue.Pop();
        queue.Pop();
        UNIT_ASSERT(queue.GetSize() == 0);
    }

    Y_UNIT_TEST(MoveConstructor) {
        using TQueueType = TQueueInplace<ui64, 32>;

        TQueueType a;
        a.Push(10);
        a.Push(11);
        a.Push(12);
        a.Push(13);
        UNIT_ASSERT(a.GetSize() == 4);

        TQueueType b(std::move(a));

        UNIT_ASSERT(a.GetSize() == 0);
        UNIT_ASSERT(a.PopDefault() == 0u);

        UNIT_ASSERT(b.GetSize() == 4);
        UNIT_ASSERT(b.PopDefault() == 10u);
        UNIT_ASSERT(b.PopDefault() == 11u);
        UNIT_ASSERT(b.PopDefault() == 12u);
        UNIT_ASSERT(b.PopDefault() == 13u);
        UNIT_ASSERT(b.PopDefault() == 0u);
    }

    Y_UNIT_TEST(MoveAssignment) {
        using TQueueType = TQueueInplace<ui64, 32>;

        TQueueType a;
        a.Push(10);
        a.Push(11);
        a.Push(12);
        a.Push(13);
        UNIT_ASSERT(a.GetSize() == 4);

        TQueueType b;
        b.Push(20);
        b.Push(21);
        b.Push(22);
        b.Push(23);
        UNIT_ASSERT(b.GetSize() == 4);

        a = std::move(b);

        UNIT_ASSERT(a.GetSize() == 4);
        UNIT_ASSERT(a.PopDefault() == 20u);
        UNIT_ASSERT(a.PopDefault() == 21u);
        UNIT_ASSERT(a.PopDefault() == 22u);
        UNIT_ASSERT(a.PopDefault() == 23u);
        UNIT_ASSERT(a.PopDefault() == 0u);

        UNIT_ASSERT(b.GetSize() == 0);
        UNIT_ASSERT(b.PopDefault() == 0u);
    }

}
