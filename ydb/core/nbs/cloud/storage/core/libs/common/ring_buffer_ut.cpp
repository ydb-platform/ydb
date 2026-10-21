#include "ring_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRingBufferTest)
{
    Y_UNIT_TEST(ShouldReturnCorrectCapacity)
    {
        {
            auto ringBuffer = TRingBuffer<int>(1);
            UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Capacity(), 1);
        }

        {
            auto ringBuffer = TRingBuffer<int>(123);
            UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Capacity(), 123);
        }
    }

    Y_UNIT_TEST(ShouldReturnCorrectSize)
    {
        auto ringBuffer = TRingBuffer<int>(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);
    }

    Y_UNIT_TEST(ShouldReturnCorrectIsEmpty)
    {
        auto ringBuffer = TRingBuffer<int>(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);
    }

    Y_UNIT_TEST(ShouldReturnCorrectIsFull)
    {
        auto ringBuffer = TRingBuffer<int>(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), true);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), true);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);
    }

    Y_UNIT_TEST(ShouldCorrectlyClear)
    {
        auto ringBuffer = TRingBuffer<int>(2);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.Clear();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);

        ringBuffer.PushBack(1);
        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), true);

        ringBuffer.Clear();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);
    }

    Y_UNIT_TEST(ShouldCorrectlyPushPopBack)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);
    }

    Y_UNIT_TEST(ShouldCorrectlyPushPopFrontFront)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);
    }

    Y_UNIT_TEST(ShouldCorrectlyBack)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 3);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 4);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 5);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 4);

        ringBuffer.Clear();

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.Clear();

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 3);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 1);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PushBack(6);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 6);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Back(), 4);
    }

    Y_UNIT_TEST(ShouldCorrectlyFront)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 3);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 4);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 5);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 4);

        ringBuffer.Clear();

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);

        ringBuffer.Clear();

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 3);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PushFront(6);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 6);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 1);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Front(), 2);
    }

    Y_UNIT_TEST(ShouldCorrectlyReturnPoppedValue)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PushFront(1));
        UNIT_ASSERT_EQUAL(1, ringBuffer.PopFront());
        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PopFront());
        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PopBack());

        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PushFront(2));
        UNIT_ASSERT_EQUAL(2, *ringBuffer.PopBack());

        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PushBack(3));
        UNIT_ASSERT_EQUAL(3, *ringBuffer.PopFront());

        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PushBack(4));
        UNIT_ASSERT_EQUAL(4, *ringBuffer.PopBack());

        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PushBack(5));
        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PushBack(6));
        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PushFront(7));
        UNIT_ASSERT_EQUAL(6, *ringBuffer.PushFront(8));
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(5, *ringBuffer.PopBack());
        UNIT_ASSERT_VALUES_EQUAL(7, *ringBuffer.PopBack());
        UNIT_ASSERT_VALUES_EQUAL(8, *ringBuffer.PopBack());
        UNIT_ASSERT_EQUAL(std::nullopt, ringBuffer.PopBack());
    }

    Y_UNIT_TEST(ShouldCorrectlyGetWithOffset)
    {
        auto ringBuffer = TRingBuffer<int>(5);

        ringBuffer.PushBack(1);
        ringBuffer.PushBack(2);
        ringBuffer.PushBack(3);
        ringBuffer.PushBack(4);
        ringBuffer.PushBack(5);

        UNIT_ASSERT_EQUAL(5, ringBuffer.Back());
        UNIT_ASSERT_EQUAL(1, ringBuffer.Back(4));

        UNIT_ASSERT_EQUAL(1, ringBuffer.Front());
        UNIT_ASSERT_EQUAL(5, ringBuffer.Front(4));
    }

    Y_UNIT_TEST(ShouldCorrectPushAndPopWithZeroCapacity)
    {
        auto ringBuffer = TRingBuffer<int>(0);
        UNIT_ASSERT_VALUES_EQUAL(100, ringBuffer.PushBack(100).value());
        UNIT_ASSERT_VALUES_EQUAL(200, ringBuffer.PushFront(200).value());
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Capacity(), 0);
    }
}

}   // namespace NYdb::NBS
