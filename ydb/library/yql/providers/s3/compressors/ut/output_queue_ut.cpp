#include <ydb/library/yql/providers/s3/compressors/output_queue_impl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TOutputQueueTests) {
    Y_UNIT_TEST(TestOutputQueueBasic) {
        TOutputQueue<0> queue;

        queue.Push("");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0);

        queue.Push("A");
        queue.Push("BB");
        queue.Push("CCC");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(queue.Volume(), 6);
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT(!queue.IsSealed());

        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "A");
        UNIT_ASSERT_VALUES_EQUAL(queue.Volume(), 5);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "BB");
        UNIT_ASSERT_VALUES_EQUAL(queue.Volume(), 3);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        UNIT_ASSERT(!queue.Empty());

        queue.Seal();
        UNIT_ASSERT(queue.IsSealed());

        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "CCC");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(queue.Volume(), 0);
        UNIT_ASSERT(queue.Empty());

        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "");
    }

    Y_UNIT_TEST(TestOutputQueueWithMaxItemSize) {
        TOutputQueue<0, 3> queue;

        queue.Push("A");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Push("XYZ");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        queue.Push("hello world");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 6);
        queue.Push("XYZXYZ");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 8);

        queue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "A");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "XYZ");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "hel");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "lo ");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "wor");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "ld");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "XYZ");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "XYZ");
        UNIT_ASSERT(queue.Empty());
    }

    Y_UNIT_TEST(TestOutputQueueWithMinItemSize) {
        TOutputQueue<3, 0> queue;

        queue.Push("XYZ");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Push("A");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        queue.Push("hello world");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);

        queue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "XYZ");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "Ahello world");

        queue = TOutputQueue<3, 0>();
        queue.Push("A");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "A");

        queue = TOutputQueue<3, 0>();
        queue.Push("hello world");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Push("A");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        queue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "hello worldA");
    }

    Y_UNIT_TEST(TestOutputQueueWithMinMaxItemSize) {
        TOutputQueue<3, 6> queue;

        queue.Push("AB");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Push("BCCD");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Push("XY");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        queue.Push("hello world !");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4);

        queue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "ABBCCD");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "XYhell");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "o worl");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "d !");

        queue = TOutputQueue<3, 6>();
        queue.Push("ABC");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Push("D");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        queue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "ABCD");

        TOutputQueue<4, 5> smallQueue;
        smallQueue.Push("ABCD");
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Size(), 1);
        smallQueue.Push("XYZ");
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Size(), 2);
        smallQueue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Pop(), "ABCD");
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Pop(), "XYZ");

        smallQueue = TOutputQueue<4, 5>();
        smallQueue.Push("ABCDE");
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Size(), 1);
        smallQueue.Push("X");
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Size(), 2);
        smallQueue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Pop(), "ABCD");
        UNIT_ASSERT_VALUES_EQUAL(smallQueue.Pop(), "EX");

        queue = TOutputQueue<3, 6>();
        queue.Push("ABCDEF");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        queue.Push("A");
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        queue.Seal();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "ABCD");
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), "EFA");
    }
}

} // namespace NYql
