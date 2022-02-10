#include <library/cpp/testing/unittest/registar.h>

#include "ring_buffer.h"

#include <util/random/random.h>

Y_UNIT_TEST_SUITE(RingBuffer) {
    struct TRingBufferTester {
        TRingBuffer<unsigned> RingBuffer;

        unsigned NextPush;
        unsigned NextPop;

        TRingBufferTester()
            : NextPush()
            , NextPop()
        {
        }

        void Push() {
            //Cerr << "push " << NextPush << "\n";
            RingBuffer.Push(NextPush);
            NextPush += 1;
        }

        void Pop() {
            //Cerr << "pop " << NextPop << "\n";
            unsigned popped = RingBuffer.Pop();
            UNIT_ASSERT_VALUES_EQUAL(NextPop, popped);
            NextPop += 1;
        }

        bool Empty() const {
            UNIT_ASSERT_VALUES_EQUAL(RingBuffer.Size(), NextPush - NextPop);
            UNIT_ASSERT_VALUES_EQUAL(RingBuffer.Empty(), RingBuffer.Size() == 0);
            return RingBuffer.Empty();
        }
    };

    void Iter() {
        TRingBufferTester rb;

        while (rb.NextPush < 1000) {
            rb.Push();
            while (!rb.Empty() && RandomNumber<bool>()) {
                rb.Pop();
            }
        }

        while (!rb.Empty()) {
            rb.Pop();
        }
    }

    Y_UNIT_TEST(Random) {
        for (unsigned i = 0; i < 100; ++i) {
            Iter();
        }
    }
}
