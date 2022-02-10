#include "bitvector.h"
#include "readonly_bitvector.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/memory/blob.h>
#include <util/stream/buffer.h>

Y_UNIT_TEST_SUITE(TBitVectorTest) {
    Y_UNIT_TEST(TestEmpty) {
        TBitVector<ui64> v64;
        UNIT_ASSERT_EQUAL(v64.Size(), 0);
        UNIT_ASSERT_EQUAL(v64.Words(), 0);

        TBitVector<ui32> v32(0);
        UNIT_ASSERT_EQUAL(v32.Size(), 0);
        UNIT_ASSERT_EQUAL(v32.Words(), 0);
    }

    Y_UNIT_TEST(TestOneWord) {
        TBitVector<ui32> v;
        v.Append(1, 1);
        v.Append(0, 1);
        v.Append(1, 3);
        v.Append(10, 4);
        v.Append(100500, 20);

        UNIT_ASSERT_EQUAL(v.Get(0, 1), 1);
        UNIT_ASSERT(v.Test(0));
        UNIT_ASSERT_EQUAL(v.Get(1, 1), 0);
        UNIT_ASSERT_EQUAL(v.Get(2, 3), 1);
        UNIT_ASSERT_EQUAL(v.Get(5, 4), 10);
        UNIT_ASSERT_EQUAL(v.Get(9, 20), 100500);

        v.Reset(0);
        v.Set(9, 1234, 15);
        UNIT_ASSERT_EQUAL(v.Get(0, 1), 0);
        UNIT_ASSERT(!v.Test(0));
        UNIT_ASSERT_EQUAL(v.Get(9, 15), 1234);

        UNIT_ASSERT_EQUAL(v.Size(), 29);
        UNIT_ASSERT_EQUAL(v.Words(), 1);
    }

    Y_UNIT_TEST(TestManyWords) {
        static const int BITS = 10;
        TBitVector<ui64> v;

        for (int i = 0, end = (1 << BITS); i < end; ++i)
            v.Append(i, BITS);

        UNIT_ASSERT_EQUAL(v.Size(), BITS * (1 << BITS));
        UNIT_ASSERT_EQUAL(v.Words(), (v.Size() + 63) / 64);
        for (int i = 0, end = (1 << BITS); i < end; ++i)
            UNIT_ASSERT_EQUAL(v.Get(i * BITS, BITS), (ui64)i);
    }

    Y_UNIT_TEST(TestMaxWordSize) {
        TBitVector<ui32> v;
        for (int i = 0; i < 100; ++i)
            v.Append(i, 32);

        for (int i = 0; i < 100; ++i)
            UNIT_ASSERT_EQUAL(v.Get(i * 32, 32), (ui32)i);

        v.Set(10 * 32, 100500, 32);
        UNIT_ASSERT_EQUAL(v.Get(10 * 32, 32), 100500);
    }

    Y_UNIT_TEST(TestReadonlyVector) {
        TBitVector<ui64> v(100);
        for (ui64 i = 0; i < v.Size(); ++i) {
            if (i % 3 == 0) {
                v.Set(i);
            }
        }
        TBufferStream bs;
        TReadonlyBitVector<ui64>::SaveForReadonlyAccess(&bs, v);
        const auto blob = TBlob::FromBuffer(bs.Buffer());
        TReadonlyBitVector<ui64> rv;
        rv.LoadFromBlob(blob);
        for (ui64 i = 0; i < rv.Size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rv.Test(i), i % 3 == 0);
        }
    }
}
