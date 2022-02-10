#include "zigzag.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TZigZagTest) {
    template <typename T>
    void TestEncodeDecode(T value) {
        auto encoded = ZigZagEncode(value);
        UNIT_ASSERT(encoded >= 0);
        auto decoded = ZigZagDecode(encoded);
        UNIT_ASSERT(decoded == value);
        static_assert(sizeof(value) == sizeof(decoded), "sizeof mismatch");
        static_assert(sizeof(value) == sizeof(encoded), "sizeof mismatch");
    }

    template <typename TSignedInt>
    void TestImpl() {
        static const int bits = CHAR_BIT * sizeof(TSignedInt);
        for (int p = 0; p + 1 < bits; ++p) {
            TSignedInt value = 1;
            value <<= p;

            TestEncodeDecode(value);
            TestEncodeDecode(-value);
            TestEncodeDecode(value - 1);
            TestEncodeDecode(-value + 1);
        }

        TestEncodeDecode((TSignedInt)123);
        TestEncodeDecode((TSignedInt)-123);
    }

    Y_UNIT_TEST(TestSigned) {
        TestImpl<i16>();
        TestImpl<i32>();
        TestImpl<i64>();
    }
}
