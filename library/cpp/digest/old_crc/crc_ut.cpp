#include "crc.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>

class TCrcTest: public TTestBase {
    UNIT_TEST_SUITE(TCrcTest);
    UNIT_TEST(TestCrc16)
    UNIT_TEST(TestCrc32)
    UNIT_TEST(TestCrc64)
    UNIT_TEST_SUITE_END();

private:
    inline void TestCrc16() {
        const ui16 expected[] = {
            41719,
            57001,
            53722,
            3276};

        Run<ui16>(expected);
    }

    inline void TestCrc32() {
        const ui32 expected[] = {
            688229491UL,
            3543112608UL,
            4127534015UL,
            3009909774UL};

        Run<ui32>(expected);
    }

    inline void TestCrc64() {
        const ui64 expected[] = {
            12116107829328640258ULL,
            18186277744016380552ULL,
            249923753044811734ULL,
            7852471725963920356ULL};

        Run<ui64>(expected);
    }

private:
    template <class T>
    inline void Run(const T* expected) {
        ui8 buf[256];

        for (size_t i = 0; i < 256; ++i) {
            buf[i] = i;
        }

        Test<T>(buf, 256, expected[0]);
        Test<T>(buf, 255, expected[1]);
        Test<T>(buf, 254, expected[2]);
        Test<T>(buf, 253, expected[3]);
    }

    template <class T>
    inline void Test(const void* data, size_t len, T expected) {
        const T res = Crc<T>(data, len);

        try {
            UNIT_ASSERT_EQUAL(res, expected);
        } catch (...) {
            Cerr << res << Endl;

            throw;
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TCrcTest);
