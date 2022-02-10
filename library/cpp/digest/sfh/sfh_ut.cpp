#include "sfh.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>

class TSfhTest: public TTestBase {
    UNIT_TEST_SUITE(TSfhTest);
    UNIT_TEST(TestSfh)
    UNIT_TEST_SUITE_END();

private:
    inline void TestSfh() {
        ui8 buf[256];

        for (size_t i = 0; i < 256; ++i) {
            buf[i] = i;
        }

        Test(buf, 256, 3840866583UL);
        Test(buf, 255, 325350515UL);
        Test(buf, 254, 2920741773UL);
        Test(buf, 253, 3586628615UL);
    }

private:
    inline void Test(const void* data, size_t len, ui32 expected) {
        const ui32 res = SuperFastHash(data, len);

        try {
            UNIT_ASSERT_EQUAL(res, expected);
        } catch (...) {
            Cerr << res << ", " << expected << Endl;

            throw;
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TSfhTest);
