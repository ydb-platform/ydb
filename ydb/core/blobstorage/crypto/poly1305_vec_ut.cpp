#include "poly1305_vec.h"
#include <ydb/core/blobstorage/crypto/ut/ut_helpers.h>
#include <ydb/core/blobstorage/crypto/ut/poly1305_test_vectors.h>


Y_UNIT_TEST_SUITE(TPoly1305Vec)
{
    inline void RunTest(const ui8* input, size_t input_size,
                   const ui8 key[32], const ui8 expectedMac[16])
    {
        ui8 mac[16];

        Poly1305Vec poly;
        poly.SetKey(key, 32);
        poly.Update(input, input_size);
        poly.Finish(mac);

        UNIT_ASSERT_ARRAYS_EQUAL(mac, expectedMac, sizeof(mac));

        // test that at the second time we get the same result

        poly.SetKey(key, 32);
        poly.Update(input, input_size);
        poly.Finish(mac);

        UNIT_ASSERT_ARRAYS_EQUAL(mac, expectedMac, sizeof(mac));
    }

    Y_UNIT_TEST(TestVector1) {
        RunTest(tc1_data, sizeof(tc1_data), tc1_key, tc1_tag);
    }

    Y_UNIT_TEST(TestVector2) {
        RunTest(tc2_data, sizeof(tc2_data) - 1, tc2_key, tc2_tag);
    }

    Y_UNIT_TEST(TestVector3) {
        RunTest(tc3_data, sizeof(tc3_data) - 1, tc3_key, tc3_tag);
    }

    Y_UNIT_TEST(TestVector4) {
        RunTest(tc4_data, sizeof(tc4_data) - 1, tc4_key, tc4_tag);
    }
}
