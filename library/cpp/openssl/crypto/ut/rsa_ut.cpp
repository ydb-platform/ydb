#include "rsa.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/openssl/big_integer/big_integer.h>

#include <util/system/byteorder.h>

using namespace NOpenSsl;
using namespace NOpenSsl::NRsa;

Y_UNIT_TEST_SUITE(Rsa) {
    Y_UNIT_TEST(Encrypt) {
        // example from Ru.Wikipedia
        const auto originData = TBigInteger::FromULong(111111);

        const auto n = TBigInteger::FromULong(3);
        const auto e = TBigInteger::FromULong(9173503);

        // check key reuse
        for (size_t i = 0; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(TBigInteger::FromULong(4051753), TPublicKey(n, e).EncryptNoPad(originData));
        }

        UNIT_ASSERT_VALUES_EQUAL(originData, TBigInteger::FromULong(111111));
        UNIT_ASSERT_VALUES_EQUAL(n, TBigInteger::FromULong(3));
        UNIT_ASSERT_VALUES_EQUAL(e, TBigInteger::FromULong(9173503));
    }
}
