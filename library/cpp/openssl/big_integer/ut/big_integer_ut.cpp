#include "big_integer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/byteorder.h>
#include <util/stream/str.h>

Y_UNIT_TEST_SUITE(BigInteger) {
    using NOpenSsl::TBigInteger;

    Y_UNIT_TEST(Initialization) {
        constexpr ui64 testVal = 12345678900;
        const auto fromULong = TBigInteger::FromULong(testVal);

        const ui64 testArea = HostToInet(testVal); // transform to big-endian
        const auto fromRegion = TBigInteger::FromRegion(&testArea, sizeof(testArea));
        UNIT_ASSERT(fromULong == fromRegion);
        UNIT_ASSERT_VALUES_EQUAL(fromULong, fromRegion);

        const auto fromULongOther = TBigInteger::FromULong(22345678900);
        UNIT_ASSERT(fromULong != fromULongOther);
    }

    Y_UNIT_TEST(Decimal) {
        UNIT_ASSERT_VALUES_EQUAL(TBigInteger::FromULong(123456789).ToDecimalString(), "123456789");
    }

    Y_UNIT_TEST(Region) {
        const auto v1 = TBigInteger::FromULong(1234567890);
        char buf[1024];
        const auto v2 = TBigInteger::FromRegion(buf, v1.ToRegion(buf));

        UNIT_ASSERT_VALUES_EQUAL(v1, v2);
    }

    Y_UNIT_TEST(Output) {
        TStringStream ss;

        ss << TBigInteger::FromULong(123456789);

        UNIT_ASSERT_VALUES_EQUAL(ss.Str(), "123456789");
    }
}
