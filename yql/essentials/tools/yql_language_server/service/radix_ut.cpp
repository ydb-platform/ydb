#include "radix.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/random/fast.h>
#include <util/string/builder.h>

namespace NLsp::NYql {

Y_UNIT_TEST_SUITE(TRadixTests) {

Y_UNIT_TEST(ValidatesAlphabet) {
    UNIT_ASSERT_EXCEPTION(TRadix("0"), yexception);
    UNIT_ASSERT_EXCEPTION(TRadix("001"), yexception);
    UNIT_ASSERT_EXCEPTION(TRadix("10"), yexception);
}

Y_UNIT_TEST(EncodesBinaryValues) {
    const TRadix binary("01");

    UNIT_ASSERT_VALUES_EQUAL(binary.Encode(0), "0");
    UNIT_ASSERT_VALUES_EQUAL(binary.Encode(2), "10");
    UNIT_ASSERT_VALUES_EQUAL(binary.Encode(7), "111");
}

Y_UNIT_TEST(EncodesHexadecimalRollovers) {
    const TRadix hexadecimal("0123456789ABCDEF");

    UNIT_ASSERT_VALUES_EQUAL(hexadecimal.Encode(15), "F");
    UNIT_ASSERT_VALUES_EQUAL(hexadecimal.Encode(16), "10");
    UNIT_ASSERT_VALUES_EQUAL(hexadecimal.Encode(255), "FF");
    UNIT_ASSERT_VALUES_EQUAL(hexadecimal.Encode(256), "100");
}

Y_UNIT_TEST(PadsAndRejectsInsufficientLength) {
    const TRadix binary("01");

    UNIT_ASSERT_VALUES_EQUAL(binary.Encode(5, 5), "00101");
    UNIT_ASSERT_EXCEPTION(binary.Encode(8, 2), yexception);
}

Y_UNIT_TEST(PropertyBased) {
    constexpr ui64 Seed = 1;
    constexpr size_t Iterations = 10'000;
    constexpr ui64 MaxN = 10'000;
    constexpr ui64 MinLen = 3;
    constexpr ui64 MaxLen = 8;

    const TRadix radix(TRadix::SimpleAlphabet());
    TReallyFastRng32 random(Seed);

    for (size_t i = 0; i < Iterations; ++i) {
        const size_t lhs = random.Uniform(0ULL, MaxN);
        const size_t rhs = random.Uniform(0ULL, MaxN);
        const size_t len = random.Uniform(MinLen, MaxLen);

        const TString lhsE = radix.Encode(lhs, len);
        const TString rhsE = radix.Encode(rhs, len);

        UNIT_ASSERT_C(
            (lhs < rhs) == (lhsE < rhsE),
            TStringBuilder() << "lhs: " << lhs << " (" << lhsE << "), "
                             << "rhs: " << rhs << " (" << rhsE << "), len: " << len);
    }
}

} // Y_UNIT_TEST_SUITE(TRadixTests)

} // namespace NLsp::NYql
