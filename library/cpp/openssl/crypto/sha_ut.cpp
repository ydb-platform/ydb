#include <library/cpp/testing/unittest/registar.h>

#include "sha.h"

constexpr TStringBuf SomeAlignedShaTestData = "some _aligned_ test data for SHA-family: align align align align";

Y_UNIT_TEST_SUITE(SHA){
    Y_UNIT_TEST(CheckOfTestDataAlignment){
        UNIT_ASSERT_VALUES_EQUAL(SomeAlignedShaTestData.size() % sizeof(ui32), 0);
    }

    Y_UNIT_TEST(Sha1Value) {
        // bash$ echo -n $SomeAlignedShaTestData | sha1sum
        const TStringBuf precalculatedDigest =
            "\xA2\x29\x8E\xE2\xEA\x06\x27\x45"
            "\x27\xC7\x78\x87\x16\x21\x8A\xA5"
            "\x0D\xBA\xBA\xB2"sv;

        auto digest = NOpenSsl::NSha1::Calc(SomeAlignedShaTestData.data(), SomeAlignedShaTestData.size());

        UNIT_ASSERT_VALUES_EQUAL(precalculatedDigest.size(), digest.size());
        UNIT_ASSERT_VALUES_EQUAL(memcmp(precalculatedDigest.data(), digest.data(), digest.size()), 0);
    }

    Y_UNIT_TEST(Sha256Value) {
        // bash$ echo -n $SomeAlignedShaTestData | sha256sum
        const TStringBuf precalculatedDigest =
            "\xED\x64\x0D\x43\xF7\x6D\x71\x98"
            "\x39\x19\xF6\xE6\x70\x21\x82\x11"
            "\xEF\x3B\xF0\xF4\x35\xBF\x42\xAB"
            "\x1C\x5C\x01\xCD\x20\x33\xD2\xFA"sv;

        auto digest = NOpenSsl::NSha256::Calc(SomeAlignedShaTestData.data(), SomeAlignedShaTestData.size());

        UNIT_ASSERT_VALUES_EQUAL(precalculatedDigest.size(), digest.size());
        UNIT_ASSERT_VALUES_EQUAL(memcmp(precalculatedDigest.data(), digest.data(), digest.size()), 0);
    }

    Y_UNIT_TEST(Sha224Value) {
        // bash$ echo -n $SomeAlignedShaTestData | sha224sum
        const TStringBuf precalculatedDigest =
            "\xD4\x8B\x12\xA8\x0B\x29\x01\x92"
            "\xC1\xF9\x2A\x71\x17\x99\x9C\x83"
            "\xDB\xC5\xBB\x7B\xBE\xC1\xF1\xD9"
            "\x97\x75\x38\xCB"sv;

        auto digest = NOpenSsl::NSha224::Calc(SomeAlignedShaTestData.data(), SomeAlignedShaTestData.size());

        UNIT_ASSERT_VALUES_EQUAL(precalculatedDigest.size(), digest.size());
        UNIT_ASSERT_VALUES_EQUAL(memcmp(precalculatedDigest.data(), digest.data(), digest.size()), 0);
    }

    Y_UNIT_TEST(FragmentedEqualNotFragmented) {
        const char* head = SomeAlignedShaTestData.data();
        const char* current = head;
        NOpenSsl::NSha1::TCalcer sha;
        int intValue;
        std::copy_n(current, sizeof(intValue), (char*)&intValue);
        current += sizeof(intValue);
        sha.UpdateWithPodValue(intValue);
        double doubleValue;
        std::copy_n(current, sizeof(doubleValue), (char*)&doubleValue);
        current += sizeof(doubleValue);
        sha.UpdateWithPodValue(doubleValue);
        char str[7];
        std::copy_n(current, std::size(str), str);
        current += std::size(str);
        sha.UpdateWithPodValue(str);
        sha.Update(current, SomeAlignedShaTestData.size() - (current - head));
        auto fragmentedDigest = sha.Final();

        auto notFragmentedDigest = NOpenSsl::NSha1::Calc(SomeAlignedShaTestData.data(), SomeAlignedShaTestData.size());

        UNIT_ASSERT_VALUES_EQUAL(memcmp(fragmentedDigest.data(), notFragmentedDigest.data(), notFragmentedDigest.size()), 0);
    }
} // UNITTEST_SIMPLE_SUITE(SHA)
