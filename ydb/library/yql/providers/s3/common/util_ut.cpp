#include "util.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NYql::NS3Util {

Y_UNIT_TEST_SUITE(TestS3UrlEscape) {
    // Tests on force UrlEscape copied from library/cpp/string_utils/quote/quote_ut.cpp
    Y_UNIT_TEST(EscapeEscapedForce) {
        TString s;

        s = "hello%3dworld";
        UNIT_ASSERT_VALUES_EQUAL(NS3Util::UrlEscapeRet(s), "hello%253dworld");
        NS3Util::UrlEscape(s);
        UNIT_ASSERT_VALUES_EQUAL(s, "hello%253dworld");
    }

    Y_UNIT_TEST(EscapeUnescapeForce) {
        TString s;

        s = "hello%3dworld";
        NS3Util::UrlEscape(s);
        UrlUnescape(s);
        UNIT_ASSERT_VALUES_EQUAL(s, "hello%3dworld");
    }

    Y_UNIT_TEST(EscapeUnescapeForceRet) {
        TString s;

        s = "hello%3dworld";
        UNIT_ASSERT_VALUES_EQUAL(UrlUnescapeRet(NS3Util::UrlEscapeRet(s)), "hello%3dworld");
    }

    // Test additional symbols escape
    Y_UNIT_TEST(EscapeAdditionalSymbols) {
        TString s = "hello#?world";

        UNIT_ASSERT_VALUES_EQUAL(NS3Util::UrlEscapeRet(s), "hello%23%3Fworld");
        NS3Util::UrlEscape(s);
        UNIT_ASSERT_VALUES_EQUAL(s, "hello%23%3Fworld");
    }
}

}  // namespace NYql::NS3Util
