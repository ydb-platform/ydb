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
    }
}

Y_UNIT_TEST_SUITE(TestUrlBuilder) {
    Y_UNIT_TEST(UriOnly) {
        TUrlBuilder builder("https://localhost/abc");
        UNIT_ASSERT_VALUES_EQUAL(builder.Build(), "https://localhost/abc");
    }

    Y_UNIT_TEST(Basic) {
        TUrlBuilder builder("https://localhost/abc");
        builder.AddUrlParam("param1", "val1");
        builder.AddUrlParam("param2", "val2");

        UNIT_ASSERT_VALUES_EQUAL(builder.Build(), "https://localhost/abc?param1=val1&param2=val2");
    }

    Y_UNIT_TEST(BasicWithEncoding) {
        auto url = TUrlBuilder("https://localhost/abc")
                        .AddUrlParam("param1", "=!@#$%^&*(){}[]\" ")
                        .AddUrlParam("param2", "val2")
                        .Build();

        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc?param1=%3D%21%40%23%24%25%5E%26%2A%28%29%7B%7D%5B%5D%22+&param2=val2");
    }

    Y_UNIT_TEST(BasicWithAdditionalEncoding) {
        auto url = TUrlBuilder("https://localhost/abc")
                        .AddUrlParam("param1", ":/?#[]@!$&\'()*+,;=")
                        .AddUrlParam("param2", "val2")
                        .Build();

        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc?param1=%3A%2F%3F%23%5B%5D%40%21%24%26%27%28%29%2A%2B%2C%3B%3D&param2=val2");
    }
}

}  // namespace NYql::NS3Util
