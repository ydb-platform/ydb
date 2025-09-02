#include "url_builder.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TUrlBuilder) {
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

        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc?param1=%3D!@%23$%25%5E%26*%28%29%7B%7D%5B%5D%22+&param2=val2");
    }

    Y_UNIT_TEST(EmptyPathComponent) {
        TUrlBuilder builder("https://localhost/abc");
        UNIT_ASSERT_EXCEPTION_CONTAINS(builder.AddPathComponent(""), std::exception, "Empty path component is not allowed");
        auto url = builder.Build();
        // not changed
        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc");
    }

    Y_UNIT_TEST(SeveralPathComponents) {
        auto url = TUrlBuilder("https://localhost/abc")
                    .AddPathComponent("oops")
                    .AddPathComponent("long oops")
                    .AddUrlParam("param1", "val1")
                    .AddUrlParam("param1", "long param")
                    .Build();
        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc/oops/long%20oops?param1=val1&param1=long+param");
    }

    Y_UNIT_TEST(SeveralPathComponentsWithSlashInBaseUri) {
        // base uri ends with '/'
        auto url = TUrlBuilder("https://localhost/abc/")
                    .AddPathComponent("oops%1234")
                    .AddPathComponent("long&oops=xxx")
                    .AddUrlParam("param1", "a&b=cdef")
                    .Build();
        UNIT_ASSERT_VALUES_EQUAL(url, "https://localhost/abc/oops%251234/long&oops=xxx?param1=a%26b%3Dcdef");
    }
}
