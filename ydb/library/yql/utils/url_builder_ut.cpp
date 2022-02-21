#include "url_builder.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TUrlBuilder) {
    Y_UNIT_TEST(Basic) {
        TUrlBuilder builder("https://localhost/abc?");
        builder.AddUrlParam("param1", "val1");
        builder.AddUrlParam("param2", "val2");

        UNIT_ASSERT_VALUES_EQUAL(builder.Build(), "https://localhost/abc?param1=val1&param2=val2");
    }

    Y_UNIT_TEST(Test1) {
        TUrlBuilder builder("https://localhost/abc?");
        builder.AddUrlParam("param1", "=!@#$%^&*(){}[]\" ");
        builder.AddUrlParam("param2", "val2");

        UNIT_ASSERT_VALUES_EQUAL(builder.Build(), "https://localhost/abc?param1=%3D!@%23$%25^%26*%28%29%7B%7D%5B%5D%22+&param2=val2");
    }
}
