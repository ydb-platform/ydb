#include <library/cpp/testing/unittest/registar.h>
#include <util/system/env.h>

Y_UNIT_TEST_SUITE(HttpProxyInsideYdb) {

Y_UNIT_TEST(TestIfEnvVariableSet) {
    UNIT_ASSERT(!GetEnv("INSIDE_YDB").empty());
}

} // Y_UNIT_TEST_SUITE(TestHttpProxy)
