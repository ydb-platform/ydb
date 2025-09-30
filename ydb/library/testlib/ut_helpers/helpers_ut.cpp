#include <ydb/library/testlib/helpers.h>
#include <library/cpp/testing/unittest/registar.h>

// Define test body as a macro that takes the flag name
#define TWIN_SUITE_TESTS(OPT_NAME) \
    Y_UNIT_TEST(TestWithFlag) { \
        if (TCurrentTest::OPT_NAME) { \
            UNIT_ASSERT(true); \
        } else { \
            UNIT_ASSERT(true); \
        } \
    } \
    Y_UNIT_TEST(AnotherTest) { \
        UNIT_ASSERT_VALUES_EQUAL(TCurrentTest::OPT_NAME, TCurrentTest::OPT_NAME); \
    }

Y_UNIT_TEST_SUITE_TWIN(TTestSuiteTwin, UseFeature, TWIN_SUITE_TESTS)
