#include <ydb/library/testlib/helpers.h>
#include <library/cpp/testing/unittest/registar.h>

// Define test body as a macro that takes the flag name
// This macro will be expanded twice - once for each variant of the suite
#define TWIN_SUITE_TESTS(FlagName) \
    Y_UNIT_TEST(TestWithFlagCheck) { \
        /* Tests can branch based on the flag value */ \
        if (TCurrentTest::FlagName) { \
            UNIT_ASSERT(TCurrentTest::FlagName == true); \
        } else { \
            UNIT_ASSERT(TCurrentTest::FlagName == false); \
        } \
    } \
    Y_UNIT_TEST(TestFlagValue) { \
        /* Tests can use the flag directly */ \
        bool flagValue = TCurrentTest::FlagName; \
        UNIT_ASSERT_VALUES_EQUAL(flagValue, TCurrentTest::FlagName); \
    } \
    Y_UNIT_TEST(TestConstexprFlag) { \
        /* The flag is constexpr, so it can be used in compile-time expressions */ \
        static_assert(TCurrentTest::FlagName == true || TCurrentTest::FlagName == false, \
                      "Flag must be boolean"); \
        UNIT_ASSERT(true); \
    }

// This creates two test suites:
//   - TTestSuiteTwin+UseFeature (with UseFeature=true)
//   - TTestSuiteTwin-UseFeature (with UseFeature=false)
Y_UNIT_TEST_SUITE_TWIN(TTestSuiteTwin, UseFeature, TWIN_SUITE_TESTS)
