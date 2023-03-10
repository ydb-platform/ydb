#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

namespace {
    TEST(TestForTest, AssertTrue) {
        UNIT_ASSERT(true);
    }

    TEST(TestForTest, AssertTStringEqual) {
        TString s1 = "q";
        TString s2 = "q";
        UNIT_ASSERT_EQUAL(s1, s2);
        UNIT_ASSERT_STRINGS_EQUAL(s1, s2);
        UNIT_ASSERT_VALUES_EQUAL(s1, s2);
    }

    TEST(TestForTest, AssertCCharNotEqualLocation) {
        const char* s1 = "1q";
        const char* s2 = "2q";
        UNIT_ASSERT_UNEQUAL(s1, s2);
        UNIT_ASSERT_STRINGS_UNEQUAL(s1, s2);
        UNIT_ASSERT_VALUES_UNEQUAL(s1, s2);
        s1++;
        s2++;
        // different address location
        UNIT_ASSERT_UNEQUAL(s1, s2);
        // but same value
        UNIT_ASSERT_STRINGS_EQUAL(s1, s2);
        UNIT_ASSERT_VALUES_EQUAL(s1, s2);
    }

    TEST(TestForTest, AssertCCharEqualLocation) {
        static const char* buf = "str";
        const char* s1 = buf;
        const char* s2 = buf;
        // same address location
        UNIT_ASSERT_EQUAL(s1, s2);
        // same value
        UNIT_ASSERT_STRINGS_EQUAL(s1, s2);
        UNIT_ASSERT_VALUES_EQUAL(s1, s2);
    }
}

