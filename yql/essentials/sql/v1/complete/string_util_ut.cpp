#include "string_util.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(StringUtilTest) {
    Y_UNIT_TEST(Blank) {
        UNIT_ASSERT_VALUES_EQUAL(LastWord(""), "");
        UNIT_ASSERT_VALUES_EQUAL(LastWord(" "), "");
        UNIT_ASSERT_VALUES_EQUAL(LastWord("  "), "");
        UNIT_ASSERT_VALUES_EQUAL(LastWord("   "), "");
    }

    Y_UNIT_TEST(Space) {
        UNIT_ASSERT_VALUES_EQUAL(LastWord("two "), "");
        UNIT_ASSERT_VALUES_EQUAL(LastWord("one two "), "");
        UNIT_ASSERT_VALUES_EQUAL(LastWord("two"), "two");
        UNIT_ASSERT_VALUES_EQUAL(LastWord("one two"), "two");
    }
} // Y_UNIT_TEST_SUITE(StringUtilTest)
