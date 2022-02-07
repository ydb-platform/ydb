#include <library/cpp/protobuf/json/util.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NProtobufJson;

Y_UNIT_TEST_SUITE(TEqualsTest) {
    Y_UNIT_TEST(TestEmpty) {
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("", ""));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("", "_"));
        UNIT_ASSERT(!EqualsIgnoringCaseAndUnderscores("f", ""));
    }

    Y_UNIT_TEST(TestTrivial) {
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("f", "f"));
        UNIT_ASSERT(!EqualsIgnoringCaseAndUnderscores("f", "o"));
        UNIT_ASSERT(!EqualsIgnoringCaseAndUnderscores("fo", "f"));
        UNIT_ASSERT(!EqualsIgnoringCaseAndUnderscores("f", "fo"));
        UNIT_ASSERT(!EqualsIgnoringCaseAndUnderscores("bar", "baz"));
    }

    Y_UNIT_TEST(TestUnderscores) {
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("foo_bar", "foobar"));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("foo_bar_", "foobar"));
        UNIT_ASSERT(!EqualsIgnoringCaseAndUnderscores("foo_bar_z", "foobar"));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("foo__bar__", "foobar"));
        UNIT_ASSERT(!EqualsIgnoringCaseAndUnderscores("foo__bar__z", "foobar"));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("_foo_bar", "foobar"));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("_foo_bar_", "foobar"));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("_foo_bar_", "foo___bar"));
    }

    Y_UNIT_TEST(TestCase) {
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("foo_bar", "FOO_BAR"));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("foobar", "fooBar"));
    }

    Y_UNIT_TEST(TestCaseAndUnderscores) {
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("fooBar", "FOO_BAR"));
        UNIT_ASSERT(EqualsIgnoringCaseAndUnderscores("FOO_BAR_BAZ", "fooBar_BAZ"));
    }
}
