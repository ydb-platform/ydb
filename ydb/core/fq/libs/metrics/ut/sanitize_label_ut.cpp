#include <ydb/core/fq/libs/metrics/sanitize_label.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(SanitizeLable) {
    Y_UNIT_TEST(Empty) {
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel(""), "");
    }

    Y_UNIT_TEST(SkipSingleBadSymbol) {
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("|"), "");
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("*"), "");
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("?"), "");
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("\""), "");
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("'"), "");
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("`"), "");
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("\\"), "");
    }

    Y_UNIT_TEST(SkipBadSymbols) {
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel("a|b*c?d\"e'f`g\\h"), "abcdefgh");
    }

    Y_UNIT_TEST(Truncate200) {
        TString s1(400, 'a');
        TString s2(200, 'a');
        UNIT_ASSERT_VALUES_EQUAL(NFq::SanitizeLabel(s1), s2);
    }
}
