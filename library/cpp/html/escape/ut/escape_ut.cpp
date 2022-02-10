#include <library/cpp/html/escape/escape.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NHtml;

Y_UNIT_TEST_SUITE(TEscapeHtml) {
    Y_UNIT_TEST(Escape) {
        UNIT_ASSERT_EQUAL(EscapeText("in & out"), "in &amp; out");
        UNIT_ASSERT_EQUAL(EscapeText("&&"), "&amp;&amp;");
        UNIT_ASSERT_EQUAL(EscapeText("&amp;"), "&amp;amp;");

        UNIT_ASSERT_EQUAL(EscapeText("<script>"), "&lt;script&gt;");

        UNIT_ASSERT_EQUAL(EscapeText("text"), "text");
    }
}
