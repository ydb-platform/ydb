#include <ydb/core/persqueue/common/common_app.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NKikimr::NPQ::NApp {

Y_UNIT_TEST_SUITE(TCommonAppTest) {

Y_UNIT_TEST(RendersFullPageWithTwoTabs) {
    TStringStream out;
    HTML_APP_PAGE(out, "page-title") {
        NAVIGATION_BAR() {
            NAVIGATION_TAB("first", "First");
            NAVIGATION_TAB("second", "Second");
            NAVIGATION_TAB_CONTENT("first") {
                PROPERTIES("props") {
                    PROPERTY("name", "value");
                }
                CONFIGURATION("cfg-body");
                LAYOUT() {
                    LAYOUT_ROW() {
                        LAYOUT_COLUMN() {
                        }
                    }
                }
            }
            NAVIGATION_TAB_CONTENT("second") {
                NAVIGATION_TAB_CONTENT_PART("part") {
                }
            }
        }
    }

    const TString html = out.Str();
    UNIT_ASSERT(html.Contains("nav-tabs"));
    UNIT_ASSERT(html.Contains("First"));
    UNIT_ASSERT(html.Contains("Second"));
    UNIT_ASSERT(html.Contains("tab-content"));
    UNIT_ASSERT(html.Contains("id=\"first\""));
    UNIT_ASSERT(html.Contains("id=\"second\""));
    UNIT_ASSERT(html.Contains("id=\"part\""));
    UNIT_ASSERT(html.Contains("properties"));
    UNIT_ASSERT(html.Contains("name"));
    UNIT_ASSERT(html.Contains("value"));
    UNIT_ASSERT(html.Contains("Configuration"));
    UNIT_ASSERT(html.Contains("cfg-body"));
    UNIT_ASSERT(html.Contains("class=\"row\""));
    UNIT_ASSERT(html.Contains("class=\"col\""));
}

Y_UNIT_TEST(NavigationBarWithoutContentClosesList) {
    TStringStream out;
    HTML_APP_PAGE(out, "empty") {
        NAVIGATION_BAR() {
            NAVIGATION_TAB("only", "Only");
        }
    }
    UNIT_ASSERT(out.Str().Contains("</UL>"));
    UNIT_ASSERT(!out.Str().Contains("tab-content"));
}

Y_UNIT_TEST(HtmlPartStreamConversion) {
    TStringStream out;
    {
        THtmlPart part(out);
        IOutputStream& stream = part;
        stream << "hello";
    }
    UNIT_ASSERT_VALUES_EQUAL(out.Str(), "hello");
}

} // Y_UNIT_TEST_SUITE(TCommonAppTest)

} // namespace NKikimr::NPQ::NApp
