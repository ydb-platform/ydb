#include "xml-options.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TestXmlOptions) {
    Y_UNIT_TEST(SetHuge) {
        NXml::TOptions opts;
        opts.Set(NXml::EOption::Huge);
        UNIT_ASSERT_EQUAL(XML_PARSE_HUGE, opts.GetMask());
    }

    Y_UNIT_TEST(VariadicContructor) {
        NXml::TOptions opts(NXml::EOption::Huge, NXml::EOption::Compact, NXml::EOption::SAX1);
        UNIT_ASSERT_EQUAL(XML_PARSE_HUGE | XML_PARSE_COMPACT | XML_PARSE_SAX1, opts.GetMask());
    }

    Y_UNIT_TEST(Chaining) {
        NXml::TOptions opts;

        opts
            .Set(NXml::EOption::Huge)
            .Set(NXml::EOption::Compact);

        UNIT_ASSERT_EQUAL(XML_PARSE_HUGE | XML_PARSE_COMPACT, opts.GetMask());
    }
}
