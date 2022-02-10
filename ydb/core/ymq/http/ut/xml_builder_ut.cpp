#include <ydb/core/ymq/http/xml_builder.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(XmlBuilderTest) {
    Y_UNIT_TEST(WritesProperly) {
        TXmlStringBuilder builder;

        {
            TXmlDocument doc(builder);
            {
                TXmlRecursiveElement elem1(builder, "upper_elem");
                TXmlElement elem2(builder, "elem", "content");
            }
        }

        UNIT_ASSERT_STRINGS_EQUAL(builder.GetResult(), R"&&&(<?xml version="1.0" encoding="UTF-8"?>)&&&" "\n" R"&&&(<upper_elem><elem>content</elem></upper_elem>)&&&" "\n");
    }

    Y_UNIT_TEST(MacroBuilder) {
        XML_BUILDER() {
            XML_DOC() {
                XML_ELEM("El1") {
                    XML_ELEM("El2") {
                        XML_ELEM_CONT("ElC1", "content1");
                    }
                    XML_ELEM("El3") {
                        XML_ELEM_CONT("ElC2", "content2");
                    }
                }
            }
        }

        UNIT_ASSERT_STRINGS_EQUAL(XML_RESULT(), R"&&&(<?xml version="1.0" encoding="UTF-8"?>)&&&" "\n" R"&&&(<El1><El2><ElC1>content1</ElC1></El2><El3><ElC2>content2</ElC2></El3></El1>)&&&" "\n");
    }
}
