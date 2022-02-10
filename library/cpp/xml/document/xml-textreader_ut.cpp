#include "xml-textreader.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/join.h>

namespace {
    /**
     * Simple wrapper around the xmlTextReader wrapper
     */
    void ParseXml(const TString& xmlData,
                  std::function<void(NXml::TConstNode)> nodeHandlerFunc,
                  const TString& localName,
                  const TString& namespaceUri = TString()) {
        TStringInput in(xmlData);
        NXml::TTextReader reader(in);

        while (reader.Read()) {
            if (reader.GetNodeType() == NXml::TTextReader::ENodeType::Element &&
                reader.GetLocalName() == localName &&
                reader.GetNamespaceUri() == namespaceUri)
            {
                const NXml::TConstNode node = reader.Expand();
                nodeHandlerFunc(node);
            }
        }
    }
}

Y_UNIT_TEST_SUITE(TestXmlTextReader) {
    Y_UNIT_TEST(BasicExample) {
        const TString xml = "<?xml version=\"1.0\"?>\n"
                            "<example toto=\"1\">\n"
                            "  <examplechild id=\"1\">\n"
                            "    <child_of_child/>\n"
                            "  </examplechild>\n"
                            "  <examplechild id=\"2\" toto=\"3\">\n"
                            "    <child_of_child>Some content : -)</child_of_child>\n"
                            "  </examplechild>\n"
                            "</example>\n";

        TStringInput input(xml);
        NXml::TTextReader reader(input);

        using ENT = NXml::TTextReader::ENodeType;

        struct TItem {
            int Depth;
            ENT Type;
            TString Name;
            TString Attrs;
            TString Value;
        };

        TVector<TItem> found;
        TVector<TString> msgs;

        while (reader.Read()) {
            // dump attributes as "k1: v1, k2: v2, ..."
            TVector<TString> kv;
            if (reader.HasAttributes()) {
                reader.MoveToFirstAttribute();
                do {
                    kv.push_back(TString::Join(reader.GetName(), ": ", reader.GetValue()));
                } while (reader.MoveToNextAttribute());
                reader.MoveToElement();
            }

            found.push_back(TItem{
                reader.GetDepth(),
                reader.GetNodeType(),
                TString(reader.GetName()),
                JoinSeq(", ", kv),
                reader.HasValue() ? TString(reader.GetValue()) : TString(),
            });
        }

        const TVector<TItem> expected = {
            TItem{0, ENT::Element, "example", "toto: 1", ""},
            TItem{1, ENT::SignificantWhitespace, "#text", "", "\n  "},
            TItem{1, ENT::Element, "examplechild", "id: 1", ""},
            TItem{2, ENT::SignificantWhitespace, "#text", "", "\n    "},
            TItem{2, ENT::Element, "child_of_child", "", ""},
            TItem{2, ENT::SignificantWhitespace, "#text", "", "\n  "},
            TItem{1, ENT::EndElement, "examplechild", "id: 1", ""},
            TItem{1, ENT::SignificantWhitespace, "#text", "", "\n  "},
            TItem{1, ENT::Element, "examplechild", "id: 2, toto: 3", ""},
            TItem{2, ENT::SignificantWhitespace, "#text", "", "\n    "},
            TItem{2, ENT::Element, "child_of_child", "", ""},
            TItem{3, ENT::Text, "#text", "", "Some content : -)"},
            TItem{2, ENT::EndElement, "child_of_child", "", ""},
            TItem{2, ENT::SignificantWhitespace, "#text", "", "\n  "},
            TItem{1, ENT::EndElement, "examplechild", "id: 2, toto: 3", ""},
            TItem{1, ENT::SignificantWhitespace, "#text", "", "\n"},
            TItem{0, ENT::EndElement, "example", "toto: 1", ""}};

        UNIT_ASSERT_VALUES_EQUAL(found.size(), expected.size());

        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(found[i].Depth, expected[i].Depth, "line " << i);
            UNIT_ASSERT_EQUAL_C(found[i].Type, expected[i].Type, "line " << i);
            UNIT_ASSERT_VALUES_EQUAL_C(found[i].Name, expected[i].Name, "line " << i);
            UNIT_ASSERT_VALUES_EQUAL_C(found[i].Attrs, expected[i].Attrs, "line " << i);
            UNIT_ASSERT_VALUES_EQUAL_C(found[i].Value, expected[i].Value, "line " << i);
        }
    }

    const TString GEODATA = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                            "<root>"
                            ""
                            "  <country id=\"225\">"
                            "    <name>Россия</name>"
                            "    <cities>"
                            "      <city>Москва</city>"
                            "      <city>Санкт-Петербург</city>"
                            "    </cities>"
                            "  </country>"
                            ""
                            "  <country id=\"149\">"
                            "    <name>Беларусь</name>"
                            "    <cities>"
                            "      <city>Минск</city>"
                            "    </cities>"
                            "  </country>"
                            ""
                            "  <country id=\"187\">"
                            "    <name>Украина</name>"
                            "    <cities>"
                            "      <city>Киев</city>"
                            "    </cities>"
                            "  </country>"
                            ""
                            "</root>";

    Y_UNIT_TEST(ParseXmlSimple) {
        struct TCountry {
            TString Name;
            TVector<TString> Cities;
        };

        THashMap<int, TCountry> data;

        auto handler = [&data](NXml::TConstNode node) {
            const int id = node.Attr<int>("id");

            TCountry& c = data[id];

            c.Name = node.FirstChild("name").Value<TString>();

            const NXml::TConstNodes cityNodes = node.Nodes("cities/city");
            for (auto cityNode : cityNodes) {
                c.Cities.push_back(cityNode.Value<TString>());
            }
        };

        ParseXml(GEODATA, handler, "country");

        UNIT_ASSERT_EQUAL(data.size(), 3);

        UNIT_ASSERT(data.contains(225));
        const TCountry& russia = data.at(225);
        UNIT_ASSERT_EQUAL(russia.Name, "Россия");
        UNIT_ASSERT_EQUAL(russia.Cities.size(), 2);
        UNIT_ASSERT_EQUAL(russia.Cities[0], "Москва");
        UNIT_ASSERT_EQUAL(russia.Cities[1], "Санкт-Петербург");

        UNIT_ASSERT(data.contains(149));
        const TCountry& belarus = data.at(149);
        UNIT_ASSERT_EQUAL(belarus.Name, "Беларусь");
        UNIT_ASSERT_EQUAL(belarus.Cities.size(), 1);
        UNIT_ASSERT_EQUAL(belarus.Cities[0], "Минск");

        UNIT_ASSERT(data.contains(187));
        const TCountry& ukraine = data.at(187);
        UNIT_ASSERT_EQUAL(ukraine.Name, "Украина");
        UNIT_ASSERT_EQUAL(ukraine.Cities.size(), 1);
        UNIT_ASSERT_EQUAL(ukraine.Cities[0], "Киев");
    }

    Y_UNIT_TEST(ParseXmlDeepLevel) {
        TVector<TString> cities;

        auto handler = [&cities](NXml::TConstNode node) {
            cities.push_back(node.Value<TString>());
        };

        ParseXml(GEODATA, handler, "city");

        UNIT_ASSERT_EQUAL(cities.size(), 4);
        UNIT_ASSERT_EQUAL(cities[0], "Москва");
        UNIT_ASSERT_EQUAL(cities[1], "Санкт-Петербург");
        UNIT_ASSERT_EQUAL(cities[2], "Минск");
        UNIT_ASSERT_EQUAL(cities[3], "Киев");
    }

    Y_UNIT_TEST(ParseXmlException) {
        // Check that exception properly passes through plain C code of libxml,
        // no leaks are detected by valgrind.
        auto handler = [](NXml::TConstNode node) {
            const int id = node.Attr<int>("id");
            if (id != 225) {
                ythrow yexception() << "unsupported id: " << id;
            }
        };

        UNIT_ASSERT_EXCEPTION(ParseXml(GEODATA, handler, "country"), yexception);
        UNIT_ASSERT_EXCEPTION(ParseXml("<a></b>", handler, "a"), yexception);
        UNIT_ASSERT_EXCEPTION(ParseXml("<root><a id=\"1\"></a><a id=\"2\"></b></root>", handler, "a"), yexception);
        UNIT_ASSERT_EXCEPTION(ParseXml("<root><a id=\"1\"></a><a id=\"2></a></root>", handler, "a"), yexception);
    }

    const TString BACKA = // UTF-8 encoding is used implicitly
        "<Companies"
        "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
        "    xmlns=\"http://maps.yandex.ru/backa/1.x\""
        "    xmlns:atom=\"http://www.w3.org/2005/Atom\""
        "    xmlns:biz=\"http://maps.yandex.ru/business/1.x\""
        "    xmlns:xal=\"urn:oasis:names:tc:ciq:xsdschema:xAL:2.0\""
        "    xmlns:gml=\"http://www.opengis.net/gml\""
        ">"
        ""
        "  <Company id=\"0001\">"
        "    <Geo>"
        "      <Location>"
        "        <gml:pos>37.62669 55.664827</gml:pos>"
        "        <kind>house</kind>"
        "      </Location>"
        "      <AddressDetails xmlns=\"urn:oasis:names:tc:ciq:xsdschema:xAL:2.0\">"
        "        <Country>"
        "          <AddressLine xml:lang=\"ru\">Москва, Каширское ш., 14</AddressLine>"
        "        </Country>"
        "      </AddressDetails>"
        "    </Geo>"
        "  </Company>"
        ""
        "  <Company id=\"0002\">"
        "    <Geo>"
        "      <Location>"
        "        <pos xmlns=\"http://www.opengis.net/gml\">150.819797 59.56092</pos>"
        "        <kind>locality</kind>"
        "      </Location>"
        "      <xal:AddressDetails>"
        "        <xal:Country>"
        "          <xal:AddressLine xml:lang=\"ru\">Магадан, ул. Пролетарская, 43</xal:AddressLine>"
        "        </xal:Country>"
        "      </xal:AddressDetails>"
        "    </Geo>"
        "  </Company>"
        ""
        "</Companies>";

    Y_UNIT_TEST(NamespaceHell) {
        using TNS = NXml::TNamespaceForXPath;
        const NXml::TNamespacesForXPath ns = {
            TNS{"b", "http://maps.yandex.ru/backa/1.x"},
            TNS{"gml", "http://www.opengis.net/gml"},
            TNS{"xal", "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0"}};

        int count = 0;
        THashMap<TString, TString> positions;
        THashMap<TString, TString> addresses;

        auto handler = [&](NXml::TConstNode node) {
            count++;
            const auto id = node.Attr<TString>("id");

            NXml::TXPathContextPtr ctxt = node.CreateXPathContext(ns);

            const NXml::TConstNode location = node.Node("b:Geo/b:Location", false, *ctxt);
            positions[id] = location.Node("gml:pos", false, *ctxt).Value<TString>();
            addresses[id] = node.Node("b:Geo/xal:AddressDetails/xal:Country/xal:AddressLine", false, *ctxt).Value<TString>();
        };

        ParseXml(BACKA, handler, "Company");
        UNIT_ASSERT_EQUAL(count, 0);
        // nothing found because namespace was not specified

        ParseXml(BACKA, handler, "Company", "http://maps.yandex.ru/backa/1.x");

        UNIT_ASSERT_VALUES_EQUAL(count, 2);

        UNIT_ASSERT_VALUES_EQUAL(positions["0001"], "37.62669 55.664827");
        UNIT_ASSERT_VALUES_EQUAL(positions["0002"], "150.819797 59.56092");

        UNIT_ASSERT_VALUES_EQUAL(addresses["0001"], "Москва, Каширское ш., 14");
        UNIT_ASSERT_VALUES_EQUAL(addresses["0002"], "Магадан, ул. Пролетарская, 43");
    }
}
