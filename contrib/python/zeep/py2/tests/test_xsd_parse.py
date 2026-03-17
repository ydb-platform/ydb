import datetime

import pytest
from lxml import etree

from tests.utils import load_xml
from zeep import exceptions, xsd
from zeep.xsd.schema import Schema


def test_sequence_parse_regression():
    schema_doc = load_xml(
        b"""
        <?xml version="1.0" encoding="utf-8"?>
        <xsd:schema xmlns:tns="http://tests.python-zeep.org/attr"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          elementFormDefault="qualified"
          targetNamespace="http://tests.python-zeep.org/attr">
          <xsd:complexType name="Result">
            <xsd:attribute name="id" type="xsd:int" use="required"/>
          </xsd:complexType>
          <xsd:element name="Response">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element minOccurs="0" maxOccurs="1" name="Result" type="tns:Result"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
    )

    response_doc = load_xml(
        b"""
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
          <s:Body xmlns:xsd="http://www.w3.org/2001/XMLSchema">
            <Response xmlns="http://tests.python-zeep.org/attr">
                <Result id="2"/>
            </Response>
          </s:Body>
        </s:Envelope>
    """
    )

    schema = xsd.Schema(schema_doc)
    elm = schema.get_element("{http://tests.python-zeep.org/attr}Response")

    node = response_doc.xpath(
        "//ns0:Response",
        namespaces={
            "xsd": "http://www.w3.org/2001/XMLSchema",
            "ns0": "http://tests.python-zeep.org/attr",
        },
    )
    response = elm.parse(node[0], None)
    assert response.Result.id == 2


def test_sequence_parse_anytype():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "container"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.AnyType(),
                    )
                ]
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"


def test_sequence_parse_anytype_nil():
    schema = xsd.Schema(
        load_xml(
            b"""
        <?xml version="1.0" encoding="utf-8"?>
        <xsd:schema xmlns:tns="http://tests.python-zeep.org/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          elementFormDefault="qualified"
          targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element minOccurs="0" maxOccurs="1" name="item_1" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    container = schema.get_element("{http://tests.python-zeep.org/}container")

    expected = etree.fromstring(
        """
        <ns0:container
            xmlns:ns0="http://tests.python-zeep.org/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <ns0:item_1 xsi:type="xsd:anyType"/>
        </ns0:container>
    """
    )
    obj = container.parse(expected, schema)
    assert obj.item_1 is None


def test_sequence_parse_anytype_obj():
    value_type = xsd.ComplexType(
        xsd.Sequence(
            [xsd.Element("{http://tests.python-zeep.org/}value", xsd.Integer())]
        )
    )

    schema = Schema(
        etree.Element(
            "{http://www.w3.org/2001/XMLSchema}Schema",
            targetNamespace="http://tests.python-zeep.org/",
        )
    )

    root = schema.root_document
    root.register_type("{http://tests.python-zeep.org/}something", value_type)

    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "container"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.AnyType(),
                    )
                ]
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container
            xmlns:ns0="http://tests.python-zeep.org/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
          <ns0:item_1 xsi:type="ns0:something">
            <ns0:value>100</ns0:value>
          </ns0:item_1>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, schema)
    assert obj.item_1.value == 100


def test_sequence_parse_anytype_regression_17():
    schema_doc = load_xml(
        b"""
        <?xml version="1.0" encoding="utf-8"?>
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/tst"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/tst">
          <complexType name="CustomField">
            <sequence>
              <element name="parentItemURI" type="xsd:string"/>
              <element name="key" type="xsd:string"/>
              <element name="value" nillable="true"/>
            </sequence>
          </complexType>
          <complexType name="Text">
            <sequence>
              <element name="type" type="xsd:string"/>
              <element name="content" type="xsd:string"/>
              <element name="contentLossy" type="xsd:boolean"/>
            </sequence>
          </complexType>

          <element name="getCustomFieldResponse">
            <complexType>
              <sequence>
                <element name="getCustomFieldReturn" type="tns:CustomField"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
    )

    xml = load_xml(
        b"""
        <?xml version="1.0" encoding="utf-8"?>
        <tst:getCustomFieldResponse
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:tst="http://tests.python-zeep.org/tst">
          <tst:getCustomFieldReturn>
            <tst:parentItemURI>blabla</tst:parentItemURI>
            <tst:key>solution</tst:key>
            <tst:value xsi:type="tst:Text">
              <tst:type xsi:type="xsd:string">text/html</tst:type>
              <tst:content xsi:type="xsd:string">Test Solution</tst:content>
              <tst:contentLossy xsi:type="xsd:boolean">false</tst:contentLossy>
            </tst:value>
          </tst:getCustomFieldReturn>
        </tst:getCustomFieldResponse>
    """
    )

    schema = xsd.Schema(schema_doc)
    elm = schema.get_element("{http://tests.python-zeep.org/tst}getCustomFieldResponse")
    result = elm.parse(xml, schema)
    assert result.getCustomFieldReturn.value.content == "Test Solution"


def test_nested_complex_type():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.ComplexType(
                            xsd.Sequence(
                                [
                                    xsd.Element(
                                        "{http://tests.python-zeep.org/}item_2a",
                                        xsd.String(),
                                    ),
                                    xsd.Element(
                                        "{http://tests.python-zeep.org/}item_2b",
                                        xsd.String(),
                                    ),
                                ]
                            )
                        ),
                    ),
                ]
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>
            <ns0:item_2a>2a</ns0:item_2a>
            <ns0:item_2b>2b</ns0:item_2b>
          </ns0:item_2>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2.item_2a == "2a"
    assert obj.item_2.item_2b == "2b"


def test_nested_complex_type_optional():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.ComplexType(
                            xsd.Sequence(
                                [
                                    xsd.Choice(
                                        [
                                            xsd.Element(
                                                "{http://tests.python-zeep.org/}item_2a1",
                                                xsd.String(),
                                                min_occurs=0,
                                            ),
                                            xsd.Element(
                                                "{http://tests.python-zeep.org/}item_2a2",
                                                xsd.String(),
                                                min_occurs=0,
                                            ),
                                        ]
                                    ),
                                    xsd.Element(
                                        "{http://tests.python-zeep.org/}item_2b",
                                        xsd.String(),
                                    ),
                                ]
                            )
                        ),
                        min_occurs=0,
                        max_occurs="unbounded",
                    ),
                ]
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == []

    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2/>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == [None]

    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>
            <ns0:item_2a1>x</ns0:item_2a1>
          </ns0:item_2>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2[0].item_2a1 == "x"
    assert obj.item_2[0].item_2b is None

    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>
            <ns0:item_2a1>x</ns0:item_2a1>
            <ns0:item_2b/>
          </ns0:item_2>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2[0].item_2a1 == "x"
    assert obj.item_2[0].item_2b is None


def test_nested_choice_optional():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Choice(
                        [
                            xsd.Element(
                                "{http://tests.python-zeep.org/}item_2", xsd.String()
                            ),
                            xsd.Element(
                                "{http://tests.python-zeep.org/}item_3", xsd.String()
                            ),
                        ],
                        min_occurs=0,
                        max_occurs=1,
                    ),
                ]
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"

    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2 is None
    assert obj.item_3 is None


def test_union():
    schema_doc = load_xml(
        b"""
        <?xml version="1.0" encoding="utf-8"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/tst"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/tst">

          <xsd:element name="State" type="tns:StateType"/>

          <xsd:complexType name="StateType">
            <xsd:simpleContent>
                <xsd:extension base="tns:StateBaseType">
                  <xsd:anyAttribute namespace="##other" processContents="lax"/>
                </xsd:extension>
            </xsd:simpleContent>
          </xsd:complexType>
          <xsd:simpleType name="tns:StateBaseType">
            <xsd:union memberTypes="tns:Type1 tns:Type2"/>
          </xsd:simpleType>

          <xsd:simpleType name="Type1">
            <xsd:restriction base="xsd:NMTOKEN">
              <xsd:maxLength value="255"/>
              <xsd:enumeration value="Idle"/>
              <xsd:enumeration value="Processing"/>
              <xsd:enumeration value="Stopped"/>
            </xsd:restriction>
          </xsd:simpleType>

          <xsd:simpleType name="Type2">
            <xsd:restriction base="xsd:NMTOKEN">
              <xsd:maxLength value="255"/>
              <xsd:enumeration value="Paused"/>
            </xsd:restriction>
          </xsd:simpleType>
        </xsd:schema>
    """
    )

    xml = load_xml(
        b"""
        <?xml version="1.0" encoding="utf-8"?>
        <tst:State xmlns:tst="http://tests.python-zeep.org/tst">Idle</tst:State>
    """
    )

    schema = xsd.Schema(schema_doc)
    elm = schema.get_element("{http://tests.python-zeep.org/tst}State")
    result = elm.parse(xml, schema)
    assert result._value_1 == "Idle"


def test_parse_invalid_values():
    schema = xsd.Schema(
        load_xml(
            b"""
        <?xml version="1.0" encoding="utf-8"?>
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/">
          <element name="container">
            <complexType>
              <sequence>
                <element name="item_1" type="xsd:dateTime" />
                <element name="item_2" type="xsd:date" />
              </sequence>
              <attribute name="attr_1" type="xsd:dateTime" />
              <attribute name="attr_2" type="xsd:date" />
            </complexType>
          </element>
        </schema>
    """
        )
    )

    xml = load_xml(
        b"""
        <?xml version="1.0" encoding="utf-8"?>
        <tns:container
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:tns="http://tests.python-zeep.org/"
            attr_1="::" attr_2="2013-10-20">
          <tns:item_1>foo</tns:item_1>
          <tns:item_2>2016-10-20</tns:item_2>
        </tns:container>
    """
    )

    elm = schema.get_element("{http://tests.python-zeep.org/}container")
    result = elm.parse(xml, schema)
    assert result.item_1 is None
    assert result.item_2 == datetime.date(2016, 10, 20)
    assert result.attr_1 is None
    assert result.attr_2 == datetime.date(2013, 10, 20)


def test_xsd_missing_localname():
    schema = xsd.Schema(
        load_xml(
            b"""
        <?xml version="1.0" encoding="utf-8"?>
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/">
          <element name="container" type="xsd:"/>
        </schema>
    """
        )
    )

    schema.get_element("{http://tests.python-zeep.org/}container")


def test_xsd_choice_with_references():
    schema = xsd.Schema(
        load_xml(
            """
            <xsd:schema
                xmlns:zeep="http://tests.python-zeep.org/"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/">

              <xsd:element name="el1" type="xsd:string"/>
              <xsd:element name="el2" type="xsd:string"/>

              <xsd:element name="container">
        		<xsd:complexType>
        			<xsd:sequence>
        				<xsd:choice>
        					<xsd:element ref="zeep:el1"/>
        					<xsd:element ref="zeep:el2"/>
        					<xsd:element name="el3" type="xsd:string"/>
        				</xsd:choice>
        			</xsd:sequence>
        		</xsd:complexType>
              </xsd:element>

             </xsd:schema>
        """
        )
    )
    schema.set_ns_prefix("tns", "http://tests.python-zeep.org/")

    xml = load_xml(
        b"""
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:BAD_ELEMENT>BAD VALUE</ns0:BAD_ELEMENT>
        </ns0:container>
    """
    )

    elm = schema.get_element("{http://tests.python-zeep.org/}container")

    with pytest.raises(exceptions.XMLParseError) as exc:
        result = elm.parse(xml, schema)
    assert "BAD_ELEMENT" in str(exc)

    xml = load_xml(
        b"""
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:el2>value2</ns0:el2>
        </ns0:container>
    """
    )

    result = elm.parse(xml, schema)

    assert result.el2 == "value2"
