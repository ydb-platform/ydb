import io

import pytest
from lxml import etree

from tests.utils import (
    DummyTransport, assert_nodes_equal, load_xml, render_node)
from zeep import xsd

import yatest.common


@pytest.fixture(scope="function")
def transport():
    transport = DummyTransport()
    transport.bind(
        "http://schemas.xmlsoap.org/soap/encoding/",
        load_xml(io.open(yatest.common.test_source_path("wsdl_files/soap-enc.xsd"), "r").read().encode("utf-8")),
    )
    return transport


def test_simple_type(transport):
    schema = xsd.Schema(
        load_xml(
            """
    <xsd:schema
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
        targetNamespace="http://tests.python-zeep.org/tns">
      <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
      <xsd:complexType name="ArrayOfString">
        <xsd:complexContent>
          <xsd:restriction base="SOAP-ENC:Array">
            <xsd:attribute ref="SOAP-ENC:arrayType" wsdl:arrayType="xsd:string[]"/>
          </xsd:restriction>
        </xsd:complexContent>
      </xsd:complexType>
    </xsd:schema>
    """
        ),
        transport=transport,
    )

    ArrayOfString = schema.get_type("ns0:ArrayOfString")
    print(ArrayOfString.__dict__)

    value = ArrayOfString(["item", "and", "even", "more", "items"])

    node = etree.Element("document")
    ArrayOfString.render(node, value)

    expected = """
        <document>
            <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">item</item>
            <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">and</item>
            <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">even</item>
            <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">more</item>
            <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">items</item>
        </document>
    """  # noqa

    assert_nodes_equal(expected, node)

    data = ArrayOfString.parse_xmlelement(node, schema)
    assert data == ["item", "and", "even", "more", "items"]
    assert data.as_value_object()


def test_simple_type_nested(transport):
    schema = xsd.Schema(
        load_xml(
            """
    <xsd:schema
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
        targetNamespace="http://tests.python-zeep.org/tns"
        xmlns:tns="http://tests.python-zeep.org/tns">
      <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
      <xsd:complexType name="container">
        <xsd:sequence>
          <xsd:element name="strings" type="tns:ArrayOfString"/>
        </xsd:sequence>
      </xsd:complexType>

      <xsd:complexType name="ArrayOfString">
        <xsd:complexContent>
          <xsd:restriction base="SOAP-ENC:Array">
            <xsd:attribute ref="SOAP-ENC:arrayType" wsdl:arrayType="xsd:string[]"/>
          </xsd:restriction>
        </xsd:complexContent>
      </xsd:complexType>
    </xsd:schema>
    """
        ),
        transport=transport,
    )

    Container = schema.get_type("ns0:container")
    value = Container(strings=["item", "and", "even", "more", "items"])

    assert value.strings == ["item", "and", "even", "more", "items"]

    node = etree.Element("document")
    Container.render(node, value)

    expected = """
        <document>
            <strings>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">item</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">and</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">even</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">more</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">items</item>
            </strings>
        </document>
    """  # noqa

    assert_nodes_equal(expected, node)

    data = Container.parse_xmlelement(node, schema)
    assert data.strings == ["item", "and", "even", "more", "items"]


def test_simple_type_nested_inline_type(transport):
    schema = xsd.Schema(
        load_xml(
            """
    <xsd:schema
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
        targetNamespace="http://tests.python-zeep.org/tns"
        xmlns:tns="http://tests.python-zeep.org/tns">
      <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
      <xsd:complexType name="container">
        <xsd:sequence>
          <xsd:element name="strings" type="tns:ArrayOfString"/>
        </xsd:sequence>
      </xsd:complexType>

      <xsd:complexType name="ArrayOfString">
        <xsd:complexContent>
          <xsd:restriction base="SOAP-ENC:Array">
            <xsd:attribute ref="SOAP-ENC:arrayType" wsdl:arrayType="xsd:string[]"/>
          </xsd:restriction>
        </xsd:complexContent>
      </xsd:complexType>
    </xsd:schema>
    """
        ),
        transport=transport,
    )

    Container = schema.get_type("ns0:container")
    node = load_xml(
        """
        <document xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <strings xsi:type="soapenc:Array" xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/">
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">item</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">and</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">even</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">more</item>
              <item xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">items</item>
            </strings>
        </document>
    """
    )  # noqa

    data = Container.parse_xmlelement(node, schema)
    assert data.strings == ["item", "and", "even", "more", "items"]


def test_complex_type(transport):
    schema = xsd.Schema(
        load_xml(
            """
    <xsd:schema
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
        xmlns:tns="http://tests.python-zeep.org/tns"
        targetNamespace="http://tests.python-zeep.org/tns">
      <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>

      <xsd:complexType name="ArrayObject">
        <xsd:sequence>
          <xsd:element name="attr_1" type="xsd:string"/>
          <xsd:element name="attr_2" type="xsd:string"/>
        </xsd:sequence>
      </xsd:complexType>
      <xsd:complexType name="ArrayOfObject">
        <xsd:complexContent>
          <xsd:restriction base="SOAP-ENC:Array">
            <xsd:sequence>
              <xsd:element name="obj" type="tns:ArrayObject" minOccurs="0" maxOccurs="unbounded"/>
            </xsd:sequence>
            <xsd:attribute ref="SOAP-ENC:arrayType" wsdl:arrayType="tns:ArrayObject[]"/>
          </xsd:restriction>
        </xsd:complexContent>
      </xsd:complexType>
    </xsd:schema>
    """
        ),
        transport=transport,
    )

    ArrayOfObject = schema.get_type("ns0:ArrayOfObject")
    ArrayObject = schema.get_type("ns0:ArrayObject")

    value = ArrayOfObject(
        [
            ArrayObject(attr_1="attr-1", attr_2="attr-2"),
            ArrayObject(attr_1="attr-3", attr_2="attr-4"),
            ArrayObject(attr_1="attr-5", attr_2="attr-6"),
        ]
    )

    node = etree.Element("document")
    ArrayOfObject.render(node, value)

    expected = """
        <document>
            <obj>
                <attr_1>attr-1</attr_1>
                <attr_2>attr-2</attr_2>
            </obj>
            <obj>
                <attr_1>attr-3</attr_1>
                <attr_2>attr-4</attr_2>
            </obj>
            <obj>
                <attr_1>attr-5</attr_1>
                <attr_2>attr-6</attr_2>
            </obj>
        </document>
    """
    assert_nodes_equal(expected, node)
    data = ArrayOfObject.parse_xmlelement(node, schema)

    assert data[0].attr_1 == "attr-1"
    assert data[0].attr_2 == "attr-2"
    assert data[1].attr_1 == "attr-3"
    assert data[1].attr_2 == "attr-4"
    assert data[2].attr_1 == "attr-5"
    assert data[2].attr_2 == "attr-6"


def test_complex_type_without_name(transport):
    schema = xsd.Schema(
        load_xml(
            """
    <xsd:schema
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
        xmlns:tns="http://tests.python-zeep.org/tns"
        targetNamespace="http://tests.python-zeep.org/tns">
      <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>

      <xsd:complexType name="ArrayObject">
        <xsd:sequence>
          <xsd:element name="attr_1" type="xsd:string"/>
          <xsd:element name="attr_2" type="xsd:string"/>
        </xsd:sequence>
      </xsd:complexType>
      <xsd:complexType name="ArrayOfObject">
        <xsd:complexContent>
          <xsd:restriction base="SOAP-ENC:Array">
            <xsd:attribute ref="SOAP-ENC:arrayType" wsdl:arrayType="tns:ArrayObject[]"/>
          </xsd:restriction>
        </xsd:complexContent>
      </xsd:complexType>
    </xsd:schema>
    """
        ),
        transport=transport,
    )

    ArrayOfObject = schema.get_type("ns0:ArrayOfObject")
    ArrayObject = schema.get_type("ns0:ArrayObject")

    value = ArrayOfObject(
        [
            ArrayObject(attr_1="attr-1", attr_2="attr-2"),
            ArrayObject(attr_1="attr-3", attr_2="attr-4"),
            ArrayObject(attr_1="attr-5", attr_2="attr-6"),
        ]
    )

    node = etree.Element("document")
    ArrayOfObject.render(node, value)

    expected = """
        <document>
            <ArrayObject>
                <attr_1>attr-1</attr_1>
                <attr_2>attr-2</attr_2>
            </ArrayObject>
            <ArrayObject>
                <attr_1>attr-3</attr_1>
                <attr_2>attr-4</attr_2>
            </ArrayObject>
            <ArrayObject>
                <attr_1>attr-5</attr_1>
                <attr_2>attr-6</attr_2>
            </ArrayObject>
        </document>
    """
    assert_nodes_equal(expected, node)
    data = ArrayOfObject.parse_xmlelement(node, schema)

    assert len(data) == 3
    assert data[0]["attr_1"] == "attr-1"
    assert data[0]["attr_2"] == "attr-2"
    assert data[1]["attr_1"] == "attr-3"
    assert data[1]["attr_2"] == "attr-4"
    assert data[2]["attr_1"] == "attr-5"
    assert data[2]["attr_2"] == "attr-6"


def test_soap_array_parse_remote_ns():
    transport = DummyTransport()
    transport.bind(
        "http://schemas.xmlsoap.org/soap/encoding/",
        load_xml(io.open(yatest.common.test_source_path("wsdl_files/soap-enc.xsd"), "r").read().encode("utf-8")),
    )

    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/"
          xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/"
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          targetNamespace="http://tests.python-zeep.org/"
          elementFormDefault="qualified">
          <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
          <xsd:simpleType name="CountryCodeType">
            <xsd:restriction base="xsd:string">
              <xsd:length value="2"/>
              <xsd:pattern value="[a-zA-Z]{2}"/>
            </xsd:restriction>
          </xsd:simpleType>
          <xsd:complexType name="CountryItemType">
            <xsd:sequence>
              <xsd:element name="code" type="tns:CountryCodeType"/>
              <xsd:element name="name" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
          <xsd:complexType name="CountriesArrayType">
            <xsd:complexContent>
              <xsd:restriction base="soapenc:Array">
                <xsd:attribute ref="soapenc:arrayType" wsdl:arrayType="tns:CountryItemType[]"/>
              </xsd:restriction>
            </xsd:complexContent>
          </xsd:complexType>
          <xsd:element name="countries" type="tns:CountriesArrayType"/>
        </xsd:schema>
    """
        ),
        transport,
    )

    doc = load_xml(
        """
      <countries
            SOAP-ENC:arrayType="ns1:CountryItemType[1]"
            xsi:type="ns1:CountriesArrayType"
            xmlns:ns1="http://tests.python-zeep.org/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <item xsi:type="ns1:CountryItemType">
          <code xsi:type="ns1:CountryCodeType">NL</code>
          <name xsi:type="xsd:string">The Netherlands</name>
        </item>
      </countries>
    """
    )

    elm = schema.get_element("ns0:countries")
    data = elm.parse(doc, schema)

    assert data[0].code == "NL"
    assert data[0].name == "The Netherlands"


def test_wsdl_array_type():
    transport = DummyTransport()
    transport.bind(
        "http://schemas.xmlsoap.org/soap/encoding/",
        load_xml(io.open(yatest.common.test_source_path("wsdl_files/soap-enc.xsd"), "r").read().encode("utf-8")),
    )

    schema = xsd.Schema(
        load_xml(
            """
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:tns="http://tests.python-zeep.org/"
                    xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
                    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
                    targetNamespace="http://tests.python-zeep.org/"
                    elementFormDefault="qualified">
          <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
          <xsd:complexType name="array">
            <xsd:complexContent>
              <xsd:restriction base="SOAP-ENC:Array">
                <xsd:attribute ref="SOAP-ENC:arrayType" wsdl:arrayType="tns:base[]"/>
              </xsd:restriction>
            </xsd:complexContent>
          </xsd:complexType>
          <xsd:complexType name="base">
            <xsd:sequence>
              <xsd:element minOccurs="0" name="item_1" type="xsd:string"/>
              <xsd:element minOccurs="0" name="item_2" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
          <xsd:element name="array" type="tns:array"/>
        </xsd:schema>
    """
        ),
        transport,
    )
    array_elm = schema.get_element("{http://tests.python-zeep.org/}array")

    item_type = schema.get_type("{http://tests.python-zeep.org/}base")
    item_1 = item_type(item_1="foo_1", item_2="bar_1")
    item_2 = item_type(item_1="foo_2", item_2="bar_2")

    # array = array_elm([
    #     xsd.AnyObject(item_type, item_1),
    #     xsd.AnyObject(item_type, item_2),
    # ])

    array = array_elm([item_1, item_2])
    node = etree.Element("document")
    assert array_elm.signature(schema=schema) == "ns0:array(ns0:array)"

    array_type = schema.get_type("ns0:array")
    assert array_type.signature(schema=schema) == (
        "ns0:array(_value_1: base[], arrayType: xsd:string, "
        + "offset: ns1:arrayCoordinate, id: xsd:ID, href: xsd:anyURI, _attr_1: {})"
    )
    array_elm.render(node, array)
    expected = """
        <document>
            <ns0:array xmlns:ns0="http://tests.python-zeep.org/">
                <base>
                    <ns0:item_1>foo_1</ns0:item_1>
                    <ns0:item_2>bar_1</ns0:item_2>
                </base>
                <base>
                    <ns0:item_1>foo_2</ns0:item_1>
                    <ns0:item_2>bar_2</ns0:item_2>
                </base>
            </ns0:array>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_soap_array_parse():
    transport = DummyTransport()
    transport.bind(
        "http://schemas.xmlsoap.org/soap/encoding/",
        load_xml(io.open(yatest.common.test_source_path("wsdl_files/soap-enc.xsd"), "r").read().encode("utf-8")),
    )

    schema = xsd.Schema(
        load_xml(
            """
    <?xml version="1.0"?>
    <schema xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
      <import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
      <complexType name="FlagDetailsStruct">
          <sequence>
              <element name="Name">
                  <simpleType>
                      <restriction base="string">
                          <maxLength value="512"/>
                      </restriction>
                  </simpleType>
              </element>
              <element name="Value" type="string"/>
          </sequence>
      </complexType>
      <complexType name="FlagDetailsList">
          <complexContent>
              <restriction base="soapenc:Array">
                  <sequence>
                      <element
                        name="FlagDetailsStruct" type="tns:FlagDetailsStruct"
                        minOccurs="0" maxOccurs="unbounded"/>
                  </sequence>
                  <attribute ref="soapenc:arrayType" use="required"/>
              </restriction>
          </complexContent>
      </complexType>
      <element name="FlagDetailsList" type="tns:FlagDetailsList"/>
    </schema>
    """
        ),
        transport,
    )

    doc = load_xml(
        """
         <FlagDetailsList xmlns="http://tests.python-zeep.org/">
            <FlagDetailsStruct>
               <Name>flag1</Name>
               <Value>value1</Value>
            </FlagDetailsStruct>
            <FlagDetailsStruct>
               <Name>flag2</Name>
               <Value>value2</Value>
            </FlagDetailsStruct>
         </FlagDetailsList>
    """
    )

    elm = schema.get_element("ns0:FlagDetailsList")
    data = elm.parse(doc, schema)
    assert data[0].Name == "flag1"
    assert data[0].Value == "value1"
    assert data[1].Name == "flag2"
    assert data[1].Value == "value2"


def test_xml_soap_enc_string(transport):
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
          <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>

          <xsd:element name="value" type="tns:ArrayOfString"/>

          <xsd:complexType name="ArrayOfString">
            <xsd:complexContent>
              <xsd:restriction base="soapenc:Array">
                <xsd:attribute ref="soapenc:arrayType" wsdl:arrayType="soapenc:string[]"/>
              </xsd:restriction>
            </xsd:complexContent>
          </xsd:complexType>

        </xsd:schema>
    """
        ),
        transport,
    )
    shoe_type = schema.get_element("{http://tests.python-zeep.org/}value")

    obj = shoe_type(["foo"])
    node = render_node(shoe_type, obj)
    expected = """
        <document>
            <ns0:value xmlns:ns0="http://tests.python-zeep.org/">
              <string>foo</string>
            </ns0:value>
        </document>
    """
    assert_nodes_equal(expected, node)

    obj = shoe_type.parse(node[0], schema)
    assert obj[0]["_value_1"] == "foo"

    # Via string-types
    string_type = schema.get_type("{http://schemas.xmlsoap.org/soap/encoding/}string")
    obj = shoe_type([string_type("foo")])
    node = render_node(shoe_type, obj)
    expected = """
        <document>
            <ns0:value xmlns:ns0="http://tests.python-zeep.org/">
              <string>foo</string>
            </ns0:value>
        </document>
    """
    assert_nodes_equal(expected, node)

    obj = shoe_type.parse(node[0], schema)
    assert obj[0]["_value_1"] == "foo"

    # Via dicts
    string_type = schema.get_type("{http://schemas.xmlsoap.org/soap/encoding/}string")
    obj = shoe_type([{"_value_1": "foo"}])
    node = render_node(shoe_type, obj)
    expected = """
        <document>
            <ns0:value xmlns:ns0="http://tests.python-zeep.org/">
              <string>foo</string>
            </ns0:value>
        </document>
    """
    assert_nodes_equal(expected, node)

    obj = shoe_type.parse(node[0], schema)
    assert obj[0]["_value_1"] == "foo"
