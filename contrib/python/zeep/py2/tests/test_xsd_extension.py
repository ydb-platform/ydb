import datetime
import io

from lxml import etree

from tests.utils import (
    DummyTransport, assert_nodes_equal, load_xml, render_node)
from zeep import xsd

import yatest.common


def test_simple_content_extension():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

          <xsd:element name="ShoeSize">
            <xsd:complexType>
              <xsd:simpleContent>
                <xsd:extension base="xsd:integer">
                  <xsd:attribute name="sizing" type="xsd:string" />
                </xsd:extension>
              </xsd:simpleContent>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """.strip()
        )
    )
    shoe_type = schema.get_element("{http://tests.python-zeep.org/}ShoeSize")

    obj = shoe_type(20, sizing="EUR")

    node = render_node(shoe_type, obj)
    expected = """
        <document>
            <ns0:ShoeSize
                xmlns:ns0="http://tests.python-zeep.org/"
                sizing="EUR">20</ns0:ShoeSize>
        </document>
    """
    assert_nodes_equal(expected, node)

    obj = shoe_type.parse(node[0], schema)
    assert obj._value_1 == 20
    assert obj.sizing == "EUR"


def test_complex_content_sequence_extension():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

            <xsd:complexType name="Address">
              <xsd:complexContent>
                <xsd:extension base="tns:Name">
                  <xsd:sequence>
                    <xsd:element name="country" type="xsd:string"/>
                  </xsd:sequence>
                </xsd:extension>
              </xsd:complexContent>
            </xsd:complexType>
          <xsd:element name="Address" type="tns:Address"/>

          <xsd:complexType name="Name">
            <xsd:sequence>
              <xsd:element name="first_name" type="xsd:string"/>
              <xsd:element name="last_name" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
    """
        )
    )
    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")

    obj = address_type(first_name="foo", last_name="bar", country="The Netherlands")

    node = etree.Element("document")
    address_type.render(node, obj)
    expected = """
        <document>
            <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:first_name>foo</ns0:first_name>
                <ns0:last_name>bar</ns0:last_name>
                <ns0:country>The Netherlands</ns0:country>
            </ns0:Address>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_complex_content_with_recursive_elements():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

            <xsd:complexType name="Pet">
              <xsd:complexContent>
                <xsd:extension base="tns:Name">
                  <xsd:sequence>
                    <xsd:element name="children" type="tns:Pet" minOccurs="0" maxOccurs="unbounded"/>
                  </xsd:sequence>
                </xsd:extension>
              </xsd:complexContent>
            </xsd:complexType>
          <xsd:element name="Pet" type="tns:Pet"/>

          <xsd:complexType name="Name">
            <xsd:sequence>
              <xsd:element name="name" type="xsd:string"/>
              <xsd:element name="common_name" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
    """
        )
    )
    pet_type = schema.get_element("{http://tests.python-zeep.org/}Pet")
    assert pet_type.signature(schema=schema) == "ns0:Pet(ns0:Pet)"
    assert (
        pet_type.type.signature(schema=schema)
        == "ns0:Pet(name: xsd:string, common_name: xsd:string, children: ns0:Pet[])"
    )

    obj = pet_type(
        name="foo",
        common_name="bar",
        children=[pet_type(name="child-1", common_name="child-cname-1")],
    )

    node = etree.Element("document")
    pet_type.render(node, obj)
    expected = """
        <document>
            <ns0:Pet xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:name>foo</ns0:name>
                <ns0:common_name>bar</ns0:common_name>
                <ns0:children>
                    <ns0:name>child-1</ns0:name>
                    <ns0:common_name>child-cname-1</ns0:common_name>
                </ns0:children>
            </ns0:Pet>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_complex_content_sequence_extension_2():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

          <xsd:element name="container" type="tns:baseResponse"/>
          <xsd:complexType name="baseResponse">
            <xsd:sequence>
              <xsd:element name="item-1" type="xsd:string"/>
              <xsd:element name="item-2" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>

          <xsd:complexType name="response">
            <xsd:complexContent>
              <xsd:extension base="tns:baseResponse">
                <xsd:sequence>
                  <xsd:element name="item-3" type="xsd:string"/>
                </xsd:sequence>
              </xsd:extension>
            </xsd:complexContent>
          </xsd:complexType>
        </xsd:schema>
    """
        )
    )
    elm_cls = schema.get_element("{http://tests.python-zeep.org/}container")

    node = load_xml(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/"
                       xmlns:i="http://www.w3.org/2001/XMLSchema-instance"
                       i:type="ns0:response">
            <ns0:item-1>item-1</ns0:item-1>
            <ns0:item-2>item-2</ns0:item-2>
            <ns0:item-3>item-3</ns0:item-3>
        </ns0:container>
    """
    )
    data = elm_cls.parse(node, schema)
    assert data["item-1"] == "item-1"
    assert data["item-2"] == "item-2"
    assert data["item-3"] == "item-3"


def test_complex_type_with_extension_optional():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

          <xsd:complexType name="containerType">
            <xsd:complexContent>
              <xsd:extension base="tns:base">
                <xsd:sequence>
                  <xsd:element name="main_1" type="xsd:string"/>
                </xsd:sequence>
              </xsd:extension>
            </xsd:complexContent>
          </xsd:complexType>
          <xsd:element name="container" type="tns:containerType"/>

          <xsd:complexType name="base">
            <xsd:sequence>
              <xsd:element minOccurs="0" name="base_1" type="tns:baseType"/>
              <xsd:element minOccurs="0" name="base_2" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>

          <xsd:complexType name="baseType">
            <xsd:sequence>
              <xsd:element minOccurs="0" name="base_1_1" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
    """
        )
    )
    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_elm(main_1="foo")

    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:main_1>foo</ns0:main_1>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)

    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item.main_1 == "foo"


def test_complex_with_simple():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="Address">
            <xsd:complexType>
              <xsd:simpleContent>
                <xsd:extension base="tns:DateTimeType">
                  <xsd:attribute name="name" type="xsd:token"/>
                </xsd:extension>
              </xsd:simpleContent>
            </xsd:complexType>
          </xsd:element>

          <xsd:simpleType name="DateTimeType">
            <xsd:restriction base="xsd:dateTime"/>
          </xsd:simpleType>
        </xsd:schema>
    """
        )
    )
    address_type = schema.get_element("ns0:Address")

    assert address_type.type.signature()
    val = datetime.datetime(2016, 5, 29, 11, 13, 45)
    obj = address_type(val, name="foobie")

    expected = """
      <document>
        <ns0:Address xmlns:ns0="http://tests.python-zeep.org/"
            name="foobie">2016-05-29T11:13:45</ns0:Address>
      </document>
    """
    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_sequence_with_type():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema
                xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">

          <complexType name="base" abstract="true">
            <sequence>
              <element name="name" type="xsd:string" minOccurs="0"/>
            </sequence>
          </complexType>

          <complexType name="subtype">
            <complexContent>
              <extension base="tns:base">
                <attribute name="attr_1" type="xsd:string"/>
              </extension>
            </complexContent>
          </complexType>

          <complexType name="polytype">
            <sequence>
              <element name="item" type="tns:base" maxOccurs="unbounded" minOccurs="0"/>
            </sequence>
          </complexType>

          <element name="Seq" type="tns:polytype"/>
        </schema>
    """
        )
    )
    seq = schema.get_type("ns0:polytype")
    sub_type = schema.get_type("ns0:subtype")
    value = seq(item=[sub_type(attr_1="test", name="name")])

    node = etree.Element("document")
    seq.render(node, value)

    expected = """
      <document>
        <ns0:item
            xmlns:ns0="http://tests.python-zeep.org/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            attr_1="test" xsi:type="ns0:subtype">
          <ns0:name>name</ns0:name>
        </ns0:item>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_complex_simple_content():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
          <xsd:element name="value" type="tns:UUID"/>

          <xsd:complexType name="UUID">
            <xsd:simpleContent>
              <xsd:extension base="tns:UUID.Content">
                <xsd:attribute name="schemeID">
                  <xsd:simpleType>
                    <xsd:restriction base="xsd:token">
                      <xsd:maxLength value="60"/>
                      <xsd:minLength value="1"/>
                    </xsd:restriction>
                  </xsd:simpleType>
                </xsd:attribute>
                <xsd:attribute name="schemeAgencyID">
                  <xsd:simpleType>
                    <xsd:restriction base="xsd:token">
                      <xsd:maxLength value="60"/>
                      <xsd:minLength value="1"/>
                    </xsd:restriction>
                  </xsd:simpleType>
                </xsd:attribute>
              </xsd:extension>
            </xsd:simpleContent>
          </xsd:complexType>
          <xsd:simpleType name="UUID.Content">
            <xsd:restriction base="xsd:token">
              <xsd:maxLength value="36"/>
              <xsd:minLength value="36"/>
              <xsd:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
            </xsd:restriction>
          </xsd:simpleType>
        </xsd:schema>
    """
        )
    )  # noqa
    value_elm = schema.get_element("ns0:value")
    value = value_elm("00163e0c-0ea1-1ed6-93af-e818529bc1f1")

    node = etree.Element("document")
    value_elm.render(node, value)
    expected = """
      <document>
        <ns0:value xmlns:ns0="http://tests.python-zeep.org/">00163e0c-0ea1-1ed6-93af-e818529bc1f1</ns0:value>
      </document>
    """  # noqa
    assert_nodes_equal(expected, node)

    item = value_elm.parse(list(node)[0], schema)
    assert item._value_1 == "00163e0c-0ea1-1ed6-93af-e818529bc1f1"


def test_issue_221():
    transport = DummyTransport()
    transport.bind(
        "https://www.w3.org/TR/xmldsig-core/xmldsig-core-schema.xsd",
        load_xml(
            io.open(yatest.common.test_source_path("wsdl_files/xmldsig-core-schema.xsd"), "r")
            .read()
            .encode("utf-8")
        ),
    )

    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <import namespace="http://www.w3.org/2000/09/xmldsig#"
                  schemaLocation="https://www.w3.org/TR/xmldsig-core/xmldsig-core-schema.xsd"/>
          <complexType name="BaseType">
            <sequence>
              <element ref="ds:Signature" minOccurs="0"/>
            </sequence>
            <attribute name="Id"/>
          </complexType>
          <element name="exportOrgRegistryRequest">
            <complexType>
              <complexContent>
                <extension base="tns:BaseType">
                  <sequence>
                    <element name="SearchCriteria" maxOccurs="100">
                      <complexType>
                        <sequence>
                          <choice>
                            <choice>
                              <element ref="tns:OGRNIP"/>
                              <sequence>
                                <element name="OGRN" type="string"/>
                                <element name="KPP" type="string" minOccurs="0"/>
                              </sequence>
                            </choice>
                            <element ref="tns:orgVersionGUID"/>
                            <element ref="tns:orgRootEntityGUID"/>
                          </choice>
                          <element name="isRegistered" type="boolean" fixed="true" minOccurs="0">
                          </element>
                        </sequence>
                      </complexType>
                    </element>
                    <element name="lastEditingDateFrom" type="date" minOccurs="0"/>
                  </sequence>
                </extension>
              </complexContent>
            </complexType>
          </element>
          <simpleType name="OGRNIPType">
            <restriction base="string">
              <length value="13"/>
            </restriction>
          </simpleType>
          <element name="OGRNIP" type="tns:OGRNIPType"/>
          <element name="orgVersionGUID" type="tns:GUIDType"/>
          <element name="orgRootEntityGUID" type="tns:GUIDType"/>
          <simpleType name="GUIDType">
            <restriction base="string">
              <pattern value="([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}"/>
            </restriction>
          </simpleType>x
        </schema>
    """
        ),
        transport=transport,
    )

    schema.set_ns_prefix("tns", "http://tests.python-zeep.org/")
    elm = schema.get_element("tns:exportOrgRegistryRequest")

    # Args
    obj = elm(None, {"OGRN": "123123123123", "isRegistered": True})
    node = etree.Element("document")
    elm.render(node, obj)
    expected = """
      <document>
        <ns0:exportOrgRegistryRequest xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:SearchCriteria>
            <ns0:OGRN>123123123123</ns0:OGRN>
            <ns0:isRegistered>true</ns0:isRegistered>
          </ns0:SearchCriteria>
        </ns0:exportOrgRegistryRequest>
      </document>
    """
    assert_nodes_equal(expected, node)

    obj = elm(SearchCriteria={"orgVersionGUID": "1234", "isRegistered": False})
    node = etree.Element("document")
    elm.render(node, obj)
    expected = """
      <document>
        <ns0:exportOrgRegistryRequest xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:SearchCriteria>
            <ns0:orgVersionGUID>1234</ns0:orgVersionGUID>
            <ns0:isRegistered>false</ns0:isRegistered>
          </ns0:SearchCriteria>
        </ns0:exportOrgRegistryRequest>
      </document>
    """
    assert_nodes_equal(expected, node)

    obj = elm(SearchCriteria={"OGRNIP": "123123123123", "isRegistered": True})
    node = etree.Element("document")
    elm.render(node, obj)
    expected = """
      <document>
        <ns0:exportOrgRegistryRequest xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:SearchCriteria>
            <ns0:OGRNIP>123123123123</ns0:OGRNIP>
            <ns0:isRegistered>true</ns0:isRegistered>
          </ns0:SearchCriteria>
        </ns0:exportOrgRegistryRequest>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_complex_content_extension_with_sequence():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

            <xsd:complexType name="Package">
              <xsd:complexContent>
                <xsd:extension base="tns:AbstractPackage">
                  <xsd:attribute name="id" type="xsd:string"/>
                </xsd:extension>
              </xsd:complexContent>
            </xsd:complexType>

            <xsd:complexType name="SpecialPackage">
              <xsd:complexContent>
                <xsd:extension base="tns:Package">
                    <xsd:sequence>
                        <xsd:element name="otherElement" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:extension>
              </xsd:complexContent>
            </xsd:complexType>

            <xsd:complexType name="AbstractPackage">
              <xsd:attribute name="pkg_id" type="xsd:string"/>
            </xsd:complexType>

          <xsd:element name="SpecialPackage" type="tns:SpecialPackage"/>
        </xsd:schema>
    """
        )
    )
    address_type = schema.get_element("{http://tests.python-zeep.org/}SpecialPackage")

    obj = address_type(id="testString", pkg_id="nameId", otherElement="foobar")

    node = etree.Element("document")
    address_type.render(node, obj)
    expected = """
        <document>
            <ns0:SpecialPackage xmlns:ns0="http://tests.python-zeep.org/" pkg_id="nameId" id="testString">
                <ns0:otherElement>foobar</ns0:otherElement>
            </ns0:SpecialPackage>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_extension_abstract_complex_type():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

            <xsd:complexType name="Package" abstract="true"/>

            <xsd:complexType name="SpecialPackage">
              <xsd:complexContent>
                <xsd:extension base="tns:Package">
                    <xsd:sequence>
                        <xsd:element name="item" type="xsd:string"/>
                    </xsd:sequence>
                </xsd:extension>
              </xsd:complexContent>
            </xsd:complexType>

          <xsd:element name="SpecialPackage" type="tns:SpecialPackage"/>
        </xsd:schema>
    """
        )
    )
    package_cls = schema.get_element("{http://tests.python-zeep.org/}SpecialPackage")

    obj = package_cls(item="foo")

    node = etree.Element("document")
    package_cls.render(node, obj)
    expected = """
        <document>
            <ns0:SpecialPackage xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:item>foo</ns0:item>
            </ns0:SpecialPackage>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_extension_base_anytype():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

            <xsd:element name="container">
              <xsd:complexType>
                <xsd:complexContent>
                  <xsd:restriction base="xsd:anyType">
                    <xsd:attribute name="attr" type="xsd:unsignedInt" use="required"/>
                    <xsd:anyAttribute namespace="##other" processContents="lax"/>
                  </xsd:restriction>
                 </xsd:complexContent>
              </xsd:complexType>
            </xsd:element>
          </xsd:schema>
    """
        )
    )
    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")

    assert container_elm.signature() == (
        "{http://tests.python-zeep.org/}container(attr: xsd:unsignedInt, _attr_1: {})"
    )

    obj = container_elm(attr="foo")

    node = render_node(container_elm, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/" attr="foo"/>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_extension_on_ref():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
          <xsd:complexType name="type">
            <xsd:complexContent>
              <xsd:extension base="tns:base">
                <xsd:sequence>
                  <xsd:element ref="tns:extra"/>
                </xsd:sequence>
              </xsd:extension>
            </xsd:complexContent>
          </xsd:complexType>
          <xsd:complexType name="base">
            <xsd:sequence>
              <xsd:element name="item-1" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
          <xsd:element name="extra" type="xsd:string"/>
        </xsd:schema>
    """
        )
    )

    type_cls = schema.get_type("ns0:type")
    assert type_cls.signature()


def test_restrict_on_ref():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
          <xsd:complexType name="type">
            <xsd:complexContent>
              <xsd:restriction base="tns:base">
                <xsd:sequence>
                  <xsd:element ref="tns:extra"/>
                </xsd:sequence>
              </xsd:restriction>
            </xsd:complexContent>
          </xsd:complexType>
          <xsd:complexType name="base">
            <xsd:sequence>
              <xsd:element name="item-1" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
          <xsd:element name="extra" type="xsd:string"/>
        </xsd:schema>
    """
        )
    )

    type_cls = schema.get_type("ns0:type")
    assert type_cls.signature()
