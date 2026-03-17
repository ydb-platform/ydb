from collections import OrderedDict

from lxml import etree

from tests.utils import assert_nodes_equal, load_xml
from zeep import xsd


def test_anyattribute():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="container">
            <complexType>
              <sequence>
                <element name="foo" type="xsd:string" />
              </sequence>
              <xsd:anyAttribute processContents="lax"/>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    assert container_elm.signature(schema) == (
        "ns0:container(foo: xsd:string, _attr_1: {})"
    )
    obj = container_elm(
        foo="bar", _attr_1=OrderedDict([("hiep", "hoi"), ("hoi", "hiep")])
    )

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" hiep="hoi" hoi="hiep">
          <ns0:foo>bar</ns0:foo>
        </ns0:container>
      </document>
    """

    node = etree.Element("document")
    container_elm.render(node, obj)
    assert_nodes_equal(expected, node)

    item = container_elm.parse(list(node)[0], schema)
    assert item._attr_1 == {"hiep": "hoi", "hoi": "hiep"}
    assert item.foo == "bar"


def test_attribute_list_type():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <simpleType name="list">
            <list itemType="int"/>
          </simpleType>

          <element name="container">
            <complexType>
              <sequence>
                <element name="foo" type="xsd:string" />
              </sequence>
              <xsd:attribute name="lijst" type="tns:list"/>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    assert container_elm.signature(schema) == (
        "ns0:container(foo: xsd:string, lijst: xsd:int[])"
    )
    obj = container_elm(foo="bar", lijst=[1, 2, 3])
    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" lijst="1 2 3">
          <ns0:foo>bar</ns0:foo>
        </ns0:container>
      </document>
    """

    node = etree.Element("document")
    container_elm.render(node, obj)
    assert_nodes_equal(expected, node)

    item = container_elm.parse(list(node)[0], schema)
    assert item.lijst == [1, 2, 3]
    assert item.foo == "bar"


def test_ref_attribute_qualified():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                attributeFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:attribute ref="tns:attr" use="required" />
            </xsd:complexType>
          </xsd:element>
          <xsd:attribute name="attr" type="xsd:string" />
        </xsd:schema>
    """
        )
    )

    elm_cls = schema.get_element("{http://tests.python-zeep.org/}container")
    instance = elm_cls(attr="hoi")

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" ns0:attr="hoi"/>
      </document>
    """

    node = etree.Element("document")
    elm_cls.render(node, instance)
    assert_nodes_equal(expected, node)


def test_ref_attribute_unqualified():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                attributeFormDefault="unqualified"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:attribute ref="tns:attr" use="required" />
            </xsd:complexType>
          </xsd:element>
          <xsd:attribute name="attr" type="xsd:string" />
        </xsd:schema>
    """
        )
    )

    elm_cls = schema.get_element("{http://tests.python-zeep.org/}container")
    instance = elm_cls(attr="hoi")

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" ns0:attr="hoi"/>
      </document>
    """

    node = etree.Element("document")
    elm_cls.render(node, instance)
    assert_nodes_equal(expected, node)


def test_complex_type_with_attributes():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:complexType name="Address">
            <xs:sequence>
              <xs:element minOccurs="0" maxOccurs="1" name="NameFirst" type="xs:string"/>
              <xs:element minOccurs="0" maxOccurs="1" name="NameLast" type="xs:string"/>
              <xs:element minOccurs="0" maxOccurs="1" name="Email" type="xs:string"/>
            </xs:sequence>
            <xs:attribute name="id" type="xs:string" use="required"/>
          </xs:complexType>
          <xs:element name="Address" type="Address"/>
        </xs:schema>
    """
        )
    )

    address_type = schema.get_element("Address")
    obj = address_type(
        NameFirst="John", NameLast="Doe", Email="j.doe@example.com", id="123"
    )

    node = etree.Element("document")
    address_type.render(node, obj)

    expected = """
        <document>
            <Address id="123">
                <NameFirst>John</NameFirst>
                <NameLast>Doe</NameLast>
                <Email>j.doe@example.com</Email>
            </Address>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_qualified_attribute():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                attributeFormDefault="qualified"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="Address">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="foo" type="xsd:string" form="unqualified" />
              </xsd:sequence>
              <xsd:attribute name="id" type="xsd:string" use="required" form="unqualified" />
              <xsd:attribute name="pos" type="xsd:string" use="required" />
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(foo="bar", id="20", pos="30")

    expected = """
      <document>
        <ns0:Address xmlns:ns0="http://tests.python-zeep.org/" id="20" ns0:pos="30">
          <foo>bar</foo>
        </ns0:Address>
      </document>
    """

    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_group():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                attributeFormDefault="qualified"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">

          <xsd:attributeGroup name="groepje">
            <xsd:attribute name="id" type="xsd:string" use="required" form="unqualified" />
            <xsd:attribute name="pos" type="xsd:string" use="required" />
          </xsd:attributeGroup>
          <xsd:element name="Address">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="foo" type="xsd:string" form="unqualified" />
              </xsd:sequence>
              <xsd:attributeGroup ref="tns:groepje"/>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(foo="bar", id="20", pos="30")

    expected = """
      <document>
        <ns0:Address xmlns:ns0="http://tests.python-zeep.org/" id="20" ns0:pos="30">
          <foo>bar</foo>
        </ns0:Address>
      </document>
    """

    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_group_nested():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                attributeFormDefault="qualified"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">

          <xsd:attributeGroup name="groepje">
            <xsd:attribute name="id" type="xsd:string" use="required" form="unqualified" />
            <xsd:attribute name="pos" type="xsd:string" use="required" />
            <xsd:attributeGroup ref="tns:nestje"/>
          </xsd:attributeGroup>

          <xsd:attributeGroup name="nestje">
            <xsd:attribute name="size" type="xsd:string" use="required" />
          </xsd:attributeGroup>

          <xsd:element name="Address">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="foo" type="xsd:string" form="unqualified" />
              </xsd:sequence>
              <xsd:attributeGroup ref="tns:groepje"/>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(foo="bar", id="20", pos="30", size="maat")

    expected = """
      <document>
        <ns0:Address
            xmlns:ns0="http://tests.python-zeep.org/" id="20" ns0:pos="30" ns0:size="maat">
          <foo>bar</foo>
        </ns0:Address>
      </document>
    """

    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_nested_attribute():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="container">
            <complexType>
              <sequence>
                <element name="item">
                    <complexType>
                        <sequence>
                            <element name="x" type="xsd:string"/>
                        </sequence>
                        <attribute name="y" type="xsd:string"/>
                    </complexType>
                </element>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    assert container_elm.signature(schema) == (
        "ns0:container(item: {x: xsd:string, y: xsd:string})"
    )
    obj = container_elm(item={"x": "foo", "y": "bar"})

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item y="bar">
            <ns0:x>foo</ns0:x>
          </ns0:item>
        </ns0:container>
      </document>
    """

    node = etree.Element("document")
    container_elm.render(node, obj)
    assert_nodes_equal(expected, node)


def test_attribute_union_type():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <simpleType name="one">
            <restriction base="xsd:string"/>
          </simpleType>
          <simpleType name="parent">
            <union memberTypes="tns:one tns:two"/>
          </simpleType>
          <simpleType name="two">
            <restriction base="xsd:string"/>
          </simpleType>
          <attribute name="something" type="tns:something"/>
          <simpleType name="something">
            <restriction base="tns:parent">
              <enumeration value="preserve"/>
            </restriction>
          </simpleType>
        </schema>
    """
        )
    )

    attr = schema.get_attribute("{http://tests.python-zeep.org/}something")
    assert attr("foo") == "foo"


def test_attribute_union_type_inline():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <simpleType name="parent">
            <union>
              <simpleType name="one">
                <restriction base="xsd:string"/>
              </simpleType>
              <simpleType name="two">
                <restriction base="xsd:string"/>
              </simpleType>
            </union>
          </simpleType>
          <attribute name="something" type="tns:something"/>
          <simpleType name="something">
            <restriction base="tns:parent">
              <enumeration value="preserve"/>
            </restriction>
          </simpleType>
        </schema>
    """
        )
    )

    attr = schema.get_attribute("{http://tests.python-zeep.org/}something")
    assert attr("foo") == "foo"


def test_attribute_value_retrieval():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <complexType name="Address">
            <sequence>
              <element name="Street" type="tns:Street"/>
            </sequence>
          </complexType>
          <complexType name="Street">
            <sequence>
                <element name="Name" type="string"/>
                <element name="Something" type="string" minOccurs="0"/>
            </sequence>
            <attribute name="ID" type="int" use="required"/>
            <attribute name="Postcode" type="string"/>
          </complexType>
        </schema>
    """
        )
    )

    Addr = schema.get_type("{http://tests.python-zeep.org/}Address")

    address = Addr()
    address.Street = {"ID": 100, "Name": "Foo"}

    expected = """
      <document>
        <ns0:Street xmlns:ns0="http://tests.python-zeep.org/" ID="100">
            <ns0:Name>Foo</ns0:Name>
        </ns0:Street>
      </document>
    """

    node = etree.Element("document")
    Addr.render(node, address)
    assert_nodes_equal(expected, node)
