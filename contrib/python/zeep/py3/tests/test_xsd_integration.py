import copy

import pytest
from lxml import etree

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import xsd


def test_xml_complex_type_nested_wrong_type():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="container">
            <complexType>
              <sequence>
                <element minOccurs="0" maxOccurs="1" name="item">
                  <complexType>
                    <sequence>
                      <element name="x" type="integer"/>
                      <element name="y" type="integer"/>
                    </sequence>
                  </complexType>
                </element>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    container_elm = schema.get_element("ns0:container")
    with pytest.raises(TypeError):
        container_elm(item={"bar": 1})


def test_xml_element_with_annotation():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="Address" type="tns:AddressType">
              <annotation>
                  <documentation>HOI!</documentation>
              </annotation>
          </element>
          <complexType name="AddressType">
            <sequence>
              <element minOccurs="0" maxOccurs="unbounded" name="foo" type="string" />
            </sequence>
          </complexType>
        </schema>
    """
        )
    )

    schema.set_ns_prefix("tns", "http://tests.python-zeep.org/")
    address_type = schema.get_element("tns:Address")
    address_type(foo="bar")


def test_xml_complex_type_parsexml():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="Address">
            <complexType>
              <sequence>
                <element minOccurs="0" maxOccurs="1" name="foo" type="string" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")

    input_node = load_xml(
        """
        <Address xmlns="http://tests.python-zeep.org/">
          <foo>bar</foo>
        </Address>
    """
    )

    obj = address_type.parse(input_node, None)
    assert obj.foo == "bar"


def test_xml_array():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="Address">
            <complexType>
              <sequence>
                <element minOccurs="0" maxOccurs="unbounded" name="foo" type="string" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )
    schema.set_ns_prefix("tns", "http://tests.python-zeep.org/")

    address_type = schema.get_element("tns:Address")

    obj = address_type()
    assert obj.foo == []
    obj.foo.append("foo")
    obj.foo.append("bar")

    expected = """
        <document  xmlns:tns="http://tests.python-zeep.org/">
          <tns:Address>
            <tns:foo>foo</tns:foo>
            <tns:foo>bar</tns:foo>
          </tns:Address>
        </document>
    """
    node = etree.Element("document", nsmap=schema._prefix_map_custom)
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_xml_complex_type_unbounded_one():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="Address">
            <complexType>
              <sequence>
                <element minOccurs="0" maxOccurs="unbounded" name="foo" type="string" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )
    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(foo=["foo"])

    expected = """
        <document>
          <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:foo>foo</ns0:foo>
          </ns0:Address>
        </document>
    """

    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_xml_complex_type_unbounded_named():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="Address" type="tns:AddressType" />
          <complexType name="AddressType">
            <sequence>
              <element minOccurs="0" maxOccurs="unbounded" name="foo" type="string" />
            </sequence>
          </complexType>
        </schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type()
    assert obj.foo == []
    obj.foo.append("foo")
    obj.foo.append("bar")

    expected = """
        <document>
          <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:foo>foo</ns0:foo>
            <ns0:foo>bar</ns0:foo>
          </ns0:Address>
        </document>
    """

    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_xml_complex_type_array_to_other_complex_object():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:complexType name="Address">
            <xs:sequence>
              <xs:element minOccurs="0" maxOccurs="1" name="foo" type="xs:string" />
            </xs:sequence>
          </xs:complexType>
          <xs:complexType name="ArrayOfAddress">
            <xs:sequence>
              <xs:element minOccurs="0" maxOccurs="unbounded" name="Address" nillable="true" type="Address" />
            </xs:sequence>
          </xs:complexType>
          <xs:element name="ArrayOfAddress" type="ArrayOfAddress"/>
        </xs:schema>
    """
        )
    )

    address_array = schema.get_element("ArrayOfAddress")
    obj = address_array()
    assert obj.Address == []

    obj.Address.append(schema.get_type("Address")(foo="foo"))
    obj.Address.append(schema.get_type("Address")(foo="bar"))

    expected = """
        <?xml version="1.0"?>
        <document>
            <ArrayOfAddress>
                <Address>
                    <foo>foo</foo>
                </Address>
                <Address>
                    <foo>bar</foo>
                </Address>
            </ArrayOfAddress>
        </document>
    """

    result = render_node(address_array, obj)
    assert_nodes_equal(expected, result)


def test_xml_complex_type_init_kwargs():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="Address">
            <complexType>
              <sequence>
                <element minOccurs="0" maxOccurs="1" name="NameFirst" type="string"/>
                <element minOccurs="0" maxOccurs="1" name="NameLast" type="string"/>
                <element minOccurs="0" maxOccurs="1" name="Email" type="string"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(NameFirst="John", NameLast="Doe", Email="j.doe@example.com")
    assert obj.NameFirst == "John"
    assert obj.NameLast == "Doe"
    assert obj.Email == "j.doe@example.com"


def test_xml_complex_type_init_args():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="Address">
            <complexType>
              <sequence>
                <element minOccurs="0" maxOccurs="1" name="NameFirst" type="string"/>
                <element minOccurs="0" maxOccurs="1" name="NameLast" type="string"/>
                <element minOccurs="0" maxOccurs="1" name="Email" type="string"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )
    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type("John", "Doe", "j.doe@example.com")
    assert obj.NameFirst == "John"
    assert obj.NameLast == "Doe"
    assert obj.Email == "j.doe@example.com"


def test_xml_element_ref_missing_namespace():
    # For buggy soap servers (#170)
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="foo" type="string"/>
          <element name="bar">
            <complexType>
              <sequence>
                <element ref="tns:foo"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    custom_type = schema.get_element("{http://tests.python-zeep.org/}bar")
    input_xml = load_xml(
        """
            <ns0:bar xmlns:ns0="http://tests.python-zeep.org/">
                <foo>bar</foo>
            </ns0:bar>
    """
    )
    item = custom_type.parse(input_xml, schema)
    assert item.foo == "bar"


def test_xml_element_ref():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="qualified">
          <element name="foo" type="string"/>
          <element name="bar">
            <complexType>
              <sequence>
                <element ref="tns:foo"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    foo_type = schema.get_element("{http://tests.python-zeep.org/}foo")
    assert isinstance(foo_type.type, xsd.String)

    custom_type = schema.get_element("{http://tests.python-zeep.org/}bar")
    custom_type.signature()
    obj = custom_type(foo="bar")

    node = etree.Element("document")
    custom_type.render(node, obj)
    expected = """
        <document>
            <ns0:bar xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:foo>bar</ns0:foo>
            </ns0:bar>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_xml_element_ref_occurs():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="qualified">
          <element name="foo" type="string"/>
          <element name="bar">
            <complexType>
              <sequence>
                <element ref="tns:foo" minOccurs="0"/>
                <element name="bar" type="string"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    foo_type = schema.get_element("{http://tests.python-zeep.org/}foo")
    assert isinstance(foo_type.type, xsd.String)

    custom_type = schema.get_element("{http://tests.python-zeep.org/}bar")
    custom_type.signature()
    obj = custom_type(bar="foo")

    node = etree.Element("document")
    custom_type.render(node, obj)
    expected = """
        <document>
            <ns0:bar xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:bar>foo</ns0:bar>
            </ns0:bar>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_xml_unqualified():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                attributeFormDefault="qualified"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="Address">
            <complexType>
              <sequence>
                <element name="foo" type="xsd:string" form="unqualified" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(foo="bar")

    expected = """
      <document>
        <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
          <foo>bar</foo>
        </ns0:Address>
      </document>
    """

    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_xml_defaults():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="item_1" type="xsd:string" default="hoi"/>
                <xsd:element name="item_2" type="xsd:string" default="hoi" minOccurs="0"/>
              </xsd:sequence>
              <xsd:attribute name="attr_1" type="xsd:string" default="hoi"/>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    container_type = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_type()
    assert obj.item_1 == "hoi"
    assert obj.item_2 is None
    assert obj.attr_1 == "hoi"

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" attr_1="hoi">
          <ns0:item_1>hoi</ns0:item_1>
        </ns0:container>
      </document>
    """
    node = etree.Element("document")
    container_type.render(node, obj)
    assert_nodes_equal(expected, node)

    obj.item_2 = "ok"

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" attr_1="hoi">
          <ns0:item_1>hoi</ns0:item_1>
          <ns0:item_2>ok</ns0:item_2>
        </ns0:container>
      </document>
    """
    node = etree.Element("document")
    container_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_xml_defaults_parse_boolean():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="foo" type="xsd:boolean" default="false"/>
              </xsd:sequence>
              <xsd:attribute name="bar" type="xsd:boolean" default="0"/>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    container_type = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_type()
    assert obj.foo == "false"
    assert obj.bar == "0"

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" bar="false">
          <ns0:foo>false</ns0:foo>
        </ns0:container>
      </document>
    """
    node = etree.Element("document")
    container_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_xml_defaults_parse():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="item_1" type="xsd:string" default="hoi" minOccurs="0"/>
              </xsd:sequence>
              <xsd:attribute name="attr_1" type="xsd:string" default="hoi"/>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")

    node = load_xml(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>hoi</ns0:item_1>
        </ns0:container>
    """
    )
    item = container_elm.parse(node, schema)
    assert item.attr_1 == "hoi"


def test_xml_init_with_dicts():
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
                <xsd:element name="name" type="xsd:string"/>
                <xsd:element minOccurs="0" name="optional" type="xsd:string"/>
                <xsd:element name="container" nillable="true" type="tns:Container"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>

          <xsd:complexType name="Container">
            <xsd:sequence>
              <xsd:element maxOccurs="unbounded" minOccurs="0" name="service"
                           nillable="true" type="tns:ServiceRequestType"/>
            </xsd:sequence>
          </xsd:complexType>

          <xsd:complexType name="ServiceRequestType">
            <xsd:sequence>
              <xsd:element name="name" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(name="foo", container={"service": [{"name": "foo"}]})

    expected = """
      <document>
        <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:name>foo</ns0:name>
          <ns0:container>
            <ns0:service>
              <ns0:name>foo</ns0:name>
            </ns0:service>
          </ns0:container>
        </ns0:Address>
      </document>
    """

    node = etree.Element("document")
    address_type.render(node, obj)
    assert_nodes_equal(expected, node)


def test_xml_sequence_in_sequence():
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
          <element name="container">
            <complexType>
              <sequence>
                <sequence>
                  <element name="item_1" type="xsd:string"/>
                  <element name="item_2" type="xsd:string"/>
                </sequence>
              </sequence>
            </complexType>
          </element>
          <element name="foobar" type="xsd:string"/>
        </schema>
    """
        )
    )
    element = schema.get_element("ns0:container")
    value = element(item_1="foo", item_2="bar")

    node = etree.Element("document")
    element.render(node, value)

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
        </ns0:container>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_xml_sequence_in_sequence_many():
    node = load_xml(
        """
        <?xml version="1.0"?>
        <schema
                xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="container">
            <complexType>
              <sequence>
                <sequence minOccurs="2" maxOccurs="2">
                  <element name="item_1" type="xsd:string"/>
                  <element name="item_2" type="xsd:string"/>
                </sequence>
              </sequence>
            </complexType>
          </element>
          <element name="foobar" type="xsd:string"/>
        </schema>
    """
    )
    schema = xsd.Schema(node)
    element = schema.get_element("ns0:container")
    value = element(
        _value_1=[
            {"item_1": "value-1-1", "item_2": "value-1-2"},
            {"item_1": "value-2-1", "item_2": "value-2-2"},
        ]
    )

    assert value._value_1 == [
        {"item_1": "value-1-1", "item_2": "value-1-2"},
        {"item_1": "value-2-1", "item_2": "value-2-2"},
    ]

    node = etree.Element("document")
    element.render(node, value)

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>value-1-1</ns0:item_1>
          <ns0:item_2>value-1-2</ns0:item_2>
          <ns0:item_1>value-2-1</ns0:item_1>
          <ns0:item_2>value-2-2</ns0:item_2>
        </ns0:container>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_xml_complex_type_empty():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <complexType name="empty"/>
          <element name="container">
            <complexType>
              <sequence>
                <element name="something" type="tns:empty"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_elm(something={})

    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:something/>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item.something is None


def test_xml_schema_as_payload():
    schema = xsd.Schema(
        load_xml(
            """
        <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element ref="xsd:schema"/>
                <xsd:any/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )
    elm_class = schema.get_element("{http://tests.python-zeep.org/}container")

    node = load_xml(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/"
                       xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:schema
              targetNamespace="http://tests.python-zeep.org/inline-schema"
              elementFormDefault="qualified">
            <xsd:element name="sub-element">
              <xsd:complexType>
                <xsd:sequence>
                  <xsd:element name="item-1" type="xsd:string"/>
                  <xsd:element name="item-2" type="xsd:string"/>
                </xsd:sequence>
              </xsd:complexType>
            </xsd:element>
          </xsd:schema>
          <ns1:sub-element xmlns:ns1="http://tests.python-zeep.org/inline-schema">
            <ns1:item-1>value-1</ns1:item-1>
            <ns1:item-2>value-2</ns1:item-2>
          </ns1:sub-element>
        </ns0:container>
    """
    )
    value = elm_class.parse(node, schema)
    assert value._value_1["item-1"] == "value-1"
    assert value._value_1["item-2"] == "value-2"


def test_xml_nill():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                elementFormDefault="qualified">
          <element name="container">
            <complexType>
              <sequence>
                <element name="foo" type="string" nillable="true"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    address_type = schema.get_element("ns0:container")
    obj = address_type()
    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:foo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true"/>
        </ns0:container>
      </document>
    """
    node = etree.Element("document")
    address_type.render(node, obj)
    etree.cleanup_namespaces(node)

    assert_nodes_equal(expected, node)


def test_xml_empty_xmlns():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <complexType name="empty"/>
          <element name="container">
            <complexType>
              <sequence>
                <element ref="schema"/>
                <any/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    node = load_xml(
        """
        <container>
            <xs:schema
                xmlns=""
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:msdata="urn:schemas-microsoft-com:xml-msdata" id="NewDataSet">
              <xs:element name="something" type="xs:string" msdata:foo=""/>
            </xs:schema>
            <something>foo</something>
        </container>
    """
    )
    item = container_elm.parse(node, schema)
    assert item._value_1 == "foo"


def test_xml_keep_objects_intact():
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
                <xsd:element name="name" type="xsd:string"/>
                <xsd:element minOccurs="0" name="optional" type="xsd:string"/>
                <xsd:element name="container" nillable="true" type="tns:Container"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>

          <xsd:complexType name="Container">
            <xsd:sequence>
              <xsd:element maxOccurs="unbounded" minOccurs="0" name="service"
                           nillable="true" type="tns:ServiceRequestType"/>
            </xsd:sequence>
          </xsd:complexType>

          <xsd:complexType name="ServiceRequestType">
            <xsd:sequence>
              <xsd:element name="name" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
    """
        )
    )

    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")
    obj = address_type(name="foo", container={"service": [{"name": "foo"}]})

    org_obj = copy.deepcopy(obj)

    node = etree.Element("document")
    address_type.render(node, obj)

    print(org_obj)
    print(obj)
    assert org_obj["container"]["service"] == obj["container"]["service"]
