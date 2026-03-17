import datetime

import pytest
from lxml import etree

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import xsd
from zeep.exceptions import ValidationError


def get_any_schema():
    return xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="item" type="string"/>
          <element name="container">
            <complexType>
              <sequence>
                <any/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )


def test_default_xsd_type():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="container"/>
        </schema>
    """
        )
    )
    assert schema

    container_cls = schema.get_element("ns0:container")
    data = container_cls()
    assert data == ""


def test_any_simple():
    schema = get_any_schema()

    item_elm = schema.get_element("{http://tests.python-zeep.org/}item")
    assert isinstance(item_elm.type, xsd.String)

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")

    # Create via arg
    obj = container_elm(xsd.AnyObject(item_elm, item_elm("argh")))
    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:item>argh</ns0:item>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item._value_1 == "argh"

    # Create via kwarg _value_1
    obj = container_elm(_value_1=xsd.AnyObject(item_elm, item_elm("argh")))
    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:item>argh</ns0:item>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item._value_1 == "argh"


def test_any_value_element_tree():
    schema = get_any_schema()

    item = etree.Element("{http://tests.python-zeep.org}lxml")
    etree.SubElement(item, "node").text = "foo"

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")

    # Create via arg
    obj = container_elm(item)
    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:lxml xmlns:ns0="http://tests.python-zeep.org">
                    <node>foo</node>
                </ns0:lxml>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert isinstance(item._value_1, etree._Element)
    assert item._value_1.tag == "{http://tests.python-zeep.org}lxml"


def test_any_value_invalid():
    schema = get_any_schema()

    class SomeThing:
        pass

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")

    # Create via arg
    item = SomeThing()
    obj = container_elm(item)
    node = etree.Element("document")

    with pytest.raises(TypeError):
        container_elm.render(node, obj)


def test_any_without_element():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="qualified">
          <element name="item">
            <complexType>
              <sequence>
                <any minOccurs="0" maxOccurs="1"/>
              </sequence>
              <attribute name="type" type="string"/>
              <attribute name="title" type="string"/>
            </complexType>
          </element>
        </schema>
    """
        )
    )
    item_elm = schema.get_element("{http://tests.python-zeep.org/}item")
    item = item_elm(
        xsd.AnyObject(xsd.String(), "foobar"), type="attr-1", title="attr-2"
    )

    node = render_node(item_elm, item)
    expected = """
        <document>
          <ns0:item xmlns:ns0="http://tests.python-zeep.org/" type="attr-1" title="attr-2">foobar</ns0:item>
        </document>
    """
    assert_nodes_equal(expected, node)

    item = item_elm.parse(list(node)[0], schema)
    assert item.type == "attr-1"
    assert item.title == "attr-2"
    assert item._value_1 is None


def test_any_with_ref():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="qualified">
          <element name="item" type="string"/>
          <element name="container">
            <complexType>
              <sequence>
                <element ref="tns:item"/>
                <any/>
                <any maxOccurs="unbounded"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    item_elm = schema.get_element("{http://tests.python-zeep.org/}item")
    assert isinstance(item_elm.type, xsd.String)

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_elm(
        item="bar",
        _value_1=xsd.AnyObject(item_elm, item_elm("argh")),
        _value_2=xsd.AnyObject(item_elm, item_elm("ok")),
    )

    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:item>bar</ns0:item>
                <ns0:item>argh</ns0:item>
                <ns0:item>ok</ns0:item>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item.item == "bar"
    assert item._value_1 == "argh"


def test_element_any_parse():
    node = load_xml(
        """
        <xsd:schema
            elementFormDefault="qualified"
            targetNamespace="https://tests.python-zeep.org"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:any/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
    )

    schema = xsd.Schema(node)

    node = load_xml(
        """
          <container xmlns="https://tests.python-zeep.org">
            <something>
              <contains>text</contains>
            </something>
          </container>
    """
    )

    elm = schema.get_element("ns0:container")
    elm.parse(node, schema)


def test_element_any_type():
    node = etree.fromstring(
        """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="container">
            <complexType>
              <sequence>
                <element name="something" type="anyType"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """.strip()
    )
    schema = xsd.Schema(node)

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_elm(something=datetime.time(18, 29, 59))

    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:something>18:29:59</ns0:something>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item.something == "18:29:59"


def test_element_any_type_unknown_type():
    node = etree.fromstring(
        """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="container">
            <complexType>
              <sequence>
                <element name="something" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """.strip()
    )
    schema = xsd.Schema(node)

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")

    node = load_xml(
        """
        <document xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:something xmlns:q1="http://microsoft.com/wsdl/types/" xsi:type="q1:guid">bar</ns0:something>
            </ns0:container>
        </document>
    """
    )
    item = container_elm.parse(list(node)[0], schema)
    assert item.something == "bar"


def test_element_any_type_elements():
    node = etree.fromstring(
        """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="container">
            <complexType>
              <sequence>
                <element name="something" type="anyType"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """.strip()
    )
    schema = xsd.Schema(node)

    Child = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Element("{http://tests.python-zeep.org/}item_1", xsd.String()),
                xsd.Element("{http://tests.python-zeep.org/}item_2", xsd.String()),
            ]
        )
    )
    child = Child(item_1="item-1", item_2="item-2")

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_elm(something=child)

    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:something>
                    <ns0:item_1>item-1</ns0:item_1>
                    <ns0:item_2>item-2</ns0:item_2>
                </ns0:something>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert len(item.something) == 2
    assert item.something[0].text == "item-1"
    assert item.something[1].text == "item-2"


def test_any_in_nested_sequence():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/"
        >
          <xsd:element name="something" type="xsd:date"/>
          <xsd:element name="foobar" type="xsd:boolean"/>
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="items" minOccurs="0">
                  <xsd:complexType>
                    <xsd:sequence>
                      <xsd:any namespace="##any" processContents="lax"/>
                    </xsd:sequence>
                  </xsd:complexType>
                </xsd:element>
                <xsd:element name="version" type="xsd:string"/>
                <xsd:any namespace="##any" processContents="lax" minOccurs="0" maxOccurs="unbounded"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )  # noqa

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    assert container_elm.signature(schema) == (
        "ns0:container(items: {_value_1: ANY}, version: xsd:string, _value_1: ANY[])"
    )

    something = schema.get_element("{http://tests.python-zeep.org/}something")
    foobar = schema.get_element("{http://tests.python-zeep.org/}foobar")

    any_1 = xsd.AnyObject(something, datetime.date(2016, 7, 4))
    any_2 = xsd.AnyObject(foobar, True)
    obj = container_elm(
        items={"_value_1": any_1}, version="str1234", _value_1=[any_1, any_2]
    )

    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
          <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:items>
              <ns0:something>2016-07-04</ns0:something>
            </ns0:items>
            <ns0:version>str1234</ns0:version>
            <ns0:something>2016-07-04</ns0:something>
            <ns0:foobar>true</ns0:foobar>
          </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item.items._value_1 == datetime.date(2016, 7, 4)
    assert item.version == "str1234"
    assert item._value_1 == [datetime.date(2016, 7, 4), True]


def test_element_any_parse_inline_schema():
    node = load_xml(
        """
        <xsd:schema
            elementFormDefault="qualified"
            targetNamespace="https://tests.python-zeep.org"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
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

    schema = xsd.Schema(node)

    node = load_xml(
        """
        <OstatDepoResult>
            <xs:schema id="OD" xmlns="" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata">
                <xs:element name="OD" msdata:IsDataSet="true" msdata:UseCurrentLocale="true">
                    <xs:complexType>
                        <xs:choice minOccurs="0" maxOccurs="unbounded">
                            <xs:element name="odr">
                                <xs:complexType>
                                    <xs:sequence>
                                        <xs:element name="item" msdata:Caption="item" type="xs:string" minOccurs="0" />
                                    </xs:sequence>
                                </xs:complexType>
                            </xs:element>
                        </xs:choice>
                    </xs:complexType>
                </xs:element>
            </xs:schema>
            <diffgr:diffgram xmlns:msdata="urn:schemas-microsoft-com:xml-msdata" xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
                <OD xmlns="">
                    <odr diffgr:id="odr1" msdata:rowOrder="0">
                        <item>hetwerkt</item>
                    </odr>
                </OD>
            </diffgr:diffgram>
        </OstatDepoResult>
    """
    )

    elm = schema.get_element("ns0:container")
    data = elm.parse(node, schema)
    assert data._value_1._value_1[0]["odr"]["item"] == "hetwerkt"


def test_any_missing():
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
                <element name="item">
                  <complexType>
                    <sequence>
                      <any minOccurs="1" maxOccurs="unbounded"/>
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

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_elm({})

    node = etree.Element("document")
    with pytest.raises(ValidationError):
        container_elm.render(node, obj)


def test_any_optional():
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
                <element name="item">
                  <complexType>
                    <sequence>
                      <any minOccurs="0" maxOccurs="unbounded"/>
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

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = container_elm({})

    node = etree.Element("document")
    container_elm.render(node, obj)
    expected = """
        <document>
            <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:item/>
            </ns0:container>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = container_elm.parse(list(node)[0], schema)
    assert item.item is None
