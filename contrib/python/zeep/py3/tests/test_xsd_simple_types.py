from lxml import etree

from tests.utils import assert_nodes_equal, load_xml
from zeep import xsd


def test_simple_type():
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
                <element name="something" type="long"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    item_cls = schema.get_element("{http://tests.python-zeep.org/}item")
    item = item_cls(something=12345678901234567890)

    node = etree.Element("document")
    item_cls.render(node, item)
    expected = """
        <document>
          <ns0:item xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:something>12345678901234567890</ns0:something>
          </ns0:item>
        </document>
    """
    assert_nodes_equal(expected, node)
    item = item_cls.parse(list(node)[0], schema)
    assert item.something == 12345678901234567890


def test_simple_type_optional():
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
                <element name="something" type="long" minOccurs="0"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    item_cls = schema.get_element("{http://tests.python-zeep.org/}item")
    item = item_cls()
    assert item.something is None

    node = etree.Element("document")
    item_cls.render(node, item)
    expected = """
        <document>
          <ns0:item xmlns:ns0="http://tests.python-zeep.org/"/>
        </document>
    """
    assert_nodes_equal(expected, node)

    item = item_cls.parse(list(node)[0], schema)
    assert item.something is None


def test_restriction_global():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <simpleType name="foo">
            <restriction base="integer">
              <minInclusive value="0"/>
              <maxInclusive value="100"/>
            </restriction>
          </simpleType>
        </schema>
    """
        )
    )

    type_cls = schema.get_type("{http://tests.python-zeep.org/}foo")
    assert type_cls.qname.text == "{http://tests.python-zeep.org/}foo"


def test_restriction_anon():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
          <element name="something">
            <simpleType>
              <restriction base="integer">
                <minInclusive value="0"/>
                <maxInclusive value="100"/>
              </restriction>
            </simpleType>
          </element>
        </schema>
    """
        )
    )

    element_cls = schema.get_element("{http://tests.python-zeep.org/}something")
    assert element_cls.type.qname == etree.QName(
        "{http://tests.python-zeep.org/}something"
    )

    obj = element_cls(75)

    node = etree.Element("document")
    element_cls.render(node, obj)
    expected = """
        <document>
            <ns0:something xmlns:ns0="http://tests.python-zeep.org/">75</ns0:something>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_simple_type_list():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">

          <simpleType name="values">
            <list itemType="integer"/>
          </simpleType>
          <element name="something" type="tns:values"/>
        </schema>
    """
        )
    )

    element_cls = schema.get_element("{http://tests.python-zeep.org/}something")
    obj = element_cls([1, 2, 3])
    assert obj == [1, 2, 3]

    node = etree.Element("document")
    element_cls.render(node, obj)
    expected = """
        <document>
            <ns0:something xmlns:ns0="http://tests.python-zeep.org/">1 2 3</ns0:something>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_simple_type_list_custom_type():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/"
                elementFormDefault="qualified">

          <simpleType name="CountryNameType">
            <list>
              <simpleType>
                <restriction base="string">
                  <enumeration value="None"/>
                  <enumeration value="AlternateName"/>
                  <enumeration value="City"/>
                  <enumeration value="Code"/>
                  <enumeration value="Country"/>
                </restriction>
              </simpleType>
            </list>
          </simpleType>
          <element name="something" type="tns:CountryNameType"/>
        </schema>
    """
        )
    )

    element_cls = schema.get_element("{http://tests.python-zeep.org/}something")
    obj = element_cls(["Code", "City"])
    assert obj == ["Code", "City"]

    node = etree.Element("document")
    element_cls.render(node, obj)
    expected = """
        <document>
            <ns0:something xmlns:ns0="http://tests.python-zeep.org/">Code City</ns0:something>
        </document>
    """
    assert_nodes_equal(expected, node)
