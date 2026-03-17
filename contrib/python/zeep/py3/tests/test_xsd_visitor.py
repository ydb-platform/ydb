import pytest
from lxml import etree

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import exceptions, xsd
from zeep.xsd.schema import Schema


def parse_schema_node(node):
    schema = Schema(node=node, transport=None, location=None)
    return schema


def test_schema_empty():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
              targetNamespace="http://tests.python-zeep.org/"
              elementFormDefault="qualified"
              attributeFormDefault="unqualified">
        </schema>
    """
    )
    schema = parse_schema_node(node)
    root = schema._get_schema_documents("http://tests.python-zeep.org/")[0]
    assert root._element_form == "qualified"
    assert root._attribute_form == "unqualified"


def test_element_simle_types():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
              targetNamespace="http://tests.python-zeep.org/">
            <element name="foo" type="string" />
            <element name="bar" type="int" />
        </schema>
    """
    )
    schema = parse_schema_node(node)
    assert schema.get_element("{http://tests.python-zeep.org/}foo")
    assert schema.get_element("{http://tests.python-zeep.org/}bar")


def test_element_simple_type_annotation():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
              targetNamespace="http://tests.python-zeep.org/">
            <element name="foo" type="string">
                <annotation>
                    <documentation>HOI!</documentation>
                </annotation>
            </element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    element = schema.get_element("{http://tests.python-zeep.org/}foo")
    assert element


def test_element_default_type():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">
            <element name="foo" />
        </schema>
    """
    )
    schema = parse_schema_node(node)
    element = schema.get_element("{http://tests.python-zeep.org/}foo")
    assert isinstance(element.type, xsd.AnyType)


def test_element_simple_type_unresolved():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">
            <element name="foo" type="tns:unresolved">
                <annotation>
                    <documentation>HOI!</documentation>
                </annotation>
            </element>
            <simpleType name="unresolved">
                <restriction base="integer">
                    <minInclusive value="0"/>
                    <maxInclusive value="100"/>
                </restriction>
            </simpleType>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    assert schema.get_type("{http://tests.python-zeep.org/}unresolved")


def test_element_max_occurs():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
              targetNamespace="http://tests.python-zeep.org/">
            <element name="container">
                <complexType>
                    <sequence>
                        <element name="e1" type="string" />
                        <element name="e2" type="string" maxOccurs="1" />
                        <element name="e3" type="string" maxOccurs="2" />
                        <element name="e4" type="string" maxOccurs="unbounded" />
                    </sequence>
                </complexType>
            </element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    elm = schema.get_element("{http://tests.python-zeep.org/}container")
    elements = dict(elm.type.elements)

    assert isinstance(elements["e1"], xsd.Element)
    assert elements["e1"].max_occurs == 1
    assert isinstance(elements["e2"], xsd.Element)
    assert elements["e2"].max_occurs == 1
    assert isinstance(elements["e3"], xsd.Element)
    assert elements["e3"].max_occurs == 2
    assert isinstance(elements["e4"], xsd.Element)
    assert elements["e4"].max_occurs == "unbounded"


def test_simple_content():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/">
            <complexType name="container">
                <simpleContent>
                    <extension base="xsd:string">
                        <attribute name="sizing" type="xsd:string" />
                    </extension>
                </simpleContent>
            </complexType>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_type = schema.get_type("{http://tests.python-zeep.org/}container")
    assert xsd_type(10, sizing="qwe")


def test_attribute_optional():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="foo">
            <complexType>
              <xsd:attribute name="base" type="xsd:string" />
            </complexType>
          </element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    value = xsd_element()

    node = render_node(xsd_element, value)
    expected = """
      <document>
        <ns0:foo xmlns:ns0="http://tests.python-zeep.org/"/>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_attribute_required():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="foo">
            <complexType>
              <xsd:attribute name="base" use="required" type="xsd:string" />
            </complexType>
          </element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    value = xsd_element()

    with pytest.raises(exceptions.ValidationError):
        node = render_node(xsd_element, value)

    value.base = "foo"
    node = render_node(xsd_element, value)

    expected = """
      <document>
        <ns0:foo xmlns:ns0="http://tests.python-zeep.org/" base="foo"/>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_attribute_default():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="foo">
            <complexType>
              <xsd:attribute name="base" default="x" type="xsd:string" />
            </complexType>
          </element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    value = xsd_element()

    node = render_node(xsd_element, value)
    expected = """
      <document>
        <ns0:foo xmlns:ns0="http://tests.python-zeep.org/" base="x"/>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_attribute_simple_type():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/">
            <element name="foo">
              <complexType>
                <attribute name="bar" use="optional">
                 <simpleType>
                  <restriction base="string">
                   <enumeration value="hoi"/>
                   <enumeration value="doei"/>
                  </restriction>
                 </simpleType>
                </attribute>
            </complexType>
          </element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    assert xsd_element(bar="hoi")


def test_attribute_any_type():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/">
          <element name="foo">
            <complexType>
              <xsd:attribute name="base" type="xsd:anyURI" />
            </complexType>
          </element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    value = xsd_element(base="hoi")

    node = render_node(xsd_element, value)
    expected = """
      <document>
        <ns0:foo xmlns:ns0="http://tests.python-zeep.org/" base="hoi"/>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_complex_content_mixed():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="foo">
            <xsd:complexType>
              <xsd:complexContent mixed="true">
                <xsd:extension base="xsd:anyType">
                  <xsd:attribute name="bar" type="xsd:anyURI" use="required"/>
                </xsd:extension>
              </xsd:complexContent>
            </xsd:complexType>
          </xsd:element>
        </schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    result = xsd_element("basetype", bar="hoi")

    node = etree.Element("document")
    xsd_element.render(node, result)

    expected = """
      <document>
        <ns0:foo xmlns:ns0="http://tests.python-zeep.org/" bar="hoi">basetype</ns0:foo>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_complex_content_extension():
    node = load_xml(
        """
        <schema
                xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <complexType name="BaseType" abstract="true">
            <sequence>
              <element name="name" type="xsd:string" minOccurs="0"/>
            </sequence>
          </complexType>
          <complexType name="SubType1">
            <complexContent>
              <extension base="tns:BaseType">
                <attribute name="attr_1" type="xsd:string"/>
                <attribute name="attr_2" type="xsd:string"/>
              </extension>
            </complexContent>
          </complexType>
          <complexType name="SubType2">
            <complexContent>
              <extension base="tns:BaseType">
                <attribute name="attr_a" type="xsd:string"/>
                <attribute name="attr_b" type="xsd:string"/>
                <attribute name="attr_c" type="xsd:string"/>
              </extension>
            </complexContent>
          </complexType>
          <element name="test" type="tns:BaseType"/>
        </schema>
    """
    )
    schema = parse_schema_node(node)

    record_type = schema.get_type("{http://tests.python-zeep.org/}SubType1")
    assert len(record_type.attributes) == 2
    assert len(record_type.elements) == 1

    record_type = schema.get_type("{http://tests.python-zeep.org/}SubType2")
    assert len(record_type.attributes) == 3
    assert len(record_type.elements) == 1

    xsd_element = schema.get_element("{http://tests.python-zeep.org/}test")
    xsd_type = schema.get_type("{http://tests.python-zeep.org/}SubType2")

    value = xsd_type(attr_a="a", attr_b="b", attr_c="c")
    node = render_node(xsd_element, value)
    expected = """
      <document>
        <ns0:test
            xmlns:ns0="http://tests.python-zeep.org/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            attr_a="a" attr_b="b" attr_c="c" xsi:type="ns0:SubType2"/>
      </document>
    """
    assert_nodes_equal(expected, node)


def test_simple_content_extension():
    node = load_xml(
        """
        <schema
                xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified"
                targetNamespace="http://tests.python-zeep.org/">
          <simpleType name="BaseType">
            <restriction base="xsd:integer">
              <minInclusive value="0"/>
              <maxInclusive value="100"/>
            </restriction>
          </simpleType>
          <complexType name="SubType1">
            <simpleContent>
              <extension base="tns:BaseType">
                <attribute name="attr_1" type="xsd:string"/>
                <attribute name="attr_2" type="xsd:string"/>
              </extension>
            </simpleContent>
          </complexType>
          <complexType name="SubType2">
            <simpleContent>
              <extension base="tns:BaseType">
                <attribute name="attr_a" type="xsd:string"/>
                <attribute name="attr_b" type="xsd:string"/>
                <attribute name="attr_c" type="xsd:string"/>
              </extension>
            </simpleContent>
          </complexType>
        </schema>
    """
    )
    schema = parse_schema_node(node)

    record_type = schema.get_type("{http://tests.python-zeep.org/}SubType1")
    assert len(record_type.attributes) == 2
    assert len(record_type.elements) == 1

    record_type = schema.get_type("{http://tests.python-zeep.org/}SubType2")
    assert len(record_type.attributes) == 3
    assert len(record_type.elements) == 1


def test_list_type():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">

          <xsd:simpleType name="listOfIntegers">
            <xsd:list itemType="integer" />
          </xsd:simpleType>

          <xsd:element name="foo">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg" type="tns:listOfIntegers"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </schema>
    """
    )

    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    value = xsd_element(arg=[1, 2, 3, 4, 5])

    node = render_node(xsd_element, value)
    expected = """
        <document>
          <ns0:foo xmlns:ns0="http://tests.python-zeep.org/">
            <arg>1 2 3 4 5</arg>
          </ns0:foo>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_list_type_unresolved():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">

          <xsd:simpleType name="listOfIntegers">
            <xsd:list itemType="tns:something" />
          </xsd:simpleType>

          <xsd:simpleType name="something">
            <xsd:restriction base="xsd:integer" />
          </xsd:simpleType>

          <xsd:element name="foo">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg" type="tns:listOfIntegers"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </schema>
    """
    )

    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    value = xsd_element(arg=[1, 2, 3, 4, 5])

    node = render_node(xsd_element, value)
    expected = """
        <document>
          <ns0:foo xmlns:ns0="http://tests.python-zeep.org/">
            <arg>1 2 3 4 5</arg>
          </ns0:foo>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_list_type_simple_type():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">

          <xsd:simpleType name="listOfIntegers">
            <xsd:list>
              <simpleType>
                <xsd:restriction base="xsd:integer" />
              </simpleType>
            </xsd:list>
          </xsd:simpleType>

          <xsd:element name="foo">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg" type="tns:listOfIntegers"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </schema>
    """
    )

    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    value = xsd_element(arg=[1, 2, 3, 4, 5])

    node = render_node(xsd_element, value)
    expected = """
        <document>
          <ns0:foo xmlns:ns0="http://tests.python-zeep.org/">
            <arg>1 2 3 4 5</arg>
          </ns0:foo>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_union_type():
    node = load_xml(
        """
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:tns="http://tests.python-zeep.org/"
                targetNamespace="http://tests.python-zeep.org/">
          <xsd:simpleType name="type">
            <xsd:union memberTypes="xsd:language">
              <xsd:simpleType>
                <xsd:restriction base="xsd:string">
                  <xsd:enumeration value=""/>
                </xsd:restriction>
              </xsd:simpleType>
            </xsd:union>
          </xsd:simpleType>

          <xsd:element name="foo">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg" type="tns:type"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </schema>
    """
    )

    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}foo")
    assert xsd_element(arg="hoi")


def test_simple_type_restriction():
    node = load_xml(
        """
        <xsd:schema
            xmlns="http://tests.python-zeep.org/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified"
            attributeFormDefault="unqualified">
          <xsd:simpleType name="type_3">
            <xsd:restriction base="type_2"/>
          </xsd:simpleType>
          <xsd:simpleType name="type_2">
            <xsd:restriction base="type_1"/>
          </xsd:simpleType>
          <xsd:simpleType name="type_1">
            <xsd:restriction base="xsd:int">
              <xsd:totalDigits value="3"/>
            </xsd:restriction>
          </xsd:simpleType>
        </xsd:schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_type("{http://tests.python-zeep.org/}type_3")
    assert xsd_element(100) == "100"


def test_strip_spaces_from_qname():
    node = load_xml(
        """
        <xsd:schema
            xmlns="http://tests.python-zeep.org/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="STRIP_IT " type="xsd:string"/>

          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="THIS_TOO " type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
    )
    schema = parse_schema_node(node)
    xsd_element = schema.get_element("{http://tests.python-zeep.org/}STRIP_IT")
    assert xsd_element("okay") == "okay"

    xsd_element = schema.get_element("{http://tests.python-zeep.org/}container")
    elm = xsd_element(THIS_TOO="okay")
    assert elm.THIS_TOO == "okay"


def test_referenced_elements_in_choice():
    node = load_xml(
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
    schema = parse_schema_node(node)
    container_element = schema.get_element("{http://tests.python-zeep.org/}container")
    for el_name, sub_element in container_element.type.elements:
        assert el_name in ("el1", "el2", "el3")
        assert sub_element.min_occurs == 0
        assert sub_element.is_optional == True
