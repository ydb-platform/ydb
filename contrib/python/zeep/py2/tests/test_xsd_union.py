from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import xsd


def test_union_same_types():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns="http://tests.python-zeep.org/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">


          <xsd:simpleType name="MMYY">
            <xsd:restriction base="xsd:int"/>
          </xsd:simpleType>

          <xsd:simpleType name="MMYYYY">
            <xsd:restriction base="xsd:int"/>
          </xsd:simpleType>

          <xsd:simpleType name="Date">
            <xsd:union memberTypes="tns:MMYY MMYYYY"/>
          </xsd:simpleType>
          <xsd:element name="item" type="tns:Date"/>
        </xsd:schema>
    """
        )
    )

    elm = schema.get_element("ns0:item")
    node = render_node(elm, "102018")
    expected = """
        <document>
          <ns0:item xmlns:ns0="http://tests.python-zeep.org/">102018</ns0:item>
        </document>
    """
    assert_nodes_equal(expected, node)
    value = elm.parse(list(node)[0], schema)
    assert value == 102018


def test_union_mixed():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
          <xsd:element name="item" type="tns:Date"/>
          <xsd:simpleType name="Date">
            <xsd:union memberTypes="xsd:date xsd:gYear xsd:gYearMonth tns:MMYY tns:MMYYYY"/>
          </xsd:simpleType>
          <xsd:simpleType name="MMYY">
            <xsd:restriction base="xsd:string">
              <xsd:pattern value="(0[123456789]|1[012]){1}\d{2}"/>
            </xsd:restriction>
          </xsd:simpleType>
          <xsd:simpleType name="MMYYYY">
            <xsd:restriction base="xsd:string">
              <xsd:pattern value="(0[123456789]|1[012]){1}\d{4}"/>
            </xsd:restriction>
          </xsd:simpleType>
        </xsd:schema>
    """
        )
    )

    elm = schema.get_element("ns0:item")
    node = render_node(elm, "102018")
    expected = """
        <document>
          <ns0:item xmlns:ns0="http://tests.python-zeep.org/">102018</ns0:item>
        </document>
    """
    assert_nodes_equal(expected, node)
    value = elm.parse(list(node)[0], schema)
    assert value == "102018"

    node = render_node(elm, "2018")
    expected = """
        <document>
          <ns0:item xmlns:ns0="http://tests.python-zeep.org/">2018</ns0:item>
        </document>
    """
    assert_nodes_equal(expected, node)
    value = elm.parse(list(node)[0], schema)
    assert value == "2018"
