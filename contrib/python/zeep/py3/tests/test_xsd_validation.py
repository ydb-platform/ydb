import pytest

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import exceptions, xsd


def test_validate_element_value():
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
                <element minOccurs="1" maxOccurs="1" name="item" type="string" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )
    schema.set_ns_prefix("tns", "http://tests.python-zeep.org/")

    container_elm = schema.get_element("tns:container")
    obj = container_elm()

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item>bar</ns0:item>
        </ns0:container>
      </document>
    """

    with pytest.raises(exceptions.ValidationError) as exc:
        result = render_node(container_elm, obj)
    assert "Missing element item (container.item)" in str(exc.value)

    obj.item = "bar"
    result = render_node(container_elm, obj)

    assert_nodes_equal(result, expected)

    obj = container_elm.parse(result[0], schema)
    assert obj.item == "bar"


def test_validate_required_attribute():
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
              <attribute name="item" type="string" use="required"/>
            </complexType>
          </element>
        </schema>
    """
        )
    )
    schema.set_ns_prefix("tns", "http://tests.python-zeep.org/")

    container_elm = schema.get_element("tns:container")
    obj = container_elm()

    expected = """
      <document>
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/" item="bar"/>
      </document>
    """

    with pytest.raises(exceptions.ValidationError) as exc:
        result = render_node(container_elm, obj)
    assert "The attribute item is not valid: Value is required (container.item)" in str(
        exc.value
    )

    obj.item = "bar"
    result = render_node(container_elm, obj)

    assert_nodes_equal(result, expected)

    obj = container_elm.parse(result[0], schema)
    assert obj.item == "bar"
