from lxml import etree

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import xsd


def test_build_occurs_1():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.All(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                ]
            )
        ),
    )

    obj = custom_type(item_1="foo", item_2="bar")
    result = render_node(custom_type, obj)

    expected = load_xml(
        """
        <document>
          <ns0:authentication xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:item_1>foo</ns0:item_1>
            <ns0:item_2>bar</ns0:item_2>
          </ns0:authentication>
        </document>
    """
    )

    assert_nodes_equal(result, expected)

    obj = custom_type.parse(result[0], None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"


def test_build_pare_other_order():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.All(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                ]
            )
        ),
    )

    xml = load_xml(
        """
        <document>
          <ns0:authentication xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:item_2>bar</ns0:item_2>
            <ns0:item_1>foo</ns0:item_1>
          </ns0:authentication>
        </document>
    """
    )

    obj = custom_type.parse(xml[0], None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"
