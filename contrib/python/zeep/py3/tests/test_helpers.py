import datetime
from collections import OrderedDict

from lxml import etree

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import helpers, xsd
from zeep.helpers import serialize_object


def test_serialize_simple():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "name"),
                        xsd.String(),
                    ),
                    xsd.Attribute(
                        etree.QName("http://tests.python-zeep.org/", "attr"),
                        xsd.String(),
                    ),
                ]
            )
        ),
    )

    obj = custom_type(name="foo", attr="x")
    assert obj.name == "foo"
    assert obj.attr == "x"

    result = serialize_object(obj)

    assert result == {"name": "foo", "attr": "x"}


def test_serialize_nested_complex_type():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "items"),
                        xsd.ComplexType(
                            xsd.Sequence(
                                [
                                    xsd.Element(
                                        etree.QName(
                                            "http://tests.python-zeep.org/", "x"
                                        ),
                                        xsd.String(),
                                    ),
                                    xsd.Element(
                                        etree.QName(
                                            "http://tests.python-zeep.org/", "y"
                                        ),
                                        xsd.ComplexType(
                                            xsd.Sequence(
                                                [
                                                    xsd.Element(
                                                        etree.QName(
                                                            "http://tests.python-zeep.org/",
                                                            "x",
                                                        ),
                                                        xsd.String(),
                                                    )
                                                ]
                                            )
                                        ),
                                    ),
                                ]
                            )
                        ),
                        max_occurs=2,
                    )
                ]
            )
        ),
    )

    obj = custom_type(
        items=[{"x": "bla", "y": {"x": "deep"}}, {"x": "foo", "y": {"x": "deeper"}}]
    )

    assert len(obj.items) == 2
    obj.items[0].x == "bla"
    obj.items[0].y.x == "deep"
    obj.items[1].x == "foo"
    obj.items[1].y.x == "deeper"

    result = serialize_object(obj)

    assert result == {
        "items": [{"x": "bla", "y": {"x": "deep"}}, {"x": "foo", "y": {"x": "deeper"}}]
    }


def test_nested_complex_types():
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
                <xsd:element name="item" type="tns:item"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
          <xsd:complexType name="item">
            <xsd:sequence>
              <xsd:element name="item_1" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
    """
        )
    )

    container_elm = schema.get_element("{http://tests.python-zeep.org/}container")
    item_type = schema.get_type("{http://tests.python-zeep.org/}item")

    instance = container_elm(item=item_type(item_1="foo"))
    result = serialize_object(instance)
    assert isinstance(result, dict), type(result)
    assert isinstance(result["item"], dict), type(result["item"])
    assert result["item"]["item_1"] == "foo"


def test_serialize_any_array():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(xsd.Sequence([xsd.Any(max_occurs=2)])),
    )

    any_obj = etree.Element("{http://tests.python-zeep.org}lxml")
    etree.SubElement(any_obj, "node").text = "foo"

    obj = custom_type(any_obj)

    expected = """
        <document>
          <ns0:authentication xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:lxml xmlns:ns0="http://tests.python-zeep.org">
              <node>foo</node>
            </ns0:lxml>
          </ns0:authentication>
        </document>
    """
    node = etree.Element("document")
    custom_type.render(node, obj)
    assert_nodes_equal(expected, node)

    schema = xsd.Schema()
    obj = custom_type.parse(list(node)[0], schema=schema)
    result = serialize_object(obj)

    assert result == {"_value_1": [any_obj]}


def test_create_xml_soap_map():
    data = OrderedDict(
        [
            ("text", "String"),
            ("bytes", b"Bytes"),
            ("boolean", True),
            ("integer", 100),
            ("float", 100.1234),
            ("datetime", datetime.datetime(2017, 10, 28, 12, 30, 10)),
            ("date", datetime.date(2016, 1, 14)),
        ]
    )
    value = helpers.create_xml_soap_map(data)

    expected = """
    <document>
        <item>
          <key xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">text</key>
          <value xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">String</value>
        </item>
        <item>
          <key xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">bytes</key>
          <value xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">Bytes</value>
        </item>
        <item>
          <key xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">boolean</key>
          <value xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:boolean">true</value>
        </item>
        <item>
          <key xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">integer</key>
          <value xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:integer">100</value>
        </item>
        <item>
          <key xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">float</key>
          <value xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:float">100.1234</value>
        </item>
        <item>
          <key xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">datetime</key>
          <value xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:dateTime">2017-10-28T12:30:10</value>
        </item>
        <item>
          <key xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">date</key>
          <value xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:date">2016-01-14</value>
        </item>
     </document>
     """  # noqa
    node = render_node(value._xsd_type, value)
    assert_nodes_equal(expected, node)
