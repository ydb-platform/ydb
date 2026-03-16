import pytest
from lxml import etree

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import xsd
from zeep.settings import Settings


def test_build_occurs_1():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
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
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"

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
    obj = custom_type.parse(expected[0], None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"


def test_build_occurs_1_skip_value():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
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
    obj = custom_type(item_1=xsd.SkipValue, item_2="bar")
    assert obj.item_1 == xsd.SkipValue
    assert obj.item_2 == "bar"

    result = render_node(custom_type, obj)
    expected = load_xml(
        """
        <document>
          <ns0:authentication xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:item_2>bar</ns0:item_2>
          </ns0:authentication>
        </document>
    """
    )
    assert_nodes_equal(result, expected)


def test_build_min_occurs_2_max_occurs_2():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                ],
                min_occurs=2,
                max_occurs=2,
            )
        ),
    )

    assert custom_type.signature()

    elm = custom_type(
        _value_1=[
            {"item_1": "foo-1", "item_2": "bar-1"},
            {"item_1": "foo-2", "item_2": "bar-2"},
        ]
    )

    assert elm._value_1 == [
        {"item_1": "foo-1", "item_2": "bar-1"},
        {"item_1": "foo-2", "item_2": "bar-2"},
    ]

    expected = load_xml(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj._value_1 == [
        {"item_1": "foo", "item_2": "bar"},
        {"item_1": "foo", "item_2": "bar"},
    ]


def test_build_min_occurs_2_max_occurs_2_error():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                ],
                min_occurs=2,
                max_occurs=2,
            )
        ),
    )

    with pytest.raises(TypeError):
        custom_type(_value_1={"item_1": "foo-1", "item_2": "bar-1", "error": True})


def test_build_sequence_and_attributes():
    custom_element = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
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
            ),
            [
                xsd.Attribute(
                    etree.QName("http://tests.python-zeep.org/", "attr_1"), xsd.String()
                ),
                xsd.Attribute("attr_2", xsd.String()),
            ],
        ),
    )
    expected = load_xml(
        """
        <ns0:authentication xmlns:ns0="http://tests.python-zeep.org/" ns0:attr_1="x" attr_2="y">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
        </ns0:authentication>
    """
    )
    obj = custom_element.parse(expected, None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"
    assert obj.attr_1 == "x"
    assert obj.attr_2 == "y"


def test_build_sequence_with_optional_elements():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "container"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.ComplexType(
                            xsd.Sequence(
                                [
                                    xsd.Element(
                                        etree.QName(
                                            "http://tests.python-zeep.org/", "item_2_1"
                                        ),
                                        xsd.String(),
                                        nillable=True,
                                    )
                                ]
                            )
                        ),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_3"),
                        xsd.String(),
                        max_occurs=2,
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_4"),
                        xsd.String(),
                        min_occurs=0,
                    ),
                ]
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>1</ns0:item_1>
          <ns0:item_2/>
          <ns0:item_3>3</ns0:item_3>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj.item_1 == "1"
    assert obj.item_2 is None
    assert obj.item_3 == ["3"]
    assert obj.item_4 is None


def test_build_max_occurs_unbounded():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                ],
                max_occurs="unbounded",
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj._value_1 == [{"item_1": "foo", "item_2": "bar"}]


def test_xml_sequence_with_choice():
    schema = xsd.Schema(
        load_xml(
            """
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/tst"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/tst">
          <element name="container">
            <complexType>
              <sequence>
                <choice>
                  <element name="item_1" type="xsd:string" />
                  <element name="item_2" type="xsd:string" />
                </choice>
                <element name="item_3" type="xsd:string" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    xml = load_xml(
        """
        <tst:container
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:tst="http://tests.python-zeep.org/tst">
          <tst:item_1>blabla</tst:item_1>
          <tst:item_3>haha</tst:item_3>
        </tst:container>
    """
    )

    elm = schema.get_element("{http://tests.python-zeep.org/tst}container")
    result = elm.parse(xml, schema)
    assert result.item_1 == "blabla"
    assert result.item_3 == "haha"


def test_xml_sequence_with_choice_max_occurs_2():
    schema = xsd.Schema(
        load_xml(
            """
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/tst"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/tst">
          <element name="container">
            <complexType>
              <sequence>
                <choice maxOccurs="2">
                  <element name="item_1" type="xsd:string" />
                  <element name="item_2" type="xsd:string" />
                </choice>
                <element name="item_3" type="xsd:string" />
              </sequence>
              <attribute name="item_1" type="xsd:string" use="optional" />
              <attribute name="item_2" type="xsd:string" use="optional" />
            </complexType>
          </element>
        </schema>
    """
        )
    )

    xml = load_xml(
        """
        <tst:container
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:tst="http://tests.python-zeep.org/tst">
          <tst:item_1>item-1-1</tst:item_1>
          <tst:item_1>item-1-2</tst:item_1>
          <tst:item_3>item-3</tst:item_3>
        </tst:container>
    """
    )

    elm = schema.get_element("{http://tests.python-zeep.org/tst}container")
    result = elm.parse(xml, schema)
    assert result._value_1 == [{"item_1": "item-1-1"}, {"item_1": "item-1-2"}]

    assert result.item_3 == "item-3"


def test_xml_sequence_with_choice_max_occurs_3():
    schema = xsd.Schema(
        load_xml(
            """
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/tst"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/tst">
          <element name="container">
            <complexType>
              <sequence>
                <choice maxOccurs="3">
                  <sequence>
                    <element name="item_1" type="xsd:string" />
                    <element name="item_2" type="xsd:string" />
                  </sequence>
                  <element name="item_3" type="xsd:string" />
                </choice>
                <element name="item_4" type="xsd:string" />
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    xml = load_xml(
        """
        <tst:container
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:tst="http://tests.python-zeep.org/tst">
          <tst:item_1>text-1</tst:item_1>
          <tst:item_2>text-2</tst:item_2>
          <tst:item_1>text-1</tst:item_1>
          <tst:item_2>text-2</tst:item_2>
          <tst:item_3>text-3</tst:item_3>
          <tst:item_4>text-4</tst:item_4>
        </tst:container>
    """
    )

    elm = schema.get_element("{http://tests.python-zeep.org/tst}container")
    result = elm.parse(xml, schema)
    assert result._value_1 == [
        {"item_1": "text-1", "item_2": "text-2"},
        {"item_1": "text-1", "item_2": "text-2"},
        {"item_3": "text-3"},
    ]
    assert result.item_4 == "text-4"


def test_xml_sequence_with_nil_element():
    schema = xsd.Schema(
        load_xml(
            """
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/">
          <element name="container">
            <complexType>
              <sequence>
                <element name="item" type="xsd:string" maxOccurs="unbounded"/>
              </sequence>
            </complexType>
          </element>
        </schema>
    """
        )
    )

    xml = load_xml(
        """
        <tns:container
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:tns="http://tests.python-zeep.org/">
          <tns:item>text-1</tns:item>
          <tns:item>text-2</tns:item>
          <tns:item/>
          <tns:item>text-4</tns:item>
          <tns:item>text-5</tns:item>
        </tns:container>
    """
    )

    elm = schema.get_element("{http://tests.python-zeep.org/}container")
    result = elm.parse(xml, schema)
    assert result.item == ["text-1", "text-2", None, "text-4", "text-5"]


def test_xml_sequence_unbounded():
    schema = xsd.Schema(
        load_xml(
            """
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/">
          <complexType name="ValueListType">
            <sequence maxOccurs="unbounded" minOccurs="0">
              <element ref="tns:Value"/>
            </sequence>
          </complexType>
          <element name="ValueList" type="tns:ValueListType"/>
          <element name="Value" type="tns:LongName"/>
          <simpleType name="LongName">
            <restriction base="string">
              <maxLength value="256"/>
           </restriction>
          </simpleType>
        </schema>
    """
        )
    )

    elm_type = schema.get_type("{http://tests.python-zeep.org/}ValueListType")

    with pytest.raises(TypeError):
        elm_type(Value="bla")
    elm_type(_value_1={"Value": "bla"})


def test_xml_sequence_recover_from_missing_element():
    schema = xsd.Schema(
        load_xml(
            """
        <schema
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/">
          <complexType name="container">
            <sequence>
              <element name="item_1" type="xsd:string"/>
              <element name="item_2" type="xsd:string"/>
              <element name="item_3" type="xsd:string"/>
              <element name="item_4" type="xsd:string"/>
            </sequence>
          </complexType>
        </schema>
    """
        ),
        settings=Settings(strict=False),
    )

    xml = load_xml(
        """
        <tns:container
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:tns="http://tests.python-zeep.org/">
          <tns:item_1>text-1</tns:item_1>
          <tns:item_3>text-3</tns:item_3>
          <tns:item_4>text-4</tns:item_4>
        </tns:container>
    """
    )
    elm_type = schema.get_type("{http://tests.python-zeep.org/}container")
    result = elm_type.parse_xmlelement(xml, schema)
    assert result.item_1 == "text-1"
    assert result.item_2 is None
    assert result.item_3 == "text-3"
    assert result.item_4 == "text-4"
