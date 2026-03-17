import pytest
from lxml import etree

from tests.utils import assert_nodes_equal, load_xml, render_node
from zeep import xsd


def test_build_objects():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "username"),
                        xsd.String(),
                    ),
                    xsd.Group(
                        etree.QName("http://tests.python-zeep.org/", "groupie"),
                        xsd.Sequence(
                            [
                                xsd.Element(
                                    etree.QName(
                                        "http://tests.python-zeep.org/", "password"
                                    ),
                                    xsd.String(),
                                )
                            ]
                        ),
                    ),
                ]
            )
        ),
    )
    assert custom_type.signature()
    obj = custom_type(username="foo", password="bar")

    expected = """
      <document>
        <ns0:authentication xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:username>foo</ns0:username>
          <ns0:password>bar</ns0:password>
        </ns0:authentication>
      </document>
    """
    node = etree.Element("document")
    custom_type.render(node, obj)
    assert_nodes_equal(expected, node)

    obj = custom_type.parse(node[0], None)
    assert obj.username == "foo"
    assert obj.password == "bar"


def test_build_group_min_occurs_1():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Group(
                etree.QName("http://tests.python-zeep.org/", "foobar"),
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
                min_occurs=1,
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

    obj = custom_type.parse(result[0], None)
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"
    assert not hasattr(obj, "foobar")


def test_build_group_min_occurs_1_parse_args():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Group(
                etree.QName("http://tests.python-zeep.org/", "foobar"),
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
                min_occurs=1,
            )
        ),
    )

    obj = custom_type("foo", "bar")
    assert obj.item_1 == "foo"
    assert obj.item_2 == "bar"


def test_build_group_min_occurs_2():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Group(
                etree.QName("http://tests.python-zeep.org/", "foobar"),
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
                min_occurs=2,
                max_occurs=2,
            )
        ),
    )

    obj = custom_type(
        _value_1=[
            {"item_1": "foo", "item_2": "bar"},
            {"item_1": "foo", "item_2": "bar"},
        ]
    )
    assert obj._value_1 == [
        {"item_1": "foo", "item_2": "bar"},
        {"item_1": "foo", "item_2": "bar"},
    ]

    result = render_node(custom_type, obj)
    expected = load_xml(
        """
        <document>
          <ns0:authentication xmlns:ns0="http://tests.python-zeep.org/">
            <ns0:item_1>foo</ns0:item_1>
            <ns0:item_2>bar</ns0:item_2>
            <ns0:item_1>foo</ns0:item_1>
            <ns0:item_2>bar</ns0:item_2>
          </ns0:authentication>
        </document>
    """
    )

    assert_nodes_equal(result, expected)

    obj = custom_type.parse(result[0], None)
    assert obj._value_1 == [
        {"item_1": "foo", "item_2": "bar"},
        {"item_1": "foo", "item_2": "bar"},
    ]
    assert not hasattr(obj, "foobar")


def test_build_group_min_occurs_2_sequence_min_occurs_2():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Group(
                etree.QName("http://tests.python-zeep.org/", "foobar"),
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
                ),
                min_occurs=2,
                max_occurs=2,
            )
        ),
    )
    expected = etree.fromstring(
        """
        <ns0:container xmlns:ns0="http://tests.python-zeep.org/">
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
          <ns0:item_1>foo</ns0:item_1>
          <ns0:item_2>bar</ns0:item_2>
        </ns0:container>
    """
    )
    obj = custom_type.parse(expected, None)
    assert obj._value_1 == [
        {
            "_value_1": [
                {"item_1": "foo", "item_2": "bar"},
                {"item_1": "foo", "item_2": "bar"},
            ]
        },
        {
            "_value_1": [
                {"item_1": "foo", "item_2": "bar"},
                {"item_1": "foo", "item_2": "bar"},
            ]
        },
    ]
    assert not hasattr(obj, "foobar")


def test_build_group_occurs_1_invalid_kwarg():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Group(
                etree.QName("http://tests.python-zeep.org/", "foobar"),
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
                min_occurs=1,
                max_occurs=1,
            )
        ),
    )

    with pytest.raises(TypeError):
        custom_type(item_1="foo", item_2="bar", error=True)


def test_build_group_min_occurs_2_invalid_kwarg():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Group(
                etree.QName("http://tests.python-zeep.org/", "foobar"),
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
                min_occurs=2,
                max_occurs=2,
            )
        ),
    )

    with pytest.raises(TypeError):
        custom_type(
            _value_1=[
                {"item_1": "foo", "item_2": "bar", "error": True},
                {"item_1": "foo", "item_2": "bar"},
            ]
        )


def test_xml_group_via_ref():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   xmlns:tns="http://tests.python-zeep.org/"
                   targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="qualified">

          <xs:element name="Address">
            <xs:complexType>
              <xs:group ref="tns:Name" />
            </xs:complexType>
          </xs:element>

          <xs:group name="Name">
            <xs:sequence>
              <xs:element name="first_name" type="xs:string" />
              <xs:element name="last_name" type="xs:string" />
            </xs:sequence>
          </xs:group>

        </xs:schema>
    """
        )
    )
    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")

    obj = address_type(first_name="foo", last_name="bar")

    node = etree.Element("document")
    address_type.render(node, obj)
    expected = """
        <document>
            <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:first_name>foo</ns0:first_name>
                <ns0:last_name>bar</ns0:last_name>
            </ns0:Address>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_xml_group_via_ref_max_occurs_unbounded():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   xmlns:tns="http://tests.python-zeep.org/"
                   targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="qualified">

          <xs:element name="Address">
            <xs:complexType>
              <xs:group ref="tns:Name" minOccurs="0" maxOccurs="unbounded"/>
            </xs:complexType>
          </xs:element>

          <xs:group name="Name">
            <xs:sequence>
              <xs:element name="first_name" type="xs:string" />
              <xs:element name="last_name" type="xs:string" />
            </xs:sequence>
          </xs:group>

        </xs:schema>
    """
        )
    )
    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")

    obj = address_type(
        _value_1=[
            {"first_name": "foo-1", "last_name": "bar-1"},
            {"first_name": "foo-2", "last_name": "bar-2"},
        ]
    )

    node = etree.Element("document")
    address_type.render(node, obj)
    expected = """
        <document>
            <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
                <ns0:first_name>foo-1</ns0:first_name>
                <ns0:last_name>bar-1</ns0:last_name>
                <ns0:first_name>foo-2</ns0:first_name>
                <ns0:last_name>bar-2</ns0:last_name>
            </ns0:Address>
        </document>
    """
    assert_nodes_equal(expected, node)

    obj = address_type.parse(node[0], None)
    assert obj._value_1[0]["first_name"] == "foo-1"
    assert obj._value_1[0]["last_name"] == "bar-1"
    assert obj._value_1[1]["first_name"] == "foo-2"
    assert obj._value_1[1]["last_name"] == "bar-2"


def test_xml_multiple_groups_in_sequence():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   xmlns:tns="http://tests.python-zeep.org/"
                   targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="unqualified">

          <xs:element name="Address" type="tns:AddressType" />

          <xs:complexType name="AddressType">
            <xs:sequence>
              <xs:group ref="tns:NameGroup"/>
              <xs:group ref="tns:AddressGroup"/>
            </xs:sequence>
          </xs:complexType>

          <xs:group name="NameGroup">
            <xs:sequence>
              <xs:element name="first_name" type="xs:string" />
              <xs:element name="last_name" type="xs:string" />
            </xs:sequence>
          </xs:group>

          <xs:group name="AddressGroup">
            <xs:annotation>
              <xs:documentation>blub</xs:documentation>
            </xs:annotation>
            <xs:sequence>
              <xs:element name="city" type="xs:string" />
              <xs:element name="country" type="xs:string" />
            </xs:sequence>
          </xs:group>
        </xs:schema>
    """
        )
    )
    address_type = schema.get_element("{http://tests.python-zeep.org/}Address")

    obj = address_type(
        first_name="foo", last_name="bar", city="Utrecht", country="The Netherlands"
    )

    node = etree.Element("document")
    address_type.render(node, obj)
    expected = """
        <document>
            <ns0:Address xmlns:ns0="http://tests.python-zeep.org/">
                <first_name>foo</first_name>
                <last_name>bar</last_name>
                <city>Utrecht</city>
                <country>The Netherlands</country>
            </ns0:Address>
        </document>
    """
    assert_nodes_equal(expected, node)


def test_xml_group_methods():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   xmlns:tns="http://tests.python-zeep.org/"
                   targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="unqualified">

          <xs:group name="Group">
            <xs:annotation>
              <xs:documentation>blub</xs:documentation>
            </xs:annotation>
            <xs:sequence>
              <xs:element name="city" type="xs:string" />
              <xs:element name="country" type="xs:string" />
            </xs:sequence>
          </xs:group>

        </xs:schema>
    """
        )
    )
    Group = schema.get_group("{http://tests.python-zeep.org/}Group")
    assert Group.signature(schema) == (
        "ns0:Group(city: xsd:string, country: xsd:string)"
    )
    assert str(Group) == (
        "{http://tests.python-zeep.org/}Group(city: xsd:string, country: xsd:string)"
    )

    assert len(list(Group)) == 2


def test_xml_group_extension():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   xmlns:tns="http://tests.python-zeep.org/"
                   targetNamespace="http://tests.python-zeep.org/"
                   elementFormDefault="unqualified">

          <xs:group name="Group">
            <xs:sequence>
              <xs:element name="item_2" type="xs:string" />
              <xs:element name="item_3" type="xs:string" />
            </xs:sequence>
          </xs:group>

          <xs:complexType name="base">
            <xs:sequence>
              <xs:element name="item_1" type="xs:string" minOccurs="0"/>
            </xs:sequence>
          </xs:complexType>

          <xs:complexType name="SubGroup">
            <xs:complexContent>
              <xs:extension base="tns:base">
                <xs:group ref="tns:Group"/>
              </xs:extension>
            </xs:complexContent>
          </xs:complexType>
        </xs:schema>
    """
        )
    )
    SubGroup = schema.get_type("{http://tests.python-zeep.org/}SubGroup")
    assert SubGroup.signature(schema) == (
        "ns0:SubGroup(item_1: xsd:string, item_2: xsd:string, item_3: xsd:string)"
    )
    SubGroup(item_1="een", item_2="twee", item_3="drie")
